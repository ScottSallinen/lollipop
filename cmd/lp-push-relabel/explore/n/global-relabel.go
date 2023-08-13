package n

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

type Phase uint8

const (
	RESUME Phase = iota
	DRAIN_MSG
	RELABEL
)

type GlobalRelabel struct {
	IntervalLiftCount atomic.Int64
	IntervalTime      atomic.Int64
	NextLiftCount     atomic.Int64
	NextTime          atomic.Int64

	CurrentLiftCount atomic.Int64

	Triggered atomic.Bool

	CurrentPhase   Phase
	t0, t1, t2, t3 time.Time

	BlockLift atomic.Bool
	BlockPush atomic.Bool
}

const (
	grAlphaLiftCount = float64(2.0)
	grAlphaTime      = float64(5.0)
	grMinInterval    = 128
)

func (gr *GlobalRelabel) Reset() {
	gr.IntervalLiftCount.Store(grMinInterval)
	gr.IntervalTime.Store(time.Second.Milliseconds())
	gr.NextLiftCount.Store(gr.IntervalLiftCount.Load())
	gr.NextTime.Store(time.Now().UnixMilli() + gr.IntervalTime.Load())
	gr.CurrentLiftCount.Store(0)
	gr.Triggered.Store(false)
}

func (gr *GlobalRelabel) OnLift(g *Graph, pr *PushRelabel) {
	// TODO: improve performance
	newCount := gr.CurrentLiftCount.Add(1)
	liftCountMet, timeMet := newCount > gr.NextLiftCount.Load(), time.Now().UnixMilli() > gr.NextTime.Load()
	if liftCountMet || timeMet {
		if !gr.Triggered.Swap(true) {
			liftCountMet, timeMet = newCount > gr.NextLiftCount.Load(), time.Now().UnixMilli() > gr.NextTime.Load()
			if liftCountMet || timeMet {
				log.Info().Msg(fmt.Sprintf("GlobalRelabel triggered. liftCountMet=%v timeMet=%v", liftCountMet, timeMet))
				gr.SyncGlobalRelabel(g, pr)
			}
		}
	}
}

func (gr *GlobalRelabel) SyncGlobalRelabel(g *Graph, pr *PushRelabel) {
	log.Info().Msg("SyncGlobalRelabel starts.")

	source, sink := g.NodeVertex(pr.SourceId.Load()), g.NodeVertex(pr.SinkId.Load())
	log.Info().Msg("Source sent: " + strconv.Itoa(int(pr.SourceSupply-source.Property.Excess)))
	log.Info().Msg("Sink excess: " + strconv.Itoa(int(sink.Property.Excess)))

	gr.t0 = time.Now()
	gr.CurrentPhase = DRAIN_MSG
	gr.BlockLift.Store(true)
	if g.Options.Dynamic {
		g.Broadcast(graph.BLOCK_TOP_ASYNC)
	}
}

func (gr *GlobalRelabel) OnSuperStepConverged(g *Graph, pr *PushRelabel) (sent uint64) {
	switch gr.CurrentPhase {
	case RESUME:
		return
	case DRAIN_MSG:
		sent += gr.startRelabel(g, pr)
		if sent > 0 {
			break
		}
		fallthrough
	case RELABEL:
		sent += gr.resumeExecution(g, pr)
		if sent == 0 {
			log.Error().Msg("resumeExecution sent 0 messages. Adding one message to avoid termination.")
			sent += 1
		}
	}
	return sent
}

func (gr *GlobalRelabel) notifyCompletion(lastGrRuntimeMilli int64, vertexCount uint32) {
	gr.IntervalLiftCount.Store(utils.Max(int64(grAlphaLiftCount*float64(vertexCount)), grMinInterval))
	gr.IntervalTime.Store(int64(grAlphaTime * float64(lastGrRuntimeMilli)))

	gr.NextLiftCount.Store(gr.CurrentLiftCount.Load() + gr.IntervalLiftCount.Load())
	gr.NextTime.Store(time.Now().UnixMilli() + gr.IntervalTime.Load())
	gr.Triggered.Swap(false)
}

func (gr *GlobalRelabel) startRelabel(g *Graph, pr *PushRelabel) (sent uint64) {
	gr.t1 = time.Now()
	gr.CurrentPhase = RELABEL
	gr.BlockPush.Store(true)
	gr.BlockLift.Store(false)
	resetHeights(g)
	if !pr.HandleDeletes {
		sent += sendMsgToSpecialHeightVerticesNoDeletes(g, pr)
	} else {
		sent += sendMsgToSpecialHeightVerticesWithDeletes(g, pr)
	}
	gr.t2 = time.Now()
	return
}

func (gr *GlobalRelabel) resumeExecution(g *Graph, pr *PushRelabel) (sent uint64) {
	gr.t3 = time.Now()
	gr.CurrentPhase = RESUME
	gr.BlockPush.Store(false)
	sent += sendMsgToActiveVertices(g)

	totalRuntime := gr.t3.Sub(gr.t0)
	vertexCount := uint32(pr.VertexCount.GetMaxVertexCount())
	log.Info().Msg(fmt.Sprintf("SyncGlobalRelabel done. vertexCount="+strconv.Itoa(int(vertexCount))+
		"Draining Messages took %.2fs, Resetting heights took %.2fs, Global Relabeling took %.2fs. Total took %.2fs",
		gr.t1.Sub(gr.t0).Seconds(), gr.t2.Sub(gr.t1).Seconds(), gr.t3.Sub(gr.t2).Seconds(), totalRuntime.Seconds()))
	gr.notifyCompletion(totalRuntime.Milliseconds(), vertexCount)
	g.Broadcast(graph.RESUME)
	return
}

func resetHeights(g *Graph) {
	g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) (accumulated int) {
		for i := 0; i < len(gt.Vertices); i++ {
			v := &gt.Vertices[i].Property
			v.HeightPos, v.HeightNeg = MaxHeight, MaxHeight
			for j := range v.Nbrs {
				v.Nbrs[j].HeightPos, v.Nbrs[j].HeightNeg = MaxHeight, MaxHeight
			}
		}
		return 0
	})
}

func sendMsgToSpecialHeightVerticesNoDeletes(g *Graph, pr *PushRelabel) (sent uint64) {
	sourceId, sinkId := pr.SourceId.Load(), pr.SinkId.Load()
	source, sink := g.NodeVertex(sourceId), g.NodeVertex(sinkId)
	source.Property.updateHeightPos(uint32(pr.VertexCount.GetMaxVertexCount()))
	sink.Property.updateHeightPos(0)
	sMailbox, sTidx := g.NodeVertexMailbox(sourceId)
	tMailbox, tTidx := g.NodeVertexMailbox(sinkId)
	sent += g.EnsureSend(g.ActiveNotification(sourceId, graph.Notification[Note]{Target: sourceId, Note: Note{PosType: EmptyValue}}, sMailbox, sTidx))
	sent += g.EnsureSend(g.ActiveNotification(sinkId, graph.Notification[Note]{Target: sinkId, Note: Note{PosType: EmptyValue}}, tMailbox, tTidx))
	return sent
}

func sendMsgToSpecialHeightVerticesWithDeletes(g *Graph, pr *PushRelabel) (sent uint64) {
	sent = uint64(g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) (sent int) {
		for i := 0; i < len(gt.Vertices); i++ {
			v := &gt.Vertices[i].Property
			if v.Type != Normal || v.Excess < 0 {
				v.HeightPosChanged, v.HeightNegChanged = v.resetHeights(&pr.VertexCount)

				noti := graph.Notification[Note]{Target: threadOffset | uint32(i), Note: Note{PosType: EmptyValue}}
				mailbox, tidx := g.NodeVertexMailbox(noti.Target)
				sent += int(g.EnsureSend(g.ActiveNotification(noti.Target, noti, mailbox, tidx)))
			}
		}
		return sent
	}))
	log.Info().Msg(fmt.Sprintf("Number of vertices with special heights: %v", sent))
	return
}

func sendMsgToActiveVertices(g *Graph) (sent uint64) {
	sentAtomic := atomic.Uint64{}
	activeVertices := g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) (accumulated int) {
		for i := 0; i < len(gt.Vertices); i++ {
			v := &gt.Vertices[i].Property
			if v.Excess != 0 {
				id := threadOffset | uint32(i)
				mailbox, tidx := g.NodeVertexMailbox(id)
				sentAtomic.Add(g.EnsureSend(g.ActiveNotification(id, graph.Notification[Note]{Target: id, Note: Note{PosType: EmptyValue}}, mailbox, tidx)))
				accumulated++
			}
		}
		return accumulated
	})
	log.Info().Msg(fmt.Sprintf("Number of active vertices: %v", activeVertices))
	return sentAtomic.Load()
}
