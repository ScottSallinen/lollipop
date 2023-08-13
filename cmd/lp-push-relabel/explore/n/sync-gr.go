package n

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

type Phase uint8

const (
	RESUME Phase = iota
	DRAIN_MSG
	RELABEL
)

func (pr *PushRelabel) SyncGlobalRelabel(g *Graph) {
	log.Info().Msg("SyncGlobalRelabel starts.")

	source, sink := g.NodeVertex(pr.SourceId.Load()), g.NodeVertex(pr.SinkId.Load())
	log.Info().Msg("Source sent: " + strconv.Itoa(int(pr.SourceSupply-source.Property.Excess)))
	log.Info().Msg("Sink excess: " + strconv.Itoa(int(sink.Property.Excess)))

	pr.t0 = time.Now()
	pr.CurrentPhase = DRAIN_MSG
	pr.BlockLift.Store(true)
	if g.Options.Dynamic {
		g.Broadcast(graph.BLOCK_TOP_ASYNC)
	}
}

func (pr *PushRelabel) startRelabel(g *Graph) (sent uint64) {
	pr.t1 = time.Now()
	pr.CurrentPhase = RELABEL
	pr.BlockPush.Store(true)
	pr.BlockLift.Store(false)
	resetHeights(g)
	if !pr.HandleDeletes {
		sent += pr.sendMsgToSpecialHeightVerticesNoDeletes(g, uint32(pr.VertexCount.GetMaxVertexCount()))
	} else {
		sent += pr.sendMsgToSpecialHeightVerticesWithDeletes(g, uint32(pr.VertexCount.GetMaxVertexCount()))
	}
	pr.t2 = time.Now()
	return
}

func (pr *PushRelabel) resumeExecution(g *Graph) (sent uint64) {
	pr.t3 = time.Now()
	pr.CurrentPhase = RESUME
	pr.BlockPush.Store(false)
	sent += sendMsgToActiveVertices(g)

	totalRuntime := pr.t3.Sub(pr.t0)
	log.Info().Msg(fmt.Sprintf("SyncGlobalRelabel done. "+
		"Draining Messages took %.2fs, Resetting heights took %.2fs, Global Relabeling took %.2fs. Total took %.2fs",
		pr.t1.Sub(pr.t0).Seconds(), pr.t2.Sub(pr.t1).Seconds(), pr.t3.Sub(pr.t2).Seconds(), totalRuntime.Seconds()))

	pr.GlobalRelabeling.GlobalRelabelingDone(totalRuntime.Milliseconds())
	g.Broadcast(graph.RESUME)
	return
}

func (pr *PushRelabel) OnSuperStepConverged(g *Graph) (sent uint64) {
	switch pr.CurrentPhase {
	case RESUME:
		return
	case DRAIN_MSG:
		sent += pr.startRelabel(g)
		if sent > 0 {
			break
		}
		fallthrough
	case RELABEL:
		sent += pr.resumeExecution(g)
		if sent == 0 {
			log.Error().Msg("resumeExecution sent 0 messages. Adding one message to avoid termination.")
			sent += 1
		}
	}
	return sent
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

func (pr *PushRelabel) sendMsgToSpecialHeightVerticesNoDeletes(g *Graph, vertexCount uint32) (sent uint64) {
	_, source := g.NodeVertexFromRaw(SourceRawId)
	sourceInternalId, _ := g.NodeVertexFromRaw(SourceRawId)
	source.Property.updateHeightPos(vertexCount)
	sMailbox, sTidx := g.NodeVertexMailbox(sourceInternalId)
	sent += g.EnsureSend(g.ActiveNotification(sourceInternalId, graph.Notification[Note]{Target: sourceInternalId, Note: Note{PosType: EmptyValue}}, sMailbox, sTidx))

	sinkInternalId, _ := g.NodeVertexFromRaw(SinkRawId)
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	sink.Property.updateHeightPos(0)
	tMailbox, tTidx := g.NodeVertexMailbox(sinkInternalId)
	sent += g.EnsureSend(g.ActiveNotification(sinkInternalId, graph.Notification[Note]{Target: sinkInternalId, Note: Note{PosType: EmptyValue}}, tMailbox, tTidx))
	return sent
}

func (pr *PushRelabel) sendMsgToSpecialHeightVerticesWithDeletes(g *Graph, vertexCount uint32) (sent uint64) {
	sentAtomic := atomic.Uint64{}
	specialHeightVertices := g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) (accumulated int) {
		for i := 0; i < len(gt.Vertices); i++ {
			specialHeight := false
			v := &gt.Vertices[i].Property
			if v.Type != Normal || v.Excess < 0 {
				v.HeightPosChanged, v.HeightNegChanged = v.resetHeights(&pr.VertexCount)
				specialHeight = true
			}

			if specialHeight {
				noti := graph.Notification[Note]{Target: threadOffset | uint32(i), Note: Note{PosType: EmptyValue}}
				mailbox, tidx := g.NodeVertexMailbox(noti.Target)
				sentAtomic.Add(g.EnsureSend(g.ActiveNotification(noti.Target, noti, mailbox, tidx)))
				accumulated++
			}
		}
		return accumulated
	})
	log.Info().Msg(fmt.Sprintf("Number of vertices with special heights: %v", specialHeightVertices))
	return sentAtomic.Load()
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
