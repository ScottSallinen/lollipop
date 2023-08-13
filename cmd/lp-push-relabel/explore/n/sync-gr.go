package n

import (
	"fmt"
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
	source.Property.HeightPos = vertexCount
	source.Property.HeightPosChanged = true
	sMailbox, sTidx := g.NodeVertexMailbox(sourceInternalId)
	sent += g.EnsureSend(g.ActiveNotification(sourceInternalId, graph.Notification[Note]{Target: sourceInternalId, Note: Note{PosType: EmptyValue}}, sMailbox, sTidx))

	sinkInternalId, _ := g.NodeVertexFromRaw(SinkRawId)
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	sink.Property.HeightPos = 0
	sink.Property.HeightPosChanged = true
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
			if v.Type == Source {
				v.HeightPos, v.HeightNeg = vertexCount, 0
				v.HeightPosChanged, v.HeightNegChanged = true, true
				specialHeight = true
			} else if v.Type == Sink {
				v.HeightPos, v.HeightNeg = 0, vertexCount
				v.HeightPosChanged, v.HeightNegChanged = true, true
				specialHeight = true
			} else if v.Excess < 0 {
				v.HeightPos = 0
				v.HeightPosChanged = true
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
