package m

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
	pr.SkipPush.Store(true)
	pr.SkipRestoreHeightInvar.Store(true)
	if g.Options.Dynamic {
		g.Broadcast(graph.BLOCK_TOP_ASYNC)
	}
}

func (pr *PushRelabel) OnSuperStepConverged(g *Graph) (sent uint64) {
	switch pr.CurrentPhase {
	case RESUME:
		return

	case DRAIN_MSG:
		pr.t1 = time.Now()
		pr.CurrentPhase = RELABEL
		pr.SkipRestoreHeightInvar.Store(false)
		resetHeights(g)
		sent += sendMsgToSpecialHeightVertices(g, pr.VertexCount.GetMaxVertexCount())
		pr.t2 = time.Now()

	case RELABEL:
		pr.t3 = time.Now()
		pr.CurrentPhase = RESUME
		pr.SkipPush.Store(false)
		sent += sendMsgToActiveVertices(g)

		totalRuntime := pr.t3.Sub(pr.t0)
		log.Info().Msg(fmt.Sprintf("SyncGlobalRelabel done. "+
			"Draining Messages took %.2fs, Resetting heights took %.2fs, Global Relabeling took %.2fs. Total took %.2fs",
			pr.t1.Sub(pr.t0).Seconds(), pr.t2.Sub(pr.t1).Seconds(), pr.t3.Sub(pr.t2).Seconds(), totalRuntime.Seconds()))
		pr.GlobalRelabeling.GlobalRelabelingDone(totalRuntime.Milliseconds())
		g.Broadcast(graph.RESUME)
	}
	return sent
}

func resetHeights(g *Graph) {
	g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) (accumulated int) {
		for i := 0; i < len(gt.Vertices); i++ {
			v := &gt.Vertices[i].Property
			v.Height = MaxHeight
			for j := range v.Nbrs {
				v.Nbrs[j].Height = MaxHeight
			}
		}
		return 0
	})
}

func sendMsgToSpecialHeightVertices(g *Graph, vertexCount int64) (sent uint64) {
	_, source := g.NodeVertexFromRaw(SourceRawId)
	sourceInternalId, _ := g.NodeVertexFromRaw(SourceRawId)
	source.Property.Height = vertexCount
	source.Property.HeightChanged = true
	sMailbox, sTidx := g.NodeVertexMailbox(sourceInternalId)
	sent += g.EnsureSend(g.ActiveNotification(sourceInternalId, graph.Notification[Note]{Target: sourceInternalId, Note: Note{PosType: EmptyValue}}, sMailbox, sTidx))

	sinkInternalId, _ := g.NodeVertexFromRaw(SinkRawId)
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	sink.Property.Height = 0
	sink.Property.HeightChanged = true
	tMailbox, tTidx := g.NodeVertexMailbox(sinkInternalId)
	sent += g.EnsureSend(g.ActiveNotification(sinkInternalId, graph.Notification[Note]{Target: sinkInternalId, Note: Note{PosType: EmptyValue}}, tMailbox, tTidx))
	return sent
}

func sendMsgToActiveVertices(g *Graph) (sent uint64) {
	sentAtomic := atomic.Uint64{}
	activeVertices := g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) (accumulated int) {
		for i := 0; i < len(gt.Vertices); i++ {
			v := &gt.Vertices[i].Property
			if v.Excess > 0 {
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
