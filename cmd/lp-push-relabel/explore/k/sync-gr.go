package k

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

var CurrentPhase = RESUME
var t0, t1, t2, t3 time.Time

func SyncGlobalRelabel() {
	log.Info().Msg("SyncGlobalRelabel starts.")

	t0 = time.Now()
	CurrentPhase = DRAIN_MSG
	SkipPush.Store(true)
	SkipRestoreHeightInvar.Store(true)
}

func (*PushRelabel) OnSuperStepConverged(g *Graph) (sent uint64) {
	switch CurrentPhase {
	case RESUME:
		return

	case DRAIN_MSG:
		t1 = time.Now()
		CurrentPhase = RELABEL
		SkipRestoreHeightInvar.Store(false)
		resetHeights(g)
		sent += sendMsgToSpecialHeightVertices(g)
		t2 = time.Now()

	case RELABEL:
		t3 = time.Now()
		CurrentPhase = RESUME
		SkipPush.Store(false)
		sent += sendMsgToActiveVertices(g)

		log.Info().Msg(fmt.Sprintf("SyncGlobalRelabel done. "+
			"Draining Messages took %.2fs, Resetting heights took %.2fs, Global Relabeling took %.2fs. Total took %.2fs",
			t1.Sub(t0).Seconds(), t2.Sub(t1).Seconds(), t3.Sub(t2).Seconds(), t3.Sub(t0).Seconds()))
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

func sendMsgToSpecialHeightVertices(g *Graph) (sent uint64) {
	_, source := g.NodeVertexFromRaw(SourceRawId)
	sourceInternalId, _ := g.NodeVertexFromRaw(SourceRawId)
	source.Property.Height = VertexCountHelper.GetMaxVertexCount()
	source.Property.HeightChanged = true
	sMailbox, sTidx := g.NodeVertexMailbox(sourceInternalId)
	sent += g.EnsureSend(g.ActiveNotification(sourceInternalId, graph.Notification[Note]{Target: sourceInternalId}, sMailbox, sTidx))

	sinkInternalId, _ := g.NodeVertexFromRaw(SinkRawId)
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	sink.Property.Height = 0
	sink.Property.HeightChanged = true
	tMailbox, tTidx := g.NodeVertexMailbox(sinkInternalId)
	sent += g.EnsureSend(g.ActiveNotification(sinkInternalId, graph.Notification[Note]{Target: sinkInternalId}, tMailbox, tTidx))
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
				sentAtomic.Add(g.EnsureSend(g.ActiveNotification(id, graph.Notification[Note]{Target: id}, mailbox, tidx)))

				accumulated++
			}
		}
		return accumulated
	})
	log.Info().Msg(fmt.Sprintf("Number of active vertices: %v", activeVertices))
	return sentAtomic.Load()
}
