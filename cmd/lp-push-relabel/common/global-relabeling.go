package common

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type GlobalRelabeling struct {
	interval    atomic.Int64
	grCount     atomic.Int64
	nextGrCount atomic.Int64

	sinkId   atomic.Uint32
	sourceId atomic.Uint32
}

const (
	grAlpha       = float64(1.0)
	grMinInterval = 128

	SynchronousGlobalRelabeling = true
)

var (
	GlobalRelabelingHelper GlobalRelabeling

	RunSynchronousGlobalRelabel func()

	SkipPush               = atomic.Bool{}
	SkipRestoreHeightInvar = atomic.Bool{}
)

func (gr *GlobalRelabeling) Reset() {
	gr.interval.Store(grMinInterval)
	gr.sinkId.Store(EmptyValue)
	gr.sourceId.Store(EmptyValue)
	gr.grCount.Store(0)
	gr.nextGrCount.Store(gr.interval.Load())
}

func (gr *GlobalRelabeling) UpdateInterval(v int64) {
	gr.interval.Store(utils.Max(int64(grAlpha*float64(v)), grMinInterval))
}

func (gr *GlobalRelabeling) RegisterSource(id uint32) {
	gr.sourceId.Store(id)
	Assert(gr.interval.Load() > 0, "Invalid Global Relabeling interval")
}

func (gr *GlobalRelabeling) RegisterSink(id uint32) {
	gr.sinkId.Store(id)
	Assert(gr.interval.Load() > 0, "Invalid Global Relabeling interval")
}

func (gr *GlobalRelabeling) OnLift(sendMsg func(sinkId uint32) uint64) (sent uint64) {
	// TODO: improve performance
	newCount := gr.grCount.Add(1)
	nextGrCount := gr.nextGrCount.Load()
	if newCount >= nextGrCount {
		gr.UpdateInterval(VertexCountHelper.GetMaxVertexCount())
		swapped := gr.nextGrCount.CompareAndSwap(nextGrCount, newCount+gr.interval.Load())
		if swapped {
			log.Info().Msg("Global Relabeling is triggered")
			// Source and sink should always be present when lift
			sent += sendMsg(gr.sinkId.Load())
			sent += sendMsg(gr.sourceId.Load())
			if SynchronousGlobalRelabeling {
				RunSynchronousGlobalRelabel()
			}
		}
	}
	return
}

func SyncGlobalRelabel[V graph.VPI[V], E graph.EPI[E], M graph.MVI[M], N any](
	g *graph.Graph[V, E, M, N],
	resetHeights func(*graph.Graph[V, E, M, N]),
	sendMsgToActiveVertices func(*graph.Graph[V, E, M, N]) int,
) {
	Assert(!g.Options.LogTimeseries, "Not supported since queries also use graph.EPOCH")
	log.Info().Msg("SyncGlobalRelabel starts.")

	// Drain messages
	t0 := time.Now()
	SkipPush.Store(true)
	SkipRestoreHeightInvar.Store(true)
	g.Broadcast(graph.EPOCH)
	g.AwaitAck()

	// Reset Heights
	t1 := time.Now()
	resetHeights(g)
	sourceInternalId, _ := g.NodeVertexFromRaw(SourceRawId)
	sinkInternalId, _ := g.NodeVertexFromRaw(SinkRawId)

	// Global relabeling
	t2 := time.Now()
	SkipRestoreHeightInvar.Store(false)
	// Send messages to source and sink and vertices with negative excess
	sMailbox, sTidx := g.NodeVertexMailbox(sourceInternalId)
	g.GraphThreads[sTidx].MsgSend += g.EnsureSend(g.ActiveNotification(sourceInternalId, graph.Notification[N]{Target: sourceInternalId}, sMailbox, sTidx))
	tMailbox, tTidx := g.NodeVertexMailbox(sinkInternalId)
	g.GraphThreads[tTidx].MsgSend += g.EnsureSend(g.ActiveNotification(sinkInternalId, graph.Notification[N]{Target: sinkInternalId}, tMailbox, tTidx))
	// Run
	g.ResetTerminationState()
	g.Broadcast(graph.RESUME)
	g.Broadcast(graph.EPOCH)
	g.AwaitAck()

	// Resume
	t3 := time.Now()
	SkipPush.Store(false)
	// Send messages to vertices with excess
	activeVertices := sendMsgToActiveVertices(g)
	log.Info().Msg(fmt.Sprintf("Number of active vertices: %v", activeVertices))

	g.ResetTerminationState()
	g.Broadcast(graph.RESUME)

	log.Info().Msg(fmt.Sprintf("SyncGlobalRelabel done. "+
		"Draining Messages took %vs, Resetting heights took %vs, Global Relabeling took %vs. Total took %vs",
		t1.Sub(t0).Seconds(), t2.Sub(t1).Seconds(), t3.Sub(t2).Seconds(), t3.Sub(t0).Seconds()))
}
