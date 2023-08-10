package n

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

type GlobalRelabeling struct {
	LiftCountInterval atomic.Int64
	TimeInterval      atomic.Int64

	NextGrLiftCount atomic.Int64
	NextGrTime      atomic.Int64

	CurrentLiftCount atomic.Int64

	SinkId   atomic.Uint32
	SourceId atomic.Uint32

	Triggered atomic.Bool

	runSyncGlobalRelabel func(g *Graph)
	sendMsg              func(g *Graph, sourceId, targetId uint32) uint64
	getVertexCount       func() uint64
}

const (
	grAlphaLiftCount = float64(2.0)
	grAlphaTime      = float64(5.0)
	grMinInterval    = 128

	SynchronousGlobalRelabeling = true
)

func (gr *GlobalRelabeling) Reset(runSyncGlobalRelabel func(g *Graph), sendMsg func(g *Graph, sourceId, targetId uint32) uint64, getVertexCount func() uint64) {
	gr.LiftCountInterval.Store(grMinInterval)
	gr.TimeInterval.Store(time.Second.Milliseconds())
	gr.NextGrLiftCount.Store(gr.LiftCountInterval.Load())
	gr.NextGrTime.Store(time.Now().UnixMilli() + gr.TimeInterval.Load())
	gr.CurrentLiftCount.Store(0)
	gr.SinkId.Store(EmptyValue)
	gr.SourceId.Store(EmptyValue)
	gr.Triggered.Store(false)
	gr.runSyncGlobalRelabel = runSyncGlobalRelabel
	gr.sendMsg = sendMsg
	gr.getVertexCount = getVertexCount
}

func (gr *GlobalRelabeling) UpdateInterval(v uint64) {
	gr.LiftCountInterval.Store(utils.Max(int64(grAlphaLiftCount*float64(v)), grMinInterval))
}

func (gr *GlobalRelabeling) UpdateTimeInterval(lastGrRuntimeMilli int64) {
	gr.TimeInterval.Store(int64(grAlphaTime * float64(lastGrRuntimeMilli)))
}

func (gr *GlobalRelabeling) RegisterSource(id uint32) {
	gr.SourceId.Store(id)
	Assert(gr.LiftCountInterval.Load() > 0, "Invalid Global Relabeling interval")
}

func (gr *GlobalRelabeling) RegisterSink(id uint32) {
	gr.SinkId.Store(id)
	Assert(gr.LiftCountInterval.Load() > 0, "Invalid Global Relabeling interval")
}

func (gr *GlobalRelabeling) OnLift(g *Graph, id uint32) (sent uint64) {
	// TODO: improve performance
	newCount := gr.CurrentLiftCount.Add(1)
	shouldGrLiftCount, shouldGrTime := newCount > gr.NextGrLiftCount.Load(), time.Now().UnixMilli() > gr.NextGrTime.Load()
	if shouldGrLiftCount || shouldGrTime {
		if !gr.Triggered.Swap(true) {
			shouldGrLiftCount, shouldGrTime := newCount > gr.NextGrLiftCount.Load(), time.Now().UnixMilli() > gr.NextGrTime.Load()
			if shouldGrLiftCount || shouldGrTime {
				log.Info().Msg(fmt.Sprintf("Global Relabeling is triggered shouldGrLiftCount=%v shouldGrTime=%v", shouldGrLiftCount, shouldGrTime))
				// Source should always be present when lift
				sent += gr.sendMsg(g, id, gr.SinkId.Load())
				sent += gr.sendMsg(g, id, gr.SourceId.Load())
				if SynchronousGlobalRelabeling {
					gr.runSyncGlobalRelabel(g)
				} else {
					gr.GlobalRelabelingDone(time.Second.Milliseconds() * 10)
				}
			}
		}
	}
	return
}

func (gr *GlobalRelabeling) GlobalRelabelingDone(lastGrRuntimeMilli int64) {
	gr.UpdateInterval(gr.getVertexCount())
	gr.UpdateTimeInterval(lastGrRuntimeMilli)

	gr.NextGrLiftCount.Store(gr.CurrentLiftCount.Load() + gr.LiftCountInterval.Load())
	gr.NextGrTime.Store(time.Now().UnixMilli() + gr.TimeInterval.Load())
	gr.Triggered.Swap(false)
}

func (gr *GlobalRelabeling) GetSourceAndSinkInternalIds() (source, sink uint32) {
	return gr.SourceId.Load(), gr.SinkId.Load()
}
