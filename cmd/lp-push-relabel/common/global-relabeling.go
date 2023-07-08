package common

import (
	"sync/atomic"

	"github.com/rs/zerolog/log"

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
	grAlpha       = 6
	grBeta        = 120_000
	grMinInterval = 100
)

var GlobalRelabelingHelper GlobalRelabeling

func (gr *GlobalRelabeling) Reset() {
	gr.sinkId.Store(EmptyValue)
	gr.sourceId.Store(EmptyValue)
	gr.grCount.Store(0)
	gr.nextGrCount.Store(gr.interval.Load())
}

func (gr *GlobalRelabeling) UpdateInterval(v, e int64) {
	gr.interval.Store(utils.Max((grAlpha*v+e/3)/grBeta, grMinInterval))
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
		swapped := gr.nextGrCount.CompareAndSwap(nextGrCount, newCount+gr.interval.Load())
		if swapped {
			log.Info().Msg("Global Relabeling is triggered")
			// Source and sink should always be present when lift
			sent += sendMsg(gr.sinkId.Load())
			sent += sendMsg(gr.sourceId.Load())
		}
	}
	return
}
