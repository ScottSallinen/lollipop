package main

import (
	"sync/atomic"
)

type VertexCount struct {
	source         uint32
	realCount      int64
	estimatedCount int64
	alpha          float32
}

var VertexCountHelper VertexCount

func (vc *VertexCount) Reset(initialEstimatedCount int64) {
	vc.source = EmptyValue
	vc.realCount = 0
	vc.estimatedCount = initialEstimatedCount
	vc.alpha = 1.1
}

func (vc *VertexCount) NewVertexN() (estimateIncreased bool) {
	newCount := atomic.AddInt64(&vc.realCount, 1)
	estimatedCount := atomic.LoadInt64(&vc.estimatedCount)
	if newCount > estimatedCount {
		newEstimatedCount := int64(float32(newCount)*vc.alpha) + 1
		if atomic.CompareAndSwapInt64(&vc.estimatedCount, estimatedCount, newEstimatedCount) {
			return true
		}
	}
	return false
}

// Depreciated
func (vc *VertexCount) NewVertex() (source uint32) {
	estimateIncreased := vc.NewVertexN()
	if estimateIncreased {
		return atomic.LoadUint32(&vc.source)
	}
	return EmptyValue
}

func (vc *VertexCount) RegisterSource(sourceInternalId uint32) (currentCount int64) {
	atomic.StoreUint32(&vc.source, sourceInternalId)
	return vc.GetMaxVertexCount()
}

func (vc *VertexCount) GetMaxVertexCount() int64 {
	return atomic.LoadInt64(&vc.estimatedCount)
}

func (vc *VertexCount) GetSourceId() uint32 {
	return atomic.LoadUint32(&vc.source)
}
