package main

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"math"
	"sync/atomic"
	"unsafe"
)

const ALPHA = 1.1

type VertexCount struct {
	source         uint32
	realCount      int64
	estimatedCount int64
}

var VertexCountHelper VertexCount

func (vc *VertexCount) Reset(initialEstimatedCount int64) {
	log.Info().Msg("Size of VertexCount is " + fmt.Sprint(unsafe.Sizeof(VertexCount{})))
	vc.source = math.MaxUint32
	vc.realCount = 0
	vc.estimatedCount = initialEstimatedCount
}

func (vc *VertexCount) NewVertex() (source uint32) {
	newCount := atomic.AddInt64(&vc.realCount, 1)
	estimatedCount := atomic.LoadInt64(&vc.estimatedCount)
	if newCount > estimatedCount {
		newEstimatedCount := int64(float64(newCount)*ALPHA) + 1
		if atomic.CompareAndSwapInt64(&vc.estimatedCount, estimatedCount, newEstimatedCount) {
			return atomic.LoadUint32(&vc.source)
		}
	}
	return math.MaxUint32
}

func (vc *VertexCount) RegisterSource(sourceInternalId uint32) (currentCount int64) {
	atomic.StoreUint32(&vc.source, sourceInternalId)
	return vc.GetMaxVertexCount()
}

func (vc *VertexCount) GetMaxVertexCount() int64 {
	return atomic.LoadInt64(&vc.estimatedCount)
}
