package main

import "sync"

type VertexCount struct {
	lock           sync.Mutex
	subscribers    map[uint32]bool
	realCount      int64
	estimatedCount int64
}

var VertexCountHelper VertexCount

func (vc *VertexCount) Reset(realCount int64) {
	vc.lock.Lock()
	vc.subscribers = make(map[uint32]bool)
	vc.realCount = realCount
	vc.estimatedCount = 0
	vc.lock.Unlock()
}

func (vc *VertexCount) NewVertex(g *Graph, sidx uint32) (msgSent int) {
	vc.lock.Lock()
	vc.realCount += 1
	if vc.realCount > vc.estimatedCount {
		vc.estimatedCount += 100 // OPTIMIZED
		for vidx, subscribing := range vc.subscribers {
			if subscribing {
				msgSent += sendNewMaxVertexCount(g, sidx, vidx, vc.estimatedCount)
			}
		}
	}
	vc.lock.Unlock()
	return
}

func (vc *VertexCount) UpdateSubscriber(g *Graph, vidx uint32, subscribe bool) (msgSent int) {
	vc.lock.Lock()
	if subscribe {
		vc.subscribers[vidx] = true
		msgSent += sendNewMaxVertexCount(g, vidx, vidx, vc.estimatedCount)
	} else {
		delete(vc.subscribers, vidx)
	}
	vc.lock.Unlock()
	return
}

func (vc *VertexCount) GetMaxVertexCount() int64 {
	vc.lock.Lock()
	count := vc.estimatedCount
	vc.lock.Unlock()
	return count
}
