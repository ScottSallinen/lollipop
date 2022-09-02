package framework

import (
	"sync"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func (frame *Framework[VertexProp, EdgeProp]) OnQueueVisitSync(g *graph.Graph[VertexProp, EdgeProp], sidx uint32, didx uint32, VisitData float64) {
	target := &g.Vertices[didx]
	//target.Mutex.Lock()
	newInfo := frame.MessageAggregator(target, didx, sidx, VisitData)
	//target.Mutex.Unlock()
	if newInfo {
		atomic.StoreInt32(&target.IsActive, 1)
	}
}

func (frame *Framework[VertexProp, EdgeProp]) ConvergeSync(g *graph.Graph[VertexProp, EdgeProp], wg *sync.WaitGroup) {
	info("ConvergeSync")
	if g.SourceInit {
		sidx := g.VertexMap[g.SourceVertex]
		frame.OnVisitVertex(g, sidx, g.InitVal)
	}
	iteration := 0
	for {
		someVertexActive := 0
		mathutils.BatchParallelFor(len(g.Vertices), graph.THREADS, func(vidx int, tidx int) {
			target := &g.Vertices[vidx]
			if !g.SourceInit && iteration == 0 {
				frame.OnQueueVisitSync(g, uint32(vidx), uint32(vidx), g.InitVal)
			}
			active := atomic.SwapInt32(&target.IsActive, 0) == 1
			if active {
				msgVal := frame.AggregateRetrieve(target)
				createsNewActivity := frame.OnVisitVertex(g, uint32(vidx), msgVal)
				if createsNewActivity > 0 {
					someVertexActive = 1
				}
			}
		})
		iteration++

		//frame.OnCompareOracle(g)

		if someVertexActive != 1 {
			break
		}
	}
	info("Sync iterations: ", iteration)
}
