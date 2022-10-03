package framework

import (
	"sync"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func (frame *Framework[VertexProp, EdgeProp, MsgType]) OnQueueVisitSync(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didx uint32, VisitData MsgType) {
	target := &g.Vertices[didx]
	newInfo := frame.MessageAggregator(target, didx, sidx, VisitData)
	if newInfo {
		atomic.StoreInt32(&target.IsActive, 1)
	}
}

func (frame *Framework[VertexProp, EdgeProp, MsgType]) ConvergeSync(g *graph.Graph[VertexProp, EdgeProp, MsgType], wg *sync.WaitGroup) {
	info("ConvergeSync")
	if g.SourceInit {
		sidx := g.VertexMap[g.SourceVertex]
		frame.MessageAggregator(&g.Vertices[sidx], sidx, sidx, g.InitVal)
		initial := frame.AggregateRetrieve(&g.Vertices[sidx])
		frame.OnVisitVertex(g, sidx, initial)
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
