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
	if g.Options.SourceInit {
		// Initial visits might arrive after other visits
		for vid, message := range g.Options.InitMessages {
			vidx := g.VertexMap[vid]
			frame.MessageAggregator(&g.Vertices[vidx], vidx, vidx, message)
			aggregated := frame.AggregateRetrieve(&g.Vertices[vidx])
			frame.OnVisitVertex(g, vidx, aggregated)
		}
	}
	iteration := 0
	for {
		someVertexActive := 0
		mathutils.BatchParallelFor(len(g.Vertices), graph.THREADS, func(vidx int, tidx int) {
			target := &g.Vertices[vidx]
			if !g.Options.SourceInit && iteration == 0 {
				frame.OnQueueVisitSync(g, uint32(vidx), uint32(vidx), g.Options.InitAllMessage)
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

		if g.Options.OracleCompareSync {
			frame.CompareToOracle(g, true, true, 0)
		}

		if someVertexActive != 1 {
			break
		}
	}
	info("Sync iterations: ", iteration)
}

// A sync variant that only only consumes messages generated from the previous iteration.
// Does not consider messages generated from the current iteration.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) ConvergeSyncPrevOnly(g *graph.Graph[VertexProp, EdgeProp, MsgType], wg *sync.WaitGroup) {
	info("ConvergeSyncPrevOnly")
	if g.Options.SourceInit {
		for vid, message := range g.Options.InitMessages {
			vidx := g.VertexMap[vid]
			frame.MessageAggregator(&g.Vertices[vidx], vidx, vidx, message)
			aggregated := frame.AggregateRetrieve(&g.Vertices[vidx])
			frame.OnVisitVertex(g, vidx, aggregated)
		}
	}
	iteration := 0
	msgsLast := make([]MsgType, len(g.Vertices))
	for {
		frontier := make([]bool, len(g.Vertices))
		someVertexActive := 0
		mathutils.BatchParallelFor(len(g.Vertices), graph.THREADS, func(vidx int, tidx int) {
			target := &g.Vertices[vidx]
			if !g.Options.SourceInit && iteration == 0 {
				frame.OnQueueVisitSync(g, uint32(vidx), uint32(vidx), g.Options.InitAllMessage)
			}
			active := atomic.SwapInt32(&target.IsActive, 0) == 1
			if active {
				msgsLast[vidx] = frame.AggregateRetrieve(target)
				frontier[vidx] = true
			}
		})
		mathutils.BatchParallelFor(len(g.Vertices), graph.THREADS, func(vidx int, tidx int) {
			if frontier[vidx] {
				createsNewActivity := frame.OnVisitVertex(g, uint32(vidx), msgsLast[vidx])
				if createsNewActivity > 0 {
					someVertexActive = 1
				}
			}
		})
		iteration++

		if g.Options.OracleCompareSync {
			frame.CompareToOracle(g, true, true, 0)
		}

		if someVertexActive != 1 {
			break
		}
	}
	info("SyncPrevOnly iterations: ", iteration)
}
