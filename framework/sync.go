package framework

import (
	"sync"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
)

func (frame *Framework[VertexProp, EdgeProp]) OnQueueVisitSync(g *graph.Graph[VertexProp, EdgeProp], sidx uint32, didx uint32, VisitData float64) {
	target := &g.Vertices[didx]
	//target.Mutex.Lock()
	newInfo := frame.MessageAggregator(target, &g.Vertices[sidx], VisitData)
	//target.Mutex.Unlock()
	if newInfo {
		atomic.StoreInt32(&target.IsActive, 1)
	}
}

func (frame *Framework[VertexProp, EdgeProp]) ConvergeSync(g *graph.Graph[VertexProp, EdgeProp], wg *sync.WaitGroup) {
	info("ConvergeSync")
	if g.SourceInit {
		sidx := g.VertexMap[g.SourceVertex]
		frame.OnVisitVertex(g, sidx, g.SourceInitVal)
	}
	iteration := 0
	for {
		vertexActive := 0
		var wg sync.WaitGroup
		wg.Add(graph.THREADS)
		batch := uint32(len(g.Vertices) / graph.THREADS)
		for t := uint32(0); t < uint32(graph.THREADS); t++ {
			go func(tidx uint32, iteration int) {
				defer wg.Done()
				start := tidx * batch
				end := (tidx + 1) * batch
				if tidx == uint32(graph.THREADS-1) {
					end = uint32(len(g.Vertices))
				}
				for j := start; j < end; j++ {
					target := &g.Vertices[j]
					active := atomic.SwapInt32(&target.IsActive, 0) == 1
					if target.Scratch != g.EmptyVal || active || iteration == 0 {
						//target.Mutex.Lock()
						msgVal := frame.AggregateRetrieve(target)
						//target.Mutex.Unlock()
						mActive := frame.OnVisitVertex(g, j, msgVal)
						if mActive > 0 {
							vertexActive = 1
						}
					}
				}
			}(t, iteration)
		}

		wg.Wait()
		iteration++

		//frame.OnCompareOracle(g)

		if vertexActive != 1 {
			break
		}
	}
	info("Sync iterations: ", iteration)
}
