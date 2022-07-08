package framework

import (
	"sync"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func OnQueueVisitSync(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{}) {
	target := &g.Vertices[didx]
	mathutils.AtomicAddFloat64(&target.Scratch, VisitData.(float64))
}

func (frame *Framework) ConvergeSync(g *graph.Graph, wg *sync.WaitGroup) {
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
					if target.Scratch != 0 || iteration == 0 {
						msgVal := mathutils.AtomicSwapFloat64(&target.Scratch, 0.0)
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
