package framework

import (
	"sync"

	"github.com/ScottSallinen/lollipop/graph"
)

func OnQueueVisitSync(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{}) {
	target := &g.Vertices[didx]
	target.Mutex.Lock()
	target.Scratch += VisitData.(float64)
	target.Mutex.Unlock()
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
						target.Mutex.Lock()
						msgVal := 0.0
						msgVal = target.Scratch
						target.Scratch = 0
						target.Mutex.Unlock()
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
		if vertexActive != 1 {
			break
		}
	}
	info("Sync iterations: ", iteration)
}
