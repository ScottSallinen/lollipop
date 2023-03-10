package main

import (
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
	"sync"
	"time"
)

var ENABLE_BFS_PHASE = true

func PeriodicGlobalResetRunnable(f *Framework, g *Graph, exit *bool, period time.Duration) {
	time.Sleep(period)
	for !*exit {
		GlobalRelabel(f, g)
		time.Sleep(period)
	}
}

func GlobalRelabel(f *Framework, g *Graph) {
	watch := mathutils.Watch{}
	info("Starting GlobalRelabel")
	watch.Start()
	g.Mutex.Lock()
	// set a flag to prevent flow push and height change
	resetPhase = true
	// process all existing messages
	processAllMessages(f, g)
	// Update Vertex height and Nbrs height
	for vi := range g.Vertices {
		v := &g.Vertices[vi].Property
		oldHeight := v.Height
		v.Height = InitialHeight
		if v.Type == Source || v.Type == Sink || v.Excess < 0 {
			// let it broadcast its height after resuming execution
			updateHeight(g, uint32(vi), oldHeight)
			if v.Type == Sink {
				info("    Current sink excess: ", v.Excess, " height: ", v.Height)
			}
		}
		for i := range v.Nbrs {
			v.Nbrs[i] = Nbr{
				Height: InitialHeight,
				ResCap: v.Nbrs[i].ResCap,
			}
		}
	}
	resetRuntime := watch.Elapsed()
	info("    Reset Phase runtime: ", resetRuntime)
	resetPhase = false
	// BFS phase
	if ENABLE_BFS_PHASE {
		bfsPhase = true
		processAllMessages(f, g)
		info("    BFS Phase runtime: ", watch.Elapsed()-resetRuntime)
		bfsPhase = false
		// resume flow push and height change
		excessVertices := make([]uint32, 0)
		for vi := range g.Vertices {
			v := &g.Vertices[vi].Property
			if v.Excess != 0 {
				send(g, uint32(vi), uint32(vi), 0)
				excessVertices = append(excessVertices, uint32(vi))
			}
		}
		info("    excessVertices ", excessVertices)
	}
	g.Mutex.Unlock()
	info("    GlobalRelabel runtime: ", watch.Elapsed())
}

func processAllMessages(f *Framework, g *Graph) {
	wg := sync.WaitGroup{}
	exit := false
	wg.Add(graph.THREADS)
	f.RunProcessMessages(g, &wg, &exit)
	wg.Wait()
}
