package main

import (
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
	"sync"
	"time"
)

func PeriodicGlobalResetRunnable(f *Framework, g *Graph, exit *bool, period time.Duration) {
	time.Sleep(period)
	for !*exit {
		GlobalReset(f, g)
		time.Sleep(period)
	}
}

func GlobalReset(f *Framework, g *Graph) {
	watch := mathutils.Watch{}
	info("Starting GlobalReset")
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
				info("Current sink excess: ", v.Excess)
			}
		}
		for i := range v.Nbrs {
			v.Nbrs[i] = Nbr{
				Height: InitialHeight,
				ResCap: v.Nbrs[i].ResCap,
			}
		}
	}
	// resume flow push and height change
	resetPhase = false
	g.Mutex.Unlock()
	info("GlobalReset runtime: ", watch.Elapsed())
}

func processAllMessages(f *Framework, g *Graph) {
	wg := sync.WaitGroup{}
	exit := false
	wg.Add(graph.THREADS)
	f.RunProcessMessages(g, &wg, &exit)
	wg.Wait()
}
