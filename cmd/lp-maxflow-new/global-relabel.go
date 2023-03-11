package main

import (
	"github.com/ScottSallinen/lollipop/mathutils"
	"sync"
	"time"
)

var ENABLE_BFS_PHASE = true

func StartPeriodicGlobalReset(f *Framework, g *Graph, period time.Duration, exit *chan bool) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(1)
	resetPhase = false
	bfsPhase = false
	go PeriodicGlobalResetRunnable(f, g, period, exit, &wg)
	return &wg
}

func PeriodicGlobalResetRunnable(f *Framework, g *Graph, period time.Duration, exit *chan bool, wg *sync.WaitGroup) {
loop:
	for {
		select {
		case <-*exit:
			break loop
		case <-time.After(period):
		}
		GlobalRelabel(f, g)
	}
	wg.Done()
}

func GlobalRelabel(f *Framework, g *Graph) {
	watch := mathutils.Watch{}
	info("Starting GlobalRelabel")
	watch.Start()
	g.Mutex.Lock()
	// set a flag to prevent flow push and height change
	resetPhase = true
	// process all existing messages
	f.ProcessAllMessages(g)
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
		f.ProcessAllMessages(g)
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
	g.ResetVotes()
	g.Mutex.Unlock()
	info("    GlobalRelabel runtime: ", watch.Elapsed())
}