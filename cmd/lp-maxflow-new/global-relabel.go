package main

import (
	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
	"math"
	"sync"
	"time"
)

func StartPeriodicGlobalReset(f *Framework, g *Graph, delay time.Duration, interval time.Duration, lockGraph bool, exit *chan bool) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(1)
	resetPhase = false
	bfsPhase = false
	go PeriodicGlobalResetRunnable(f, g, delay, interval, lockGraph, exit, &wg)
	return &wg
}

func PeriodicGlobalResetRunnable(f *Framework, g *Graph, delay time.Duration, interval time.Duration, lockGraph bool, exit *chan bool, wg *sync.WaitGroup) {
	select {
	case <-*exit:
		wg.Done()
		return
	case <-time.After(delay):
	}
loop:
	for {
		GlobalRelabel(f, g, lockGraph)
		select {
		case <-*exit:
			break loop
		case <-time.After(interval):
		}
	}
	wg.Done()
}

func GlobalRelabel(f *Framework, g *Graph, lockGraph bool) {
	watch := mathutils.Watch{}
	info("Starting GlobalRelabel")
	watch.Start()
	if lockGraph {
		g.Mutex.Lock()
	} else {
		enforce.ENFORCE(g.Mutex.TryLock() == false)
	}

	// set a flag to prevent flow push and height change
	resetPhase = true
	// process all existing messages
	f.ProcessAllMessages(g)
	// Update Vertex height and Nbrs height
	positiveVertices := 0
	negativeVertices := 0
	for vi := range g.Vertices {
		v := &g.Vertices[vi].Property
		oldHeight := v.Height
		v.Height = math.MaxUint32
		if v.Type == Source || v.Type == Sink || v.Excess < 0 {
			// let it broadcast its height after resuming execution
			updateHeight(g, uint32(vi), oldHeight)
			if v.Type == Sink {
				info("    Current sink excess: ", v.Excess, " height: ", v.Height)
			}
		}
		for i, n := range v.Nbrs {
			v.Nbrs[i] = Nbr{
				Height: math.MaxUint32,
				ResCap: n.ResCap,
			}
		}
		if v.Excess > 0 {
			positiveVertices += 1
		} else if v.Excess < 0 {
			negativeVertices += 1
		}
	}
	resetRuntime := watch.Elapsed()
	info("    excessVertices count positive ", positiveVertices, " negative ", negativeVertices)
	info("    Reset Phase runtime: ", resetRuntime)
	resetPhase = false

	// BFS phase
	bfsPhase = true
	f.ProcessAllMessages(g)
	info("    BFS Phase runtime: ", watch.Elapsed()-resetRuntime)
	bfsPhase = false
	// resume flow push and height change
	for vi := range g.Vertices {
		v := &g.Vertices[vi].Property
		if v.Excess != 0 {
			send(g, uint32(vi), uint32(vi), 0)
		}
	}

	g.ResetVotes()
	if lockGraph {
		g.Mutex.Unlock()
	}
	info("    GlobalRelabel runtime: ", watch.Elapsed())
}
