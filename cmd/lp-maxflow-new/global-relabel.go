package main

import (
	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
	"math"
	"sync"
	"sync/atomic"
)

const ALPHA = 6
const BETA = 12
const minGrInterval = 100

var grFrame *Framework
var earliestNextGrCount = int64(0)
var relabelCount = int64(0)
var GrInterval = int64(minGrInterval)
var grShouldRun = int64(0)

func onRelabeled(g *Graph) {
	newCount := atomic.AddInt64(&relabelCount, 1)
	mEarliestNextGrCount := atomic.LoadInt64(&earliestNextGrCount)
	if newCount >= mEarliestNextGrCount {
		swapped := atomic.CompareAndSwapInt64(&grShouldRun, 0, 1)
		if swapped {
			info("GlobalRelabelTriggered")
			if g.Options.LogTimeseries {
				resetPhase = true
			}
			go GlobalRelabel(grFrame, g, true)
		}
	}
}

func GlobalRelabel(f *Framework, g *Graph, lockGraph bool) {
	if atomic.LoadInt64(&grShouldRun) != 1 {
		return
	}
	if lockGraph {
		g.Mutex.Lock()
		defer g.Mutex.Unlock()
	} else {
		enforce.ENFORCE(g.Mutex.TryLock() == false)
	}
	if atomic.LoadInt64(&grShouldRun) != 1 {
		return
	}
	watch := mathutils.Watch{}
	info("Starting GlobalRelabel")
	watch.Start()

	// set a flag to prevent flow push and height change
	resetPhase = true
	// process all existing messages
	f.ProcessAllMessages(g)
	// Update Vertex height and Nbrs height
	positiveVertices := make([]int, graph.THREADS)
	negativeVertices := make([]int, graph.THREADS)
	parallelForEachVertex(g, func(vi uint32, ti uint32) {
		v := &g.Vertices[vi].Property
		oldHeight := v.Height
		v.Height = math.MaxUint32
		if v.Type == Source || v.Type == Sink || v.Excess < 0 {
			// let it broadcast its height after resuming execution
			updateHeight(g, vi, oldHeight)
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
			positiveVertices[ti] += 1
		} else if v.Excess < 0 {
			negativeVertices[ti] += 1
		}
	})
	resetRuntime := watch.Elapsed()
	info("    excessVertices count positive ", mathutils.Sum(positiveVertices), " negative ", mathutils.Sum(negativeVertices))
	info("    Reset Phase runtime: ", resetRuntime)
	resetPhase = false

	// BFS phase
	bfsPhase = true
	f.ProcessAllMessages(g)
	info("    BFS Phase runtime: ", watch.Elapsed()-resetRuntime)
	bfsPhase = false
	// resume flow push and height change
	parallelForEachVertex(g, func(vi uint32, _ uint32) {
		v := &g.Vertices[vi].Property
		if v.Excess != 0 {
			send(g, vi, vi, 0)
		}
	})

	g.ResetVotes()
	SetNextEarliestGrTime()
}

func SetNextEarliestGrTime() {
	nextCount := atomic.LoadInt64(&relabelCount) + GrInterval
	atomic.StoreInt64(&earliestNextGrCount, nextCount)
	atomic.StoreInt64(&grShouldRun, 0)
}

func UpdateGrInterval(n, m int) {
	GrInterval = mathutils.Max(int64((ALPHA*n+m/3)/BETA/10000), minGrInterval)
}

func parallelForEachVertex(g *Graph, applicator func(vidx uint32, tidx uint32)) {
	var wg sync.WaitGroup
	wg.Add(graph.THREADS)
	n := uint32(len(g.Vertices))
	for t := uint32(0); t < uint32(graph.THREADS); t++ {
		go func(tidx uint32) {
			defer wg.Done()
			for vidx := uint32(0); vidx < n; vidx += 1 {
				if g.Vertices[vidx].ToThreadIdx() == tidx {
					applicator(vidx, tidx)
				}
			}
		}(t)
	}
	wg.Wait()
}
