package main

import (
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"

	_ "net/http/pprof"
)

func OnCheckCorrectness(g *graph.Graph) error {
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool) *graph.Graph {
	frame := framework.Framework{}
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel

	g := &graph.Graph{}
	g.OnInitVertex = OnInitVertex

	if !dynamic {
		g.LoadGraphStatic(gName)
	}

	frame.Init(g, async, dynamic)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	if dynamic {
		go g.LoadGraphDynamic(gName, &feederWg)
	}

	frame.Run(g, &feederWg, &frameWait)
	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()
	gName := *gptr
	doAsync := *aptr
	doDynamic := *dptr
	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	runAsync := doAsync
	if doDynamic {
		runAsync = true
	}
	g := LaunchGraphExecution(gName, runAsync, doDynamic)
	g.ComputeGraphStats(false, false)
}
