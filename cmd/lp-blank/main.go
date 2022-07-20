package main

import (
	"flag"
	"log"
	"net/http"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/gorilla/mux"
)

func OnCheckCorrectness(g *graph.Graph) error {
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool) *graph.Graph {
	frame := framework.Framework{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve

	g := &graph.Graph{}
	frame.Launch(g, gName, async, dynamic, false)
	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()
	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)

	g := LaunchGraphExecution(*gptr, *aptr, *dptr)
	g.ComputeGraphStats(false, false)
}
