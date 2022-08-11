package main

import (
	"flag"
	"log"
	"net/http"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

// This function is mostly optional, but is a good way to describe
// an algorithm that can check for correctness of a result (outside just the go tests)
// For example, for breadth first search, you wouldn't expect a neighbour to be more than
// one hop away; for graph colouring you wouldn't expect two neighbours to have the same colour,
// etc. This can codify the desire to ensure correct behaviour.
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

	// Some potential extra defines here, for if the algorithm has a "point" initialization
	// or is instead initialized by default behaviour (where every vertex is visited initially)
	// g.SourceInit = true
	// g.SourceInitVal = 1.0
	// g.EmptyVal = math.MaxFloat64
	// g.SourceVertex = rawSrc

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

	g := LaunchGraphExecution(*gptr, *aptr, *dptr)
	g.ComputeGraphStats(false, false)
}
