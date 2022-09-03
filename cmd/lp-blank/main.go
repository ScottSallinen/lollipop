package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"strings"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

// Function which defines how to convert a line of text into an edge
func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)
	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])
	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty{}}
}

// This function is mostly optional, but is a good way to describe
// an algorithm that can check for correctness of a result (outside just the go tests)
// For example, for breadth first search, you wouldn't expect a neighbour to be more than
// one hop away; for graph colouring you wouldn't expect two neighbours to have the same colour,
// etc. This can codify the desire to ensure correct behaviour.
func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracle bool, undirected bool) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
	frame := framework.Framework[VertexProperty, EdgeProperty, MessageValue]{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.OnEdgeAddRev = OnEdgeAddRev
	frame.OnEdgeDelRev = OnEdgeDelRev
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve
	frame.EdgeParser = EdgeParser

	g := &graph.Graph[VertexProperty, EdgeProperty, MessageValue]{}

	// Some potential extra defines here, for if the algorithm has a "point" initialization
	// or is instead initialized by default behaviour (where every vertex is visited initially)
	// g.SourceInit = true
	// g.SourceVertex = rawSrc

	g.InitVal = 1.0
	g.EmptyVal = EMPTYVAL
	// g.SendRevMsgs = false

	frame.Launch(g, gName, async, dynamic, oracle, undirected)
	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	uptr := flag.Bool("u", false, "Interpret the input graph as undirected (add transpose edges)")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
	pptr := flag.Bool("p", false, "Save vertex properties to disk")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()

	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(*gptr, *aptr, *dptr, false, *uptr)
	g.ComputeGraphStats(false, false)

	if *pptr {
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}
