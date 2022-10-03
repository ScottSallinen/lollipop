package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

func info(args ...any) {
	log.Println("[CC]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 2 || sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst)}
}

// OnCheckCorrectness: Performs some sanity checks for correctness.
func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	// Make sure the labels inside connected components are consistent
	for vidx := range g.Vertices {
		ourValue := g.Vertices[vidx].Property.Value
		for eidx := range g.Vertices[vidx].OutEdges {
			target := g.Vertices[vidx].OutEdges[eidx].Destination
			enforce.ENFORCE(g.Vertices[target].Property.Value == ourValue)
		}
	}
	if graph.DEBUG {
		info("vertex values: ")
		for vidx := range g.Vertices {
			info("# vidx = ", vidx, " Id = ", g.Vertices[vidx].Id, " value = ", g.Vertices[vidx].Property.Value)
		}
	}
	return nil
}

func OracleComparison(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], oracle *graph.Graph[VertexProperty, EdgeProperty, MessageValue], resultCache *[]float64, cache bool) {
	ia := make([]float64, len(g.Vertices))
	ib := make([]float64, len(g.Vertices))
	numEdges := uint64(0)

	for v := range g.Vertices {
		ia[v] = float64(oracle.Vertices[v].Property.Value)
		ib[v] = float64(g.Vertices[v].Property.Value)
		numEdges += uint64(len(g.Vertices[v].OutEdges))
	}

	if resultCache == nil && cache {
		*resultCache = make([]float64, len(ia))
		copy(*resultCache, ia)
	}
	if resultCache != nil {
		copy(ia, *resultCache)
	}
	info("vertexCount ", uint64(len(g.Vertices)), " edgeCount ", numEdges)
	graph.ResultCompare(ia, ib)
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracleRun bool, oracleFin bool) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
	frame := framework.Framework[VertexProperty, EdgeProperty, MessageValue]{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve
	frame.OracleComparison = OracleComparison
	frame.EdgeParser = EdgeParser

	g := &graph.Graph[VertexProperty, EdgeProperty, MessageValue]{}
	// Init all vertexs
	g.SourceInit = false
	g.InitVal = EMPTYVAL
	g.EmptyVal = EMPTYVAL

	frame.Launch(g, gName, async, dynamic, oracleRun, true) // undirected should always be true.

	if oracleFin {
		frame.CompareToOracle(g, true)
	}

	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
	optr := flag.Bool("o", false, "Compare to oracle results during runtime")
	fptr := flag.Bool("f", false, "Compare to oracle results (computed via async) upon finishing the initial algorithm.")
	pptr := flag.Bool("p", false, "Save vertex properties to disk")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()
	gName := *gptr
	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(gName, *aptr, *dptr, *optr, *fptr)

	g.ComputeGraphStats(false, false)

	if *pptr {
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}
