package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

func info(args ...any) {
	log.Println("[SSSP]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 2 || sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])

	weight := 1.0
	var err error
	if sflen >= 3 {
		weight, err = strconv.ParseFloat(stringFields[2], 32)
		enforce.ENFORCE(err, "Text file parse error: weight not floats?")
	}

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty{Weight: weight}}
}

// OnCheckCorrectness: Performs some sanity checks for correctness.
func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	maxValue := 0.0
	// Denote vertices that claim unvisted, and ensure out edges are at least as good as we could provide
	for vidx := range g.Vertices {
		ourValue := g.Vertices[vidx].Property.Value
		if ourValue < EMPTYVAL {
			maxValue = math.Max(maxValue, ourValue)
		}

		if initVal, ok := g.Options.InitMessages[g.Vertices[vidx].Id]; ok {
			enforce.ENFORCE(ourValue == float64(initVal), ourValue)
		}
		if ourValue == EMPTYVAL { // we were never visted

		} else {
			for eidx := range g.Vertices[vidx].OutEdges {
				target := g.Vertices[vidx].OutEdges[eidx].Destination
				enforce.ENFORCE(g.Vertices[target].Property.Value <= (ourValue + g.Vertices[vidx].OutEdges[eidx].Property.Weight))
			}
		}
	}
	info("maxValue ", maxValue)
	return nil
}

func OracleComparison(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], oracle *graph.Graph[VertexProperty, EdgeProperty, MessageValue], resultCache *[]float64, cache bool) {
	ia := make([]float64, len(g.Vertices))
	ib := make([]float64, len(g.Vertices))
	numEdges := uint64(0)

	for v := range g.Vertices {
		ia[v] = oracle.Vertices[v].Property.Value
		ib[v] = g.Vertices[v].Property.Value
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
	graph.ResultCompare(ia, ib, 0)
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracleRun bool, oracleFin bool, rawSrc uint32, undirected bool) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
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
	g.Options = graph.GraphOptions[MessageValue]{
		Undirected:    undirected,
		OracleCompare: oracleRun,
		SourceInit:    true,
		InitMessages:  map[uint32]MessageValue{rawSrc: 1.0},
		EmptyVal:      EMPTYVAL,
	}

	frame.Launch(g, gName, async, dynamic)

	if oracleFin {
		frame.CompareToOracle(g, false, true, 0)
	}

	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
	uptr := flag.Bool("u", false, "Interpret the input graph as undirected (add transpose edges)")
	optr := flag.Bool("o", false, "Compare to oracle results during runtime")
	fptr := flag.Bool("f", false, "Compare to oracle results (computed via async) upon finishing the initial algorithm.")
	pptr := flag.Bool("p", false, "Save vertex properties to disk")
	iptr := flag.Int("i", 1, "Init vertex (raw id)")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()
	gName := *gptr
	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(gName, *aptr, *dptr, *optr, *fptr, uint32(*iptr), *uptr)

	g.ComputeGraphStats(false, false)

	if *pptr {
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}
