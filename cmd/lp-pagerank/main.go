package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/cmd/common"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...any) {
	log.Println("[Pagerank]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 2 || sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty{}}
}

// OnCheckCorrectness: Performs some sanity checks for correctness.
func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty]) error {
	sum := 0.0
	sumsc := 0.0
	resid := 0.0
	for vidx := range g.Vertices {
		sum += g.Vertices[vidx].Property.Value
		resid += g.Vertices[vidx].Property.Residual
		sumsc += g.Vertices[vidx].Scratch
	}
	totalAbs := (sum) / float64(len(g.Vertices))
	totalResid := (resid) / float64(len(g.Vertices))
	totalScratch := (sumsc) / float64(len(g.Vertices))
	total := totalAbs + totalResid + totalScratch

	if !mathutils.FloatEquals(total, INITMASS) {
		info("Total absorbed: ", totalAbs)
		info("Total residual: ", totalResid)
		info("Total scratch: ", totalScratch)
		info("Total sum mass: ", total)
		return errors.New("final mass not equal to init")
	}
	return nil
}

func OracleComparison(g *graph.Graph[VertexProperty, EdgeProperty], oracle *graph.Graph[VertexProperty, EdgeProperty], resultCache *[]float64) {
	ia := make([]float64, len(g.Vertices))
	ib := make([]float64, len(g.Vertices))
	numEdges := uint64(0)

	for v := range g.Vertices {
		ia[v] = oracle.Vertices[v].Property.Value
		ib[v] = g.Vertices[v].Property.Value
		numEdges += uint64(len(g.Vertices[v].OutEdges))
	}

	// TODO: should be parameterized...
	const ORACLEEDGES = 28511807
	const ORACLEVERTICES = 1791489

	if resultCache == nil && numEdges == ORACLEEDGES {
		*resultCache = make([]float64, len(ia))
		copy(*resultCache, ia)
	}
	if resultCache != nil {
		copy(ia, *resultCache)
	}
	info("vertexCount ", uint64(len(g.Vertices)), " edgeCount ", numEdges, " vertexPct ", (len(g.Vertices)*100)/ORACLEVERTICES, " edgePct ", (numEdges*100)/ORACLEEDGES)
	graph.ResultCompare(ia, ib)

	iaRank := mathutils.NewIndexedFloat64Slice(ia)
	ibRank := mathutils.NewIndexedFloat64Slice(ib)
	sort.Sort(sort.Reverse(iaRank))
	sort.Sort(sort.Reverse(ibRank))

	topN := 1000
	topK := 100
	if len(iaRank.Idx) < topN {
		topN = len(iaRank.Idx)
	}
	if len(iaRank.Idx) < topK {
		topK = len(iaRank.Idx)
	}
	iaRk := make([]int, topK)
	copy(iaRk, iaRank.Idx[:topK])
	ibRk := make([]int, topK)
	copy(ibRk, ibRank.Idx[:topK])

	mRBO6 := mathutils.CalculateRBO(iaRank.Idx[:topN], ibRank.Idx[:topN], 0.6)
	mRBO9 := mathutils.CalculateRBO(iaRk, ibRk, 0.9)
	info("top", topN, " RBO6 ", fmt.Sprintf("%.4f", mRBO6*100.0), " top", topK, " RBO9 ", fmt.Sprintf("%.4f", mRBO9*100.0))
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracleRun bool, oracleFin bool, undirected bool) *graph.Graph[VertexProperty, EdgeProperty] {
	frame := framework.Framework[VertexProperty, EdgeProperty]{}
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

	g := &graph.Graph[VertexProperty, EdgeProperty]{}
	g.EmptyVal = 0.0

	frame.Launch(g, gName, async, dynamic, oracleRun, undirected)

	if oracleFin {
		frame.CompareToOracle(g)
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
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()

	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//debug.SetGCPercent(-1)
	//runtime.SetMutexProfileFraction(1)

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(*gptr, *aptr, *dptr, *optr, *fptr, *uptr)

	g.ComputeGraphStats(false, false)

	if *pptr {
		graphName := common.ExtractGraphName(*gptr)
		common.WriteVertexProps(g, graphName, *dptr)
	}
}
