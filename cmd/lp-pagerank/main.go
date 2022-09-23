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

	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...any) {
	log.Println("[Pagerank]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])
	//ts, _ := strconv.ParseFloat(stringFields[2], 64)

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst)}
	//return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty(ts)}
}

// OnCheckCorrectness: Performs some sanity checks for correctness.
func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	sum := 0.0
	sumsc := 0.0
	resid := 0.0
	for vidx := range g.Vertices {
		sum += g.Vertices[vidx].Property.Value
		resid += g.Vertices[vidx].Property.Residual
		sumsc += g.Vertices[vidx].Property.Scratch
	}
	normFactor := float64(len(g.Vertices))
	if NORMALIZE {
		normFactor = 1
	}
	totalAbs := (sum) / normFactor // Only this value is normalized in onfinish
	totalResid := (resid) / float64(len(g.Vertices))
	totalScratch := (sumsc) / float64(len(g.Vertices))
	total := totalAbs + totalResid + totalScratch

	if !mathutils.FloatEquals(total, INITMASS, EPSILON) {
		info("Total absorbed: ", totalAbs)
		info("Total residual: ", totalResid)
		info("Total scratch: ", totalScratch)
		info("Total sum mass: ", total)
		return errors.New("final mass not equal to init")
	}
	return nil
}

func OracleComparison(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], oracle *graph.Graph[VertexProperty, EdgeProperty, MessageValue], resultCache *[]float64) {
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

	println("pos,   rawId,       score, rawId(oracle),   score(oracle)")
	for i := 0; i < mathutils.Min(10, len(oracle.Vertices)); i++ {
		println(fmt.Sprintf("%d,%10d,\t%10.3f,%14d,\t%10.3f", i, g.Vertices[ibRank.Idx[i]].Id, g.Vertices[ibRank.Idx[i]].Property.Value, oracle.Vertices[iaRank.Idx[i]].Id, oracle.Vertices[iaRank.Idx[i]].Property.Value))
	}
}

func PrintTopN(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], size int) {
	ia := make([]float64, len(g.Vertices))
	for v := range g.Vertices {
		ia[v] = g.Vertices[v].Property.Value
	}
	iaRank := mathutils.NewIndexedFloat64Slice(ia)
	sort.Sort(sort.Reverse(iaRank))
	topN := size
	if len(iaRank.Idx) < topN {
		topN = len(iaRank.Idx)
	}
	println("pos,   rawId,       score")
	for i := 0; i < topN; i++ {
		println(fmt.Sprintf("%d,%10d,\t%10.3f", i, g.Vertices[iaRank.Idx[i]].Id, iaRank.Float64Slice[i]))
	}
}

type NamedEntry struct {
	Name  string
	Entry []mathutils.Pair[uint32, float64]
}

var tsDB = make([]NamedEntry, 0)

//var tsLast = uint64(0)

// Logs top N vertices
func LogTimeSeries(name string, data []mathutils.Pair[uint32, VertexProperty]) {
	ia := make([]float64, len(data))
	for v := range data {
		ia[v] = data[v].Second.Value
	}
	iaRank := mathutils.NewIndexedFloat64Slice(ia)
	sort.Sort(sort.Reverse(iaRank))
	topN := 10
	if len(iaRank.Idx) < topN {
		topN = len(iaRank.Idx)
	}
	newDbEntry := make([]mathutils.Pair[uint32, float64], topN)
	for i := 0; i < topN; i++ {
		newDbEntry[i] = mathutils.Pair[uint32, float64]{First: data[iaRank.Idx[i]].First, Second: iaRank.Float64Slice[i]}
		//info(fmt.Sprintf("%d,%10d,\t%10.3f", i, g.Vertices[iaRank.Idx[i]].Id, iaRank.Float64Slice[i]))
	}
	entry := NamedEntry{name, newDbEntry}
	info(entry)
	tsDB = append(tsDB, entry)
}

// Spits out top10 for each point in time (entry).
// Will print all possible vertices for each row (blanks for non entries) to help with formatting.
func PrintTimeSeries() {
	info("Timeseries:")
	allVerticesMap := make(map[uint32]int) // Map of real vertex index to our array index
	allVerticesArr := make([]float64, 0)

	header := ","
	for i := range tsDB {
		for _, e := range tsDB[i].Entry {
			if _, ok := allVerticesMap[e.First]; !ok {
				allVerticesMap[e.First] = len(allVerticesArr)
				allVerticesArr = append(allVerticesArr, 0)
				header += strconv.FormatUint(uint64(e.First), 10) + ","
			}
		}
	}
	println(header)

	for i := range tsDB {
		for j := range allVerticesArr {
			allVerticesArr[j] = 0
		}
		line := tsDB[i].Name + ","
		for _, e := range tsDB[i].Entry {
			allVerticesArr[allVerticesMap[e.First]] = e.Second
		}
		for j := range allVerticesArr {
			if allVerticesArr[j] != 0 {
				line += fmt.Sprintf("%.3f,", allVerticesArr[j])
			} else {
				line += ","
			}
		}
		println(line)
	}
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracleRun bool, oracleFin bool, undirected bool) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
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
	g.EmptyVal = EMPTYVAL
	g.InitVal = INITMASS

	//go frame.LogTimeSeriesRunnable(g, LogTimeSeries)

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
	//PrintTimeSeries()

	//PrintTopN(g, 10)
	if *pptr {
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}
