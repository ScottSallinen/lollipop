package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...any) {
	log.Println("[Pagerank]\t", fmt.Sprint(args...))
}

var logTimestampPos = 2
var oraclF *os.File

//var ecount = uint64(0) // for a sliding window graph strategy on non-temporal data

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])
	ts, _ := strconv.ParseUint(stringFields[logTimestampPos], 10, 64)

	//ts := ecount // for sliding window graph strategy on non-temporal data
	//ecount++

	//return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst)}
	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty(ts)}
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
	if NORMALIZE || PPR {
		normFactor = 1
	}
	divFactor := float64(len(g.Vertices))
	if PPR {
		divFactor = 1
	}
	totalAbs := (sum) / normFactor // Only this value is normalized in onfinish
	totalResid := (resid) / divFactor
	totalScratch := (sumsc) / divFactor
	total := totalAbs + totalResid + totalScratch

	compVal := EPSILON
	if PPR {
		compVal = INITMASS * 0.1
	}
	if !mathutils.FloatEquals(total, INITMASS, compVal) {
		info("Total absorbed: ", totalAbs)
		info("Total residual: ", totalResid)
		info("Total scratch: ", totalScratch)
		info("Total sum mass: ", total)
		return errors.New("final mass not equal to init")
	}
	return nil
}

func OracleComparison(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], oracle *graph.Graph[VertexProperty, EdgeProperty, MessageValue], resultCache *[]float64, cache bool) {
	ia := make([]float64, len(g.Vertices))
	ib := make([]float64, len(g.Vertices))
	numEdges := uint64(0)

	numIgnore := 0

	for v := range g.Vertices {
		ia[v] = oracle.Vertices[v].Property.Value
		ib[v] = g.Vertices[v].Property.Value
		numEdges += uint64(len(g.Vertices[v].OutEdges))
		// Ignore vertices with no edges
		if uint64(len(g.Vertices[v].OutEdges)) == 0 {
			numIgnore++
		}
	}

	if *resultCache != nil && cache {
		info("Using cached oracle result.")
		copy(ia, *resultCache)
	} else if *resultCache == nil && cache {
		*resultCache = make([]float64, len(ia))
		copy(*resultCache, ia)
		info("Cached the oracle result.")
	}

	info("vertexCount ", uint64(len(g.Vertices)), " edgeCount ", numEdges)
	l1avg, p95l1, _ := graph.ResultCompare(ia, ib, numIgnore)

	iaRank := mathutils.NewIndexedFloat64Slice(ia)
	ibRank := mathutils.NewIndexedFloat64Slice(ib)
	sort.Sort(sort.Reverse(iaRank))
	sort.Sort(sort.Reverse(ibRank))

	topN := 1000
	topK := 100
	topM := 10
	if len(iaRank.Idx) < topN {
		topN = len(iaRank.Idx)
	}
	if len(iaRank.Idx) < topK {
		topK = len(iaRank.Idx)
	}
	if len(iaRank.Idx) < topM {
		topM = len(iaRank.Idx)
	}
	iaRk := make([]int, topK)
	copy(iaRk, iaRank.Idx[:topK])

	ibRk := make([]int, topK)
	copy(ibRk, ibRank.Idx[:topK])
	icRk := make([]int, topM)
	copy(icRk, ibRank.Idx[:topM])

	mRBO6 := mathutils.CalculateRBO(iaRank.Idx[:topN], ibRank.Idx[:topN], 0.6)
	mRBO9 := mathutils.CalculateRBO(iaRk, ibRk, 0.9)
	mRBO5 := mathutils.CalculateRBO(iaRk, icRk, 0.5)
	info("top", topN, " RBO6 ", fmt.Sprintf("%.4f", mRBO6*100.0), " top", topK, " RBO9 ", fmt.Sprintf("%.4f", mRBO9*100.0), " top", topM, " RBO5 ", fmt.Sprintf("%.4f", mRBO5*100.0))

	wStr := fmt.Sprintf("%.3e,%.3e,%.4f,%.4f", l1avg, p95l1, mRBO6*100.0, mRBO5*100.0)
	oraclF.WriteString(wStr + "\n")

	//println("pos,   rawId,        score, rawId(oracle),   score(oracle)")
	//for i := 0; i < mathutils.Min(10, len(oracle.Vertices)); i++ {
	//	println(fmt.Sprintf("%d,%10d,\t%10.3f,%14d,\t%10.3f", i, g.Vertices[ibRank.Idx[i]].Id, g.Vertices[ibRank.Idx[i]].Property.Value, oracle.Vertices[iaRank.Idx[i]].Id, iaRank.Float64Slice[i]))
	//}
}

func PrintTopN(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], size int) {
	info("PrintTopN:")
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
	Name   time.Time
	vCount uint64
	eCount uint64
	Entry  []mathutils.Pair[uint32, float64]
}

var tsDB = make([]NamedEntry, 0)

func ApplyTimeSeries(entries chan framework.TimeseriesEntry[VertexProperty], wg *sync.WaitGroup) {
	//for e := range entries {
	//	ia := make([]float64, len(e.VertexData))
	//	for v := range e.VertexData {
	//		ia[v] = e.VertexData[v].Second.Value
	//	}
	//	iaRank := mathutils.NewIndexedFloat64Slice(ia)
	//	sort.Sort(sort.Reverse(iaRank))
	//	topN := 10
	//	if len(iaRank.Idx) < topN {
	//		topN = len(iaRank.Idx)
	//	}
	//	newDbEntry := make([]mathutils.Pair[uint32, float64], topN)
	//	for i := 0; i < topN; i++ {
	//		newDbEntry[i] = mathutils.Pair[uint32, float64]{First: e.VertexData[iaRank.Idx[i]].First, Second: iaRank.Float64Slice[i]}
	//		//info(fmt.Sprintf("%d,%10d,\t%10.3f", i, g.Vertices[iaRank.Idx[i]].Id, iaRank.Float64Slice[i]))
	//	}
	//	entry := NamedEntry{e.Name, uint64(len(e.VertexData)), e.EdgeCount, newDbEntry}
	//	//info(entry)
	//	tsDB = append(tsDB, entry)
	//	PrintTimeSeries(true, false)
	//}
}

// Spits out top10 for each point in time (entry).
// Will print all possible vertices for each row (blanks for non entries) to help with formatting.
func PrintTimeSeries(fileOut bool, stdOut bool) {
	if stdOut {
		info("Timeseries:")
	}
	allVerticesMap := make(map[uint32]int) // Map of real vertex index to our array index
	allVerticesArr := make([]float64, 0)

	var f *os.File
	var err error
	if fileOut {
		f, err = os.Create("results/timeseries.txt")
		enforce.ENFORCE(err)
		defer f.Close()
	}

	header := "ts,,v,e,,"
	for i := range tsDB {
		for _, e := range tsDB[i].Entry {
			if _, ok := allVerticesMap[e.First]; !ok {
				allVerticesMap[e.First] = len(allVerticesArr)
				allVerticesArr = append(allVerticesArr, 0)
				header += strconv.FormatUint(uint64(e.First), 10) + ","
			}
		}
	}
	if stdOut {
		println(header)
	}
	if fileOut {
		f.WriteString(header + "\n")
	}

	for i := range tsDB {
		for j := range allVerticesArr {
			allVerticesArr[j] = 0
		}
		// Tweaked for better excel importing...
		line := tsDB[i].Name.Format(time.RFC3339) + "," + tsDB[i].Name.Format("2006-01-02") + "," + strconv.FormatUint(tsDB[i].vCount, 10) + "," + strconv.FormatUint(tsDB[i].eCount, 10) + "," + tsDB[i].Name.Format("2006-01-02") + ","
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
		if stdOut {
			println(line)
		}
		if fileOut {
			f.WriteString(line + "\n")
		}
	}
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracleRun bool, oracleFin bool, timeSeries bool, undirected bool) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
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
	frame.GetTimestamp = GetTimestamp
	frame.SetTimestamp = SetTimestamp
	frame.ApplyTimeSeries = ApplyTimeSeries

	// wikipedia-growth
	// interestArray := []uint32{73, 259, 9479, 6276, 1710, 864, 2169, 110, 10312, 69, 425, 611, 1566, 11297, 1916, 1002, 975, 6413, 526, 5079, 1915, 11956, 2034, 956, 208, 15, 77041, 652, 20, 1352, 1918, 388, 1806, 1920, 3517, 863, 1594, 24772, 2008, 78349, 397, 1923, 1105, 8707, 7, 4336, 1753, 205, 17, 984, 5732, 983, 70, 1924, 111, 51076, 6903, 4083, 1936, 1115, 154942, 1550, 2266, 179, 1933, 37976, 2844, 57028, 1934, 1932, 84204, 1931, 490, 1935, 2312, 1925, 1846, 5081, 1930, 4378, 1917, 68, 3080, 2734, 435, 1482, 1929, 1922, 4104, 2814, 1926, 1919, 1164, 1110, 1928, 2843, 4364, 1921, 4148, 2041}

	//initMap := make(map[uint32]MessageValue)
	//for _, v := range interestArray {
	//	initMap[v] = MessageValue(INITMASS / len(interestArray))
	//}

	g := &graph.Graph[VertexProperty, EdgeProperty, MessageValue]{}
	g.Options = graph.GraphOptions[MessageValue]{
		Undirected:            undirected,
		EmptyVal:              EMPTYVAL,
		InitAllMessage:        INITMASS,
		LogTimeseries:         timeSeries,
		TimeSeriesInterval:    (24 * 60 * 60) * 1,
		AsyncContinuationTime: 0, // if logging timeseries
		//InsertDeleteOnExpire:  (24 * 60 * 60) * 120,
		OracleCompare:     oracleRun,
		OracleInterval:    1000,
		OracleCompareSync: false,
	}

	if PPR {
		g.Options.SourceInit = true
		//g.Options.InitMessages = initMap
	}

	if oracleRun || g.Options.OracleCompareSync {
		var err error
		oraclF, err = os.Create("results/oracleComp.txt")
		enforce.ENFORCE(err)
		oraclF.WriteString("AvgL1,P95L1,1000.RBO6,10.RBO5\n")
		defer oraclF.Close()
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
	optr := flag.Bool("o", false, "Compare to oracle results during runtime. If timeseries enabled, will run on each logging of data instead of intervaled.")
	sptr := flag.Bool("s", false, "Log timeseries data")
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

	g := LaunchGraphExecution(*gptr, *aptr, *dptr, *optr, *fptr, *sptr, *uptr)

	g.ComputeGraphStats(false, true)

	if *sptr {
		PrintTimeSeries(true, false)
	}
	PrintTopN(g, 10)

	if *pptr {
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}
