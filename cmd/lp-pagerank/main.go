package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"

	_ "net/http/pprof"
)

const DEBUG = false

func info(args ...interface{}) {
	log.Println("[Pagerank]\t", fmt.Sprint(args...))
}

func dinfo(args ...interface{}) {
	if DEBUG {
		info(args...)
	}
}

func OnCheckCorrectness(g *graph.Graph) error {
	sum := 0.0
	sumsc := 0.0
	resid := 0.0
	for vidx := range g.Vertices {
		sum += g.Vertices[vidx].Properties.Value
		resid += g.Vertices[vidx].Properties.Residual
		sumsc += g.Vertices[vidx].Scratch
	}
	totalAbs := (sum) / float64(len(g.Vertices))
	totalResid := (resid) / float64(len(g.Vertices))
	total := totalAbs + totalResid
	//info("Total absorbed: ", totalAbs)
	//info("Total residual: ", totalResid)
	//info("Total scratch: ", sumsc/float64(len(g.Vertices)))
	//info("Total sum mass: ", total)

	if !mathutils.FloatEquals(total, INITMASS) {
		info("Total absorbed: ", totalAbs)
		info("Total residual: ", totalResid)
		info("Total scratch: ", sumsc/float64(len(g.Vertices)))
		info("Total sum mass: ", total)
		return errors.New("Final mass not equal to init.")
	}
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracle bool) *graph.Graph {
	frame := framework.Framework{}
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel

	frame.OnCompareOracle = CompareToOracle

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

	if oracle {
		exit := false
		defer func() { exit = true }()
		go CompareToOracleRunnable(g, &exit, 1000*time.Millisecond)
	}

	frame.Run(g, &feederWg, &frameWait)
	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second")
	cptr := flag.Bool("c", false, "Compare static vs dynamic")
	optr := flag.Bool("o", false, "Compare to oracle results during runtime")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()
	gName := *gptr
	doAsync := *aptr
	doDynamic := *dptr
	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	gNameMainT := strings.Split(gName, "/")
	gNameMain := gNameMainT[len(gNameMainT)-1]
	gNameMainTD := strings.Split(gNameMain, ".")
	if len(gNameMainTD) > 1 {
		gNameMain = gNameMainTD[len(gNameMainT)-2]
	} else {
		gNameMain = gNameMainTD[0]
	}
	gNameMain = "results/" + gNameMain

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	runAsync := doAsync
	if doDynamic {
		runAsync = true
	}
	g := LaunchGraphExecution(gName, runAsync, doDynamic, *optr)

	CompareToOracle(g)
	g.ComputeGraphStats(false, false)
	resName := "static"
	if doDynamic {
		resName = "dynamic"
	}
	g.WriteVertexProps(gNameMain + "-props-" + resName + ".txt")

	if *cptr {
		runAsync = doAsync
		if !doDynamic {
			runAsync = true
		}
		gAlt := LaunchGraphExecution(gName, runAsync, !doDynamic, *optr)

		gAlt.ComputeGraphStats(false, false)
		resName := "dynamic"
		if doDynamic {
			resName = "static"
		}
		gAlt.WriteVertexProps(gNameMain + "-props-" + resName + ".txt")

		a := make([]float64, len(g.Vertices))
		b := make([]float64, len(g.Vertices))

		for vidx := range g.Vertices {
			g1raw := g.Vertices[vidx].Id
			g2idx := gAlt.VertexMap[g1raw]

			g1values := &g.Vertices[vidx].Properties
			g2values := &gAlt.Vertices[g2idx].Properties

			a[vidx] = g1values.Value
			b[vidx] = g2values.Value
		}

		graph.ResultCompare(a, b)
		info("Comparison verified.")
	}
}

func CompareToOracleRunnable(g *graph.Graph, exit *bool, sleepTime time.Duration) {
	time.Sleep(sleepTime)
	for !*exit {
		CompareToOracle(g)
		time.Sleep(sleepTime)
	}
}

var resultCache []float64

func CompareToOracle(g *graph.Graph) {
	numEdges := uint64(0)

	g.Mutex.Lock()
	g.Watch.Pause()
	info("----INLINE----")
	info("inlineTime(ms) ", g.Watch.Elapsed().Milliseconds())
	frame := framework.Framework{}
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel

	altG := &graph.Graph{}
	altG.OnInitVertex = OnInitVertex

	altG.VertexMap = g.VertexMap // ok to shallow copy, we do not edit.
	altG.Vertices = make([]graph.Vertex, len(g.Vertices))
	gVertexStash := make([]graph.Vertex, len(g.Vertices))
	for v := range g.Vertices {
		altG.Vertices[v].Id = g.Vertices[v].Id
		altG.Vertices[v].OutEdges = g.Vertices[v].OutEdges
		numEdges += uint64(len(g.Vertices[v].OutEdges))
		gVertexStash[v].Properties.Value = g.Vertices[v].Properties.Value
		gVertexStash[v].Id = g.Vertices[v].Id
		gVertexStash[v].Properties.Residual = g.Vertices[v].Properties.Residual
		gVertexStash[v].Active = g.Vertices[v].Active
		gVertexStash[v].Scratch = g.Vertices[v].Scratch
	}

	if resultCache == nil {
		frame.Init(altG, true, false)
		var feederWg sync.WaitGroup
		feederWg.Add(1)
		var frameWait sync.WaitGroup
		frameWait.Add(1)
		frame.Run(altG, &feederWg, &frameWait)
	}

	ia := make([]float64, len(g.Vertices))
	ib := make([]float64, len(g.Vertices))

	// Here we "early finish" proper G immediately for a fair comparison (i.e., including sink adjustment)
	// to compare a fully finished to the current state. Since the OnFinish is minute in cost but big in effect,
	// important to compare with it applied to both .
	frame.OnFinish(g, nil)

	for v := range g.Vertices {
		ia[v] = altG.Vertices[v].Properties.Value
		ib[v] = g.Vertices[v].Properties.Value
		// Resetting the effect of the "early finish"
		g.Vertices[v].Properties.Value = gVertexStash[v].Properties.Value
		g.Vertices[v].Properties.Residual = gVertexStash[v].Properties.Residual
		g.Vertices[v].Scratch = gVertexStash[v].Scratch
	}
	if resultCache == nil && numEdges == 28511807 {
		resultCache = make([]float64, len(ia))
		copy(resultCache, ia)
	}
	if resultCache != nil {
		copy(ia, resultCache)
	}
	info("vertexCount ", uint64(len(g.Vertices)), " edgeCount ", numEdges, " vertexPct ", (len(g.Vertices)*100)/1791489, " edgePct ", (numEdges*100)/28511807)
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
	iaRk := make([]int, topK)
	copy(iaRk, iaRank.Idx[:topK])
	ibRk := make([]int, topK)
	copy(ibRk, ibRank.Idx[:topK])

	mRBO6 := mathutils.CalculateRBO(iaRank.Idx[:topN], ibRank.Idx[:topN], 0.6)
	mRBO9 := mathutils.CalculateRBO(iaRk, ibRk, 0.9)
	info("top", topN, " RBO6 ", fmt.Sprintf("%.4f", mRBO6*100.0), " top", topK, " RBO9 ", fmt.Sprintf("%.4f", mRBO9*100.0))

	/*
		// Next test, how long to finish G from its current state?
		mirrorG := &graph.Graph{}
		mirrorG.OnInitVertex = OnInitVertex

		mirrorG.VertexMap = g.VertexMap // ok to shallow copy, we do not edit.
		mirrorG.Vertices = make([]graph.Vertex, len(g.Vertices))
		for v := range g.Vertices {
			mirrorG.Vertices[v].OutEdges = g.Vertices[v].OutEdges // shallow
			mirrorG.Vertices[v].Id = g.Vertices[v].Id
			mirrorG.Vertices[v].Properties.Residual = g.Vertices[v].Properties.Residual
			mirrorG.Vertices[v].Properties.Value = g.Vertices[v].Properties.Value
			mirrorG.Vertices[v].Active = g.Vertices[v].Active
			mirrorG.Vertices[v].Scratch = g.Vertices[v].Scratch
		}

		frame.Init(mirrorG, true, false)
		mirrorG.MessageQ = make([]chan graph.Message, graph.THREADS)
		for i := 0; i < graph.THREADS; i++ {
			mirrorG.MsgSend[i] = g.MsgSend[i]
			mirrorG.MsgRecv[i] = g.MsgRecv[i]
			mirrorG.MessageQ[i] = g.MessageQ[i]
		}

		var mirrorGfeederWg sync.WaitGroup
		mirrorGfeederWg.Add(1)
		var mirrorGframeWait sync.WaitGroup
		mirrorGframeWait.Add(1)

		frame.Run(mirrorG, &mirrorGfeederWg, &mirrorGframeWait)
	*/

	g.Mutex.Unlock()
	info("----END_INLINE----")
	g.Watch.UnPause()
}
