package main

import (
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
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

	//exit := false
	//defer func() { exit = true }()
	//go CompareToOracleRunnable(g, &exit)

	frame.Run(g, &feederWg, &frameWait)
	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second")
	cptr := flag.Bool("c", false, "Compare static vs dynamic")
	optr := flag.Bool("o", false, "Save execution props as oracle values")
	pptr := flag.Bool("p", false, "Comepare results to oracle values")
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

	if *pptr {
		vmap := gNameMain + ".vresmap"
		file, err := os.Open(vmap)
		enforce.ENFORCE(err)
		defer file.Close()
		err = gob.NewDecoder(file).Decode(&graph.ORACLEMAP)
		enforce.ENFORCE(err)
	}

	runAsync := doAsync
	if doDynamic {
		runAsync = true
	}
	g := LaunchGraphExecution(gName, runAsync, doDynamic)

	CompareToOracle(g)
	g.ComputeGraphStats(false, false)
	resName := "static"
	if doDynamic {
		resName = "dynamic"
	}
	g.WriteVertexProps(gNameMain + "-props-" + resName + ".txt")

	if *optr {
		g.WriteVertexProps(gNameMain + "-props-oracle.txt")

		vmap := gNameMain + ".vresmap"
		graph.ORACLEMAP = make(map[uint32]float64)
		for vidx := range g.Vertices {
			graph.ORACLEMAP[g.Vertices[vidx].Id] = g.Vertices[vidx].Properties.Value
		}
		newFile, err := os.Create(vmap)
		enforce.ENFORCE(err)
		defer newFile.Close()
		err = gob.NewEncoder(newFile).Encode(graph.ORACLEMAP)
		enforce.ENFORCE(err)
	}

	if *cptr {
		runAsync = doAsync
		if !doDynamic {
			runAsync = true
		}
		gAlt := LaunchGraphExecution(gName, runAsync, !doDynamic)

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

func CompareToOracleRunnable(g *graph.Graph, exit *bool) {
	time.Sleep(1 * time.Second)
	for !*exit {
		CompareToOracle(g)
		time.Sleep(1 * time.Second)
	}
}

func CompareToOracle(g *graph.Graph) {
	/*
		if graph.ORACLEMAP != nil {
			a := make([]float64, len(graph.ORACLEMAP))
			b := make([]float64, len(graph.ORACLEMAP))

			vidx := 0
			g.Mutex.Lock()

			for id, value := range graph.ORACLEMAP {
				g2idx, ok := g.VertexMap[id]

				g2value := 1.0 * 0.85
				if ok {
					g2value = g.Vertices[g2idx].Properties.Value
					//numEdges += uint64(len(g.Vertices[g2idx].OutEdges))
				}
				a[vidx] = value
				b[vidx] = g2value
				vidx++
			}
			graph.ResultCompare(a, b)
			g.Mutex.Unlock()
		}
	*/

	numEdges := uint64(0)

	g.Mutex.Lock()
	g.Watch.Pause()
	info("----INLINE----")
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

	frame.Init(altG, true, false)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	frame.Run(altG, &feederWg, &frameWait)

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
	}
	info("edgeCount: ", numEdges)
	graph.ResultCompare(ia, ib)

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
