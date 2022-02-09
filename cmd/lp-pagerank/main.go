package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"

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
	info("Total absorbed: ", totalAbs)
	info("Total residual: ", totalResid)
	info("Total scratch: ", sumsc/float64(len(g.Vertices)))
	info("Total sum mass: ", total)

	if !mathutils.FloatEquals(total, INITMASS) {
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

	frame.Run(g, &feederWg, &frameWait)
	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	cptr := flag.Bool("c", false, "Compare static vs dynamic")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()
	gName := *gptr
	doAsync := *aptr
	doDynamic := *dptr
	graph.THREADS = *tptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	runAsync := doAsync
	if doDynamic {
		runAsync = true
	}
	g := LaunchGraphExecution(gName, runAsync, doDynamic)

	g.ComputeGraphStats(false, false)
	resName := "static"
	if doDynamic {
		resName = "dynamic"
	}
	g.WriteVertexProps("results/props-" + resName + ".txt")

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
		gAlt.WriteVertexProps("results/props-" + resName + ".txt")

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
