package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...interface{}) {
	log.Println("[Pagerank]\t", fmt.Sprint(args...))
}

// OnCheckCorrectness: Performs some sanity checks for correctness.
func OnCheckCorrectness(g *graph.Graph) error {
	sum := 0.0
	sumsc := 0.0
	resid := 0.0
	for vidx := range g.Vertices {
		sum += g.Vertices[vidx].Value
		resid += g.Vertices[vidx].Residual
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

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracleRun bool, oracleFin bool) *graph.Graph {
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
	g.EmptyVal = 0.0

	frame.Launch(g, gName, async, dynamic, oracleRun)

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
	optr := flag.Bool("o", false, "Compare to oracle results during runtime")
	fptr := flag.Bool("f", false, "Compare to oracle results (computed via async) upon finishing the initial algorithm.")
	pptr := flag.Bool("p", false, "Save vertex properties to disk")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()
	gName := *gptr
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

	//debug.SetGCPercent(-1)
	//runtime.SetMutexProfileFraction(1)

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(gName, *aptr, *dptr, *optr, *fptr)

	g.ComputeGraphStats(false, false)

	if *pptr {
		resName := "static"
		if *dptr {
			resName = "dynamic"
		}
		g.WriteVertexProps(gNameMain + "-props-" + resName + ".txt")
	}
}
