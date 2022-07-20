package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"

	_ "net/http/pprof"
)

func info(args ...interface{}) {
	log.Println("[SSSP]\t", fmt.Sprint(args...))
}

// OnCheckCorrectness: Performs some sanity checks for correctness.
func OnCheckCorrectness(g *graph.Graph) error {
	noInVtx := make(map[uint32]uint32)
	// First check: denote vertices that claim unvisted, and ensure out edges are at least as good as we could provide
	for vidx := range g.Vertices {
		ourValue := g.Vertices[vidx].Value
		if g.Vertices[vidx].Id == g.SourceVertex {
			enforce.ENFORCE(ourValue == g.SourceInitVal, ourValue)
		}
		if ourValue == g.EmptyVal { // we were never visted
			noInVtx[uint32(vidx)] = 1
		} else {
			for eidx := range g.Vertices[vidx].OutEdges {
				target := g.Vertices[vidx].OutEdges[eidx].Target
				enforce.ENFORCE(g.Vertices[target].Value <= (ourValue + g.Vertices[vidx].OutEdges[eidx].Weight))
			}
		}
	}
	maxValue := 0.0
	// Ensure unvisited vertices have no in edges or all pointing edges are also unvisted vertices.
	for vidx := range g.Vertices {
		ourValue := g.Vertices[vidx].Value
		if ourValue < g.EmptyVal {
			maxValue = math.Max(maxValue, ourValue)
		}
		if ourValue == g.EmptyVal { // we were never visted
			for eidx := range g.Vertices[vidx].OutEdges {
				target := g.Vertices[vidx].OutEdges[eidx].Target
				_, inMap := noInVtx[target]
				enforce.ENFORCE(!inMap)
			}
		}
	}
	info("maxValue ", maxValue)
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracle bool, rawSrc uint32) *graph.Graph {
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
	g.SourceInit = true
	g.SourceInitVal = 1.0
	g.EmptyVal = math.MaxFloat64
	g.SourceVertex = rawSrc

	frame.Launch(g, gName, async, dynamic, oracle)

	frame.CompareToOracle(g)

	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
	optr := flag.Bool("o", false, "Compare to oracle results during runtime")
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

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(gName, *aptr, *dptr, *optr, 0)

	g.ComputeGraphStats(false, false)

	if *pptr {
		resName := "static"
		if *dptr {
			resName = "dynamic"
		}
		g.WriteVertexProps(gNameMain + "-props-" + resName + ".txt")
	}
}
