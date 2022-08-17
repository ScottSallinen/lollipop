package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

func info(args ...interface{}) {
	log.Println("[SSSP]\t", fmt.Sprint(args...))
}

// OnCheckCorrectness: Performs some sanity checks for correctness.
func OnCheckCorrectness(g *graph.Graph) error {
	maxValue := 0.0
	// Denote vertices that claim unvisted, and ensure out edges are at least as good as we could provide
	for vidx := range g.Vertices {
		ourValue := g.Vertices[vidx].Value
		if ourValue < g.EmptyVal {
			maxValue = math.Max(maxValue, ourValue)
		}

		if g.Vertices[vidx].Id == g.SourceVertex {
			enforce.ENFORCE(ourValue == g.SourceInitVal, ourValue)
		}
		if ourValue == g.EmptyVal { // we were never visted

		} else {
			for eidx := range g.Vertices[vidx].OutEdges {
				target := g.Vertices[vidx].OutEdges[eidx].Target
				enforce.ENFORCE(g.Vertices[target].Value <= (ourValue + g.Vertices[vidx].OutEdges[eidx].GetWeight()))
			}
		}
	}
	info("maxValue ", maxValue)
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracleRun bool, oracleFin bool, rawSrc uint32) *graph.Graph {
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

	frame.Launch(g, gName, async, dynamic, oracleRun, false)

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
	iptr := flag.Int("i", 1, "Init vertex (raw id)")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()
	gName := *gptr
	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	gNameMainT := strings.Split(gName, "/")
	gNameMain := gNameMainT[len(gNameMainT)-1]
	gNameMainTD := strings.Split(gNameMain, ".")
	if len(gNameMainTD) > 1 {
		gNameMain = gNameMainTD[len(gNameMainTD)-2]
	} else {
		gNameMain = gNameMainTD[0]
	}
	gNameMain = "results/" + gNameMain

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(gName, *aptr, *dptr, *optr, *fptr, uint32(*iptr))

	g.ComputeGraphStats(false, false)

	if *pptr {
		resName := "static"
		if *dptr {
			resName = "dynamic"
		}
		g.WriteVertexProps(gNameMain + "-props-" + resName + ".txt")
	}
}
