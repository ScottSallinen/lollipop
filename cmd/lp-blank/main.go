package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

// This function is mostly optional, but is a good way to describe
// an algorithm that can check for correctness of a result (outside just the go tests)
// For example, for breadth first search, you wouldn't expect a neighbour to be more than
// one hop away; for graph colouring you wouldn't expect two neighbours to have the same colour,
// etc. This can codify the desire to ensure correct behaviour.
func OnCheckCorrectness(g *graph.Graph[VertexProperty]) error {
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, oracle bool, undirected bool) *graph.Graph[VertexProperty] {
	frame := framework.Framework[VertexProperty]{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve

	g := &graph.Graph[VertexProperty]{}

	// Some potential extra defines here, for if the algorithm has a "point" initialization
	// or is instead initialized by default behaviour (where every vertex is visited initially)
	// g.SourceInit = true
	// g.SourceInitVal = 1.0
	// g.EmptyVal = math.MaxFloat64
	// g.SourceVertex = rawSrc

	frame.Launch(g, gName, async, dynamic, oracle, undirected)
	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	uptr := flag.Bool("u", false, "Interpret the input graph as undirected (add transpose edges)")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
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
		gNameMain = gNameMainTD[len(gNameMainTD)-2]
	} else {
		gNameMain = gNameMainTD[0]
	}
	gNameMain = "results/" + gNameMain

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(gName, *aptr, *dptr, false, *uptr)
	g.ComputeGraphStats(false, false)

	if *pptr {
		resName := "static"
		if *dptr {
			resName = "dynamic"
		}
		WriteVertexProps(g, gNameMain+"-props-"+resName+".txt")
	}
}

func WriteVertexProps(g *graph.Graph[VertexProperty], fname string) {
	f, err := os.Create(fname)
	enforce.ENFORCE(err)
	defer f.Close()
	for vidx := range g.Vertices {
		_, err := f.WriteString(fmt.Sprintf("%d %.4f\n", g.Vertices[vidx].Id, g.Vertices[vidx].Property.Value))
		enforce.ENFORCE(err)
	}
}
