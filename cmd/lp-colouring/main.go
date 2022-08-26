package main

import (
	"flag"
	"fmt"
	"github.com/ScottSallinen/lollipop/enforce"
	"log"
	"math"
	"net/http"

	_ "net/http/pprof"
	"strconv"
	"strings"

	"github.com/ScottSallinen/lollipop/cmd/common"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

func info(args ...any) {
	log.Println("[Colouring]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 2 || sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty{}}
}

func ComputeGraphColouringStat(g *graph.Graph[VertexProperty, EdgeProperty]) (maxColour uint32, nColours int) {
	maxColour = uint32(0)
	allColours := make(map[uint32]bool)
	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		allColours[v.Property.Colour] = true
		if v.Property.Colour > maxColour {
			maxColour = v.Property.Colour
		}
	}
	nColours = len(allColours)
	return maxColour, nColours
}

func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty]) error {
	// Check correctness
	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		if v.Property.Colour == EmptyColour {
			return fmt.Errorf("vertex %d is not coloured", v.Id)
		}
		for ei := range v.OutEdges {
			target := &g.Vertices[v.OutEdges[ei].Destination]
			if v.Property.Colour == target.Property.Colour {
				//g.PrintStructure()
				//g.PrintVertexProperty("OnCheckCorrectness ")
				return fmt.Errorf("an edge exists between Vertex %d and Vertex %d which have the same colour %d", v.Id, target.Id, v.Property.Colour)
			}
		}
	}

	maxColour, nColours := ComputeGraphColouringStat(g)
	info("Max colour: ", maxColour, " Number of colours: ", nColours, " Ratio: ", float64(maxColour+1)/float64(nColours))
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool) *graph.Graph[VertexProperty, EdgeProperty] {
	frame := framework.Framework[VertexProperty, EdgeProperty]{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve
	frame.EdgeParser = EdgeParser

	g := &graph.Graph[VertexProperty, EdgeProperty]{}
	g.SourceInit = false
	g.EmptyVal = math.MaxFloat64

	frame.Launch(g, gName, async, dynamic, false, true)

	return g
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
	pptr := flag.Bool("p", false, "Save vertex properties to disk")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()

	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(*gptr, *aptr, *dptr)

	g.ComputeGraphStats(false, false)

	if *pptr {
		graphName := common.ExtractGraphName(*gptr)
		common.WriteVertexProps(g, graphName, *dptr)
	}
}
