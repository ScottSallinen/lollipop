package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/ScottSallinen/lollipop/enforce"

	_ "net/http/pprof"
	"strconv"
	"strings"

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

func ComputeGraphColouringStat(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) (maxColour uint32, nColours int) {
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

func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		colour := v.Property.Colour
		degree := uint32(len(v.OutEdges))
		if colour == EMPTYVAL {
			return fmt.Errorf("vertex %d (index=%d) is not coloured", v.Id, vi)
		}
		if colour > degree {
			return fmt.Errorf("vertex %d (index=%d) has a colour (%d) that is larger than its own degree (%d)", v.Id, vi, colour, degree)
		}
		for ei := range v.OutEdges {
			targetIndex := v.OutEdges[ei].Destination
			target := &g.Vertices[targetIndex]
			if colour == target.Property.Colour && v.Id != target.Id { // Ignore if its a self edge
				//g.PrintStructure()
				//g.PrintVertexProperty("OnCheckCorrectness ")
				return fmt.Errorf("an edge exists between Vertex %d (index=%d) and Vertex %d (index=%d) which have the same colour %d", v.Id, vi, target.Id, targetIndex, v.Property.Colour)
			}
		}
	}

	maxColour, nColours := ComputeGraphColouringStat(g)
	info("Max colour: ", maxColour, " Number of colours: ", nColours, " Ratio: ", float64(maxColour+1)/float64(nColours))
	return nil
}

func LaunchGraphExecution(gName string, async bool, dynamic bool) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
	frame := framework.Framework[VertexProperty, EdgeProperty, MessageValue]{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.OnEdgeAddRev = OnEdgeAddRev
	frame.OnEdgeDelRev = OnEdgeDelRev
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve
	frame.EdgeParser = EdgeParser

	g := &graph.Graph[VertexProperty, EdgeProperty, MessageValue]{}
	g.SourceInit = false
	g.InitVal = 0
	g.EmptyVal = EMPTYVAL

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
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}
