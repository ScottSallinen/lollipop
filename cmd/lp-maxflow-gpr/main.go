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
	enforce.ENFORCE(sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])
	capacity, _ := strconv.Atoi(stringFields[1])

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty{uint32(capacity)}}
}

func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	var sourceIndex, sinkIndex uint32
	for i, m := range g.Options.InitMessages {
		if m[0].Type == InitSource {
			sourceIndex = i
		} else if m[0].Type == InitSink {
			sinkIndex = i
		} else {
			enforce.ENFORCE(false, "unknown initial message")
		}
	}

	source := &g.Vertices[sourceIndex]
	sink := &g.Vertices[sinkIndex]

	enforce.ENFORCE(source.Property.InitHeight == uint32(len(g.Vertices)), "source InitHeight != # of vertices")
	enforce.ENFORCE(source.Property.Height == uint32(len(g.Vertices)), "source Height != # of vertices")
	enforce.ENFORCE(sink.Property.InitHeight == 0, "sink InitHeight != 0")
	enforce.ENFORCE(sink.Property.Height == 0, "sink Height != 0")

	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		if v.Property.Type == Normal {
			enforce.ENFORCE(v.Property.Excess == 0, fmt.Sprintf("normal vertex index %d ID %d has a non-zero excess of %d", vi, v.Id, v.Property.Excess))
		}
	}

	sinkInFlow := sink.Property.Excess
	sourceOutFlow := uint32(0)
	for i := range source.OutEdges {
		edge := &source.OutEdges[i]
		sourceOutFlow += edge.Property.Capacity
	}
	sourceOutFlow -= source.Property.Excess
	enforce.ENFORCE(sourceOutFlow == sinkInFlow, fmt.Sprintf("sourceOutFlow (%d) != sinkInFlow (%d)", sourceOutFlow, sinkInFlow))

	g.ComputeInEdges()
	// TODO: check in flow == out flow for all vertices
	// TODO: check excess == 0 for normal vertices
	// TODO: check Neighbours
	return nil
}

func GetFrameworkAndGraph(gName string, sourceRaw, sinkRaw uint32) (frame framework.Framework[VertexProperty, EdgeProperty, MessageValue], g graph.Graph[VertexProperty, EdgeProperty, MessageValue]) {
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve
	frame.EdgeParser = EdgeParser

	g.Options = graph.GraphOptions[MessageValue]{
		Undirected:    false,
		EmptyVal:      nil,
		LogTimeseries: false,
		OracleCompare: false,
		SourceInit:    true,
	}

	g.LoadVertexMap(gName, EdgeParser)
	sourceHeight := uint32(len(g.VertexMap))
	source, sourceOk := g.VertexMap[sourceRaw]
	sink, sinkOk := g.VertexMap[sinkRaw]
	enforce.ENFORCE(source != sink)
	enforce.ENFORCE(sourceOk)
	enforce.ENFORCE(sinkOk)

	g.Options.InitMessages = map[uint32]MessageValue{
		source: {{
			Source: source, Type: InitSource, Height: sourceHeight, Value: sourceHeight,
		}},
		sink: {{
			Source: sink, Type: InitSink, Height: 0, Value: 0,
		}},
	}
	return frame, g
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, source, sink uint32) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
	frame, g := GetFrameworkAndGraph(gName, source, sink)
	frame.Launch(&g, gName, async, dynamic)
	return &g
}

func main() {
	gptr := flag.String("g", "data/maxflow/test-1.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
	pptr := flag.Bool("p", false, "Save vertex properties to disk")
	tptr := flag.Int("t", 32, "Thread count")
	source := flag.Uint("source", 0, "Raw ID of the source vertex")
	sink := flag.Uint("source", 1, "Raw ID of the sink vertex")
	flag.Parse()

	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(*gptr, *aptr, *dptr, uint32(*source), uint32(*sink))

	g.ComputeGraphStats(false, false)

	if *pptr {
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}
