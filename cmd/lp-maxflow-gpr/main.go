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
	log.Println("[MaxFlowGPR]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])
	capacity, _ := strconv.Atoi(stringFields[2])

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty{uint32(capacity)}}
}

func OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	var sourceIndex, sinkIndex uint32
	for mi, m := range g.Options.InitMessages {
		if m[0].Type == InitSource {
			sourceIndex = mi
		} else if m[0].Type == InitSink {
			sinkIndex = mi
		} else {
			enforce.ENFORCE(false, "unknown initial message")
		}
	}

	source := &g.Vertices[sourceIndex]
	sink := &g.Vertices[sinkIndex]
	enforce.ENFORCE(source.Property.Type == Source)
	enforce.ENFORCE(sink.Property.Type == Sink)

	// Check source height and sink height
	enforce.ENFORCE(source.Property.InitHeight == uint32(len(g.Vertices)), "source InitHeight != # of vertices")
	enforce.ENFORCE(source.Property.Height == uint32(len(g.Vertices)), "source Height != # of vertices")
	enforce.ENFORCE(sink.Property.InitHeight == 0, "sink InitHeight != 0")
	enforce.ENFORCE(sink.Property.Height == 0, "sink Height != 0")

	// Make sure all messages are processed
	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		enforce.ENFORCE(len(v.Property.MessageBuffer) == 0, fmt.Sprintf("vertex index %d ID %d has outstanding messages", vi, v.Id))
	}

	// Check Excess
	for vi := range g.Vertices {
		if v := &g.Vertices[vi]; v.Property.Type == Normal {
			enforce.ENFORCE(v.Property.Excess == 0, fmt.Sprintf("normal vertex index %d ID %d has a non-zero excess of %d", vi, v.Id, v.Property.Excess))
		}
	}

	// Check sum of edge capacities in the original graph == in the residual graph
	for vi := range g.Vertices {
		if v := &g.Vertices[vi]; v.Property.Type == Normal {
			sumEdgeCapacityOriginal := uint32(0)
			sumEdgeCapacityResidual := uint32(0)
			for ei := range v.OutEdges {
				sumEdgeCapacityOriginal += v.OutEdges[ei].Property.Capacity
			}
			for _, neighbour := range v.Property.Neighbours {
				sumEdgeCapacityResidual += neighbour.ResidualCapacity
			}
			enforce.ENFORCE(sumEdgeCapacityOriginal == sumEdgeCapacityResidual, fmt.Sprintf(
				"normal vertex index %d ID %d sumEdgeCapacityOriginal (%d) != sumEdgeCapacityResidual (%d)",
				vi, v.Id, sumEdgeCapacityOriginal, sumEdgeCapacityResidual),
			)
		}
	}

	// Check sourceOut and sinkIn
	sinkIn := sink.Property.Excess
	sourceOut := uint32(0)
	for ei := range source.OutEdges {
		edge := &source.OutEdges[ei]
		sourceOut += edge.Property.Capacity
	}
	sourceOut -= source.Property.Excess
	enforce.ENFORCE(sourceOut == sinkIn, fmt.Sprintf("sourceOutFlow (%d) != sinkInFlow (%d)", sourceOut, sinkIn))

	// g.ComputeInEdges()
	// TODO: check Neighbours
	// TODO: Check inflow == outflow for all vertices (doesn't seem to be easy)
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
