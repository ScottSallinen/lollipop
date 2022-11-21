package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

func info(args ...any) {
	log.Println("[MaxFlowNew]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProp] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])
	capacity, _ := strconv.Atoi(stringFields[2])

	return graph.RawEdge[EdgeProp]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProp{uint32(capacity)}}
}

func OnCheckCorrectness(g *Graph, sourceRaw, sinkRaw uint32) error {
	source := &g.Vertices[g.VertexMap[sourceRaw]]
	sink := &g.Vertices[g.VertexMap[sinkRaw]]
	enforce.ENFORCE(source.Property.Type == Source)
	enforce.ENFORCE(sink.Property.Type == Sink)

	// Check heights
	enforce.ENFORCE(source.Property.Height == int64(len(g.Vertices)), "source height != # of vertices")
	enforce.ENFORCE(sink.Property.Height == 0, "sink height != 0")

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
			sumEdgeCapacityOriginal := int64(0)
			sumEdgeCapacityResidual := int64(0)
			for ei := range v.OutEdges {
				sumEdgeCapacityOriginal += int64(v.OutEdges[ei].Property.Capacity)
			}
			for _, neighbour := range v.Property.Nbrs {
				sumEdgeCapacityResidual += neighbour.ResCap
			}
			enforce.ENFORCE(sumEdgeCapacityOriginal == sumEdgeCapacityResidual, fmt.Sprintf(
				"normal vertex index %d ID %d sumEdgeCapacityOriginal (%d) != sumEdgeCapacityResidual (%d)",
				vi, v.Id, sumEdgeCapacityOriginal, sumEdgeCapacityResidual),
			)
		}
	}

	// Check sourceOut and sinkIn
	sinkIn := sink.Property.Excess
	sourceOut := int64(0)
	for ei := range source.OutEdges {
		edge := &source.OutEdges[ei]
		sourceOut += int64(edge.Property.Capacity)
	}
	sourceOut -= source.Property.Excess
	enforce.ENFORCE(sourceOut == sinkIn, fmt.Sprintf("sourceOutFlow (%d) != sinkInFlow (%d)", sourceOut, sinkIn))

	// g.ComputeInEdges()
	// TODO: Check inflow == outflow for all vertices (doesn't seem to be easy)

	PrintMessageCounts()
	ResetMessageCounts()
	return nil
}

func GetFrameworkAndGraph(sourceRaw, sinkRaw, sourceHeight uint32) (*framework.Framework[VertexProp, EdgeProp, MessageValue], *Graph) {
	enforce.ENFORCE(sourceRaw != sinkRaw)
	tempVertexCount = sourceHeight

	g := Graph{}
	g.Options = graph.GraphOptions[MessageValue]{
		Undirected:    false,
		EmptyVal:      nil,
		LogTimeseries: false,
		OracleCompare: false,
		SourceInit:    false,
		InitAllMessage: MessageValue{{
			Type:   Init,
			Source: EmptyValue,
			Height: EmptyValue,
			Value:  EmptyValue,
		}},
	}

	frame := framework.Framework[VertexProp, EdgeProp, MessageValue]{}
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve
	frame.EdgeParser = EdgeParser
	frame.OnInitVertex = func(g *Graph, vidx uint32) {
		v := &g.Vertices[vidx]
		switch v.Id {
		case sourceRaw:
			v.Property.Type = Source
			v.Property.Height = int64(sourceHeight)
		case sinkRaw:
			v.Property.Type = Sink
			v.Property.Height = 0
		default:
			v.Property.Type = Normal
			v.Property.Height = math.MaxUint32 // OPTIMIZATION
		}
		v.Property.Nbrs = make(map[uint32]Nbr)
		v.Property.Excess = 0
	}
	frame.OnCheckCorrectness = func(g *Graph) error {
		return OnCheckCorrectness(g, sourceRaw, sinkRaw)
	}

	return &frame, &g
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, source, sink, sourceHeight uint32) *Graph {
	frame, g := GetFrameworkAndGraph(source, sink, sourceHeight)
	frame.Launch(g, gName, async, dynamic)
	return g
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
	vertices := flag.Uint("vertices", 1, "Number of vertices") // TODO: remove
	flag.Parse()

	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(*gptr, *aptr, *dptr, uint32(*source), uint32(*sink), uint32(*vertices))

	g.ComputeGraphStats(false, false)

	if *pptr {
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}
