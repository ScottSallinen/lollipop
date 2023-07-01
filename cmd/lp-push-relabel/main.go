package main

import (
	"flag"
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
)

func assert(cond bool, msg string) {
	if !cond {
		log.Panic().Msg(msg)
	}
}

type VertexType uint8

const (
	EmptyValue = math.MaxUint32
	MaxHeight  = math.MaxUint32

	Normal VertexType = 0
	Source VertexType = 1
	Sink   VertexType = 2
)

// Set these flags before running the algorithm
var (
	initialHeight = int64(0) // The initial height of a vertex that is newly added to the graph.
	sourceRawId   = graph.AsRawType(-1)
	sinkRawId     = graph.AsRawType(-1)
)

func (t VertexType) String() string {
	switch t {
	case Normal:
		return "Normal"
	case Source:
		return "Source"
	case Sink:
		return "Sink"
	default:
		log.Panic().Msg("Unknown VertexType: " + string(t))
		return string(t)
	}
}

func RunAggH(options graph.GraphOptions) (maxFlow int32, g *graph.Graph[VPropAgg, EPropAgg, MessageAgg, NoteAgg]) {
	alg := new(PushRelabelAgg)
	g = graph.LaunchGraphExecution[*EPropAgg, VPropAgg, EPropAgg, MessageAgg, NoteAgg](alg, options)
	return alg.GetMaxFlowValue(g), g
}

func RunMsgH(options graph.GraphOptions) (maxFlow int32, g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]) {
	alg := new(PushRelabelMsg)
	g = graph.LaunchGraphExecution[*EPropMsg, VPropMsg, EPropMsg, MessageMsg, NoteMsg](alg, options)
	return alg.GetMaxFlowValue(g), g
}

func RunMsgA(options graph.GraphOptions) (maxFlow int32, g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA]) {
	alg := new(PushRelabelMsgA)
	g = graph.LaunchGraphExecution[*EdgePMsgA, VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA](alg, options)
	return alg.GetMaxFlowValue(g), g
}

func main() {
	sourceId := flag.Int("S", -1, "Source vertex (raw id).")
	sinkId := flag.Int("T", -1, "Sink vertex (raw id).")
	initialEstimatedCount := flag.Uint("V", 30, "Initial estimated number of vertices.")
	implementation := flag.String("I", "agg", "Implementation. Can be aggregation (agg) or direct message passing (msg)")
	benchmark := flag.Bool("B", false, "Run benchmarks")
	graphOptions := graph.FlagsToOptions()

	if *benchmark {
		RunBenchmarks()
		return
	}

	initialHeight = 0
	sourceRawId = graph.RawType(*sourceId)
	sinkRawId = graph.RawType(*sinkId)
	VertexCountHelper.Reset(int64(*initialEstimatedCount))

	switch *implementation {
	case "agg":
		RunAggH(graphOptions)
	case "msg-h":
		RunMsgH(graphOptions)
	case "msg-a":
		RunMsgA(graphOptions)
	default:
		log.Fatal().Msg("Unknown implementation: " + *implementation)
	}
}
