package main

import (
	"flag"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

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

	InitialHeight = 0
	SourceRawId = graph.RawType(*sourceId)
	SinkRawId = graph.RawType(*sinkId)
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
