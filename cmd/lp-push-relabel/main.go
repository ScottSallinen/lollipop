package main

import (
	"flag"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/pr-a"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/pr-b"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/pr-c"
)

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
		pr_a.RunAggH(graphOptions)
	case "msg-h":
		pr_b.RunMsgH(graphOptions)
	case "msg-a":
		pr_c.RunMsgA(graphOptions)
	default:
		log.Fatal().Msg("Unknown implementation: " + *implementation)
	}
}
