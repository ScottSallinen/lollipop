package main

import (
	"flag"
	"fmt"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/k"
)

func main() {
	sourceId := flag.Int("S", -1, "Source vertex (raw id).")
	sinkId := flag.Int("T", -1, "Sink vertex (raw id).")
	initialEstimatedCount := flag.Uint("V", 30, "Initial estimated number of vertices.")
	//implementation := flag.String("I", "agg", "Implementation. see README.md")
	benchmark := flag.Bool("B", false, "Run benchmarks")
	globalRelabeling := flag.Bool("G", false, "Enable global relabeling")
	graphOptions := graph.FlagsToOptions()

	if *benchmark {
		RunBenchmarks()
		return
	}

	InitialHeight = 0
	SourceRawId = graph.RawType(*sourceId)
	SinkRawId = graph.RawType(*sinkId)
	GlobalRelabelingEnabled = *globalRelabeling
	VertexCountHelper.Reset(int64(*initialEstimatedCount))

	//switch *implementation {
	//case "agg":
	//	a.RunAggH(graphOptions)
	//case "msg-h":
	//	b.RunMsgH(graphOptions)
	//case "msg-a":
	//	c.RunMsgA(graphOptions)
	//default:
	//	log.Fatal().Msg("Unknown implementation: " + *implementation)
	//}
	mf, _ := k.Run(graphOptions)
	log.Info().Msg(fmt.Sprintf("Maximum flow is %v", mf))
}
