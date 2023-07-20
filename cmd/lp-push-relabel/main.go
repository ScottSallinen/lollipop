package main

import (
	"flag"
	"fmt"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/l"
)

func main() {
	sourceId := flag.Int("S", -1, "Source vertex (raw id).")
	sinkId := flag.Int("T", -1, "Sink vertex (raw id).")
	initialEstimatedCount := flag.Uint("V", 30, "Initial estimated number of vertices.")
	benchmark := flag.Bool("B", false, "Run benchmarks")
	globalRelabeling := flag.Bool("G", false, "Enable global relabeling")
	graphOptions := graph.FlagsToOptions()

	if *benchmark {
		RunBenchmarks()
		return
	}

	InitialHeight = MaxHeight
	SourceRawId = graph.RawType(*sourceId)
	SinkRawId = graph.RawType(*sinkId)
	GlobalRelabelingEnabled = *globalRelabeling
	VertexCountHelper.Reset(int64(*initialEstimatedCount))

	mf, _ := l.Run(graphOptions)
	log.Info().Msg(fmt.Sprintf("Maximum flow is %v", mf))
}
