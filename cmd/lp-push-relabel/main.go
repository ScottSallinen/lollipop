package main

import (
	"flag"
	"fmt"
	"os"

	"golang.org/x/exp/slices"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/m"
)

func main() {
	if slices.Contains(os.Args, "-B") || slices.Contains(os.Args, "--B") {
		RunBenchmarks()
		return
	}

	sourceId := flag.Int("S", -1, "Source vertex (raw id).")
	sinkId := flag.Int("T", -1, "Sink vertex (raw id).")
	disableGlobalRelabeling := flag.Bool("DG", false, "Disable global relabeling")
	stability := flag.Bool("Stability", false, "Collect statistics related to solution stability.")
	flag.Bool("B", false, "Run benchmarks")
	graphOptions := graph.FlagsToOptions()

	if *stability {
		if !graphOptions.LogTimeseries {
			log.Panic().Msg("Cannot analyze stability without timeseries")
		}
		graphOptions.OracleCompare = true
	}

	InitialHeight = MaxHeight
	SourceRawId = graph.RawType(*sourceId)
	SinkRawId = graph.RawType(*sinkId)
	GlobalRelabelingEnabled = !*disableGlobalRelabeling

	m.CheckStability = *stability
	mf, _ := m.Run(graphOptions)
	log.Info().Msg(fmt.Sprintf("Maximum flow is %v", mf))

	if *stability {
		m.PrintVertexFlowDB(true, false)
	}
}
