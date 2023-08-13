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

type TestGraph struct {
	Path         string
	TimestampPos int32
	WeightPos    int32
	VertexCount  uint64 // Optional
}

var (
	SmallOne = TestGraph{
		Path:        "../../data/small-weighted-1.txt",
		VertexCount: 4,
	}
	SmallTwo = TestGraph{
		Path:        "../../data/small-weighted-2.txt",
		VertexCount: 3,
	}
	HiveCommentsSample = TestGraph{
		Path:         "../../data/comment-graph-sample.txt",
		TimestampPos: 1,
		VertexCount:  17500,
	}
	HiveComments = TestGraph{
		Path:         "/home/luuo/hive-comments.txt",
		TimestampPos: 1,
		VertexCount:  671295,
	}
	Ethereum = TestGraph{
		Path:        "/home/luuo/eth-transfers-t200m.txt.p",
		WeightPos:   1,
		VertexCount: 5672202,
	}
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
