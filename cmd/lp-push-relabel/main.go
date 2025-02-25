package main

import (
	"flag"
	"fmt"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
)

type TestGraph struct {
	Path         string
	TimestampPos int8
	WeightPos    int8
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
	sourceId := flag.Int("S", -1, "Source vertex (raw id).")
	sinkId := flag.Int("T", -1, "Sink vertex (raw id).")
	stability := flag.Bool("Stability", false, "Collect statistics related to solution stability.")
	flag.Bool("B", false, "Run benchmarks")
	graphOptions := graph.FlagsToOptions()

	if *stability {
		if !graphOptions.LogTimeseries {
			log.Panic().Msg("Cannot analyze stability without timeseries")
		}
		graphOptions.OracleCompare = true
	}

	SourceRawId = graph.RawType(*sourceId)
	SinkRawId = graph.RawType(*sinkId)

	CheckStability = *stability
	mf, _ := Run(graphOptions)
	log.Info().Msg(fmt.Sprintf("Maximum flow is %v", mf))

	if *stability {
		PrintVertexFlowDB(true, false)
	}
}
