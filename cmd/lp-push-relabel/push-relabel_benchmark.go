package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
)

type testCase struct {
	MaxFlow     int64
	Source      uint32
	Sink        uint32
	Filename    string
	VertexCount uint32
}

const (
	GraphPath = "D:\\common\\hive-comments.txt"
	GraphSize = 671295
)

var (
	testCasesBenchmark = [...]testCase{
		// These don't need Global Relabeling
		{3, 23505, 18965, GraphPath, GraphSize},
		{1, 629280, 367395, GraphPath, GraphSize},
		{0, 163178, 652920, GraphPath, GraphSize},
		{3, 620597, 441410, GraphPath, GraphSize},
		{0, 573253, 168575, GraphPath, GraphSize},
		{1, 658312, 33793, GraphPath, GraphSize},
	}
	baseOptionsBenchmark = graph.GraphOptions{
		CheckCorrectness: true,
		NumThreads:       10,
		QueueMultiplier:  9,
	}
)

func runBenchmark(run func(options graph.GraphOptions) (int32, int64), options graph.GraphOptions) (algTimes []int64, avg int64) {
	for _, tc := range testCasesBenchmark {
		options.Name = tc.Filename
		sourceRawId = graph.RawType(tc.Source)
		sinkRawId = graph.RawType(tc.Sink)
		VertexCountHelper.Reset(int64(tc.VertexCount))

		options.NumThreads = 16
		options.QueueMultiplier = 9
		initialHeight = MaxHeight
		//options.Profile = true

		maxFlow, algTime := run(options)
		assert(tc.MaxFlow == int64(maxFlow), "")
		algTimes = append(algTimes, algTime/1_000_000)
	}
	return algTimes, utils.Sum(algTimes) / int64(len(algTimes))
}

func RunBenchmarks() {
	algTimesMsg, avgMsg := runBenchmark(RunMsg, baseOptionsBenchmark)
	algTimesAgg, avgAgg := runBenchmark(RunAgg, baseOptionsBenchmark)
	log.Info().Msg(fmt.Sprintf("Agg - Algorithm runtimes: %v", algTimesAgg))
	log.Info().Msg(fmt.Sprintf("Agg - Algorithm runtime average: %v", avgAgg))
	log.Info().Msg(fmt.Sprintf("Msg - Algorithm runtimes: %v", algTimesMsg))
	log.Info().Msg(fmt.Sprintf("Msg - Algorithm runtime average: %v", avgMsg))
}
