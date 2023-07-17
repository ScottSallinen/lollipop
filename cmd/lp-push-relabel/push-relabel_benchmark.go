package main

import (
	"fmt"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/constraints"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/a"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/b"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/h"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/i"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/j"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/k"
)

type testCase struct {
	MaxFlow     int64
	Source      uint32
	Sink        uint32
	Filename    string
	VertexCount uint32
}

type benchmarkResult struct {
	name     string
	runtimes []int64
	messages []uint64
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
		// These need Global Relabeling
		{48, 54646, 266979, GraphPath, GraphSize},
	}
	baseOptionsBenchmark = graph.GraphOptions{
		CheckCorrectness:      true,
		NumThreads:            16,
		QueueMultiplier:       8,
		TimestampPos:          1,
		AllowAsyncVertexProps: false,
		AlgTimeIncludeQuery:   true,
	}
)

func runBenchmark[V graph.VPI[V], E graph.EPI[E], M graph.MVI[M], N any, MF constraints.Integer](
	run func(options graph.GraphOptions) (maxFlow MF, g *graph.Graph[V, E, M, N]),
	options graph.GraphOptions, name string) (result benchmarkResult) {

	log.Info().Msg(fmt.Sprintf("%s - Start", name))
	result.name = name
	for _, tc := range testCasesBenchmark {
		options.Name = tc.Filename
		SourceRawId = graph.RawType(tc.Source)
		SinkRawId = graph.RawType(tc.Sink)
		GlobalRelabelingHelper.UpdateInterval(int64(tc.VertexCount))
		if options.Dynamic {
			VertexCountHelper.Reset(1000)
		} else {
			VertexCountHelper.Reset(int64(tc.VertexCount))
		}

		InitialHeight = MaxHeight
		//options.Profile = true

		maxFlow, g := run(options)
		Assert(tc.MaxFlow == int64(maxFlow), "")

		msgSend := uint64(0)
		for t := 0; t < int(g.NumThreads); t++ {
			msgSend += g.GraphThreads[t].MsgSend
		}

		result.runtimes = append(result.runtimes, int64(g.AlgTimer.Elapsed()/1_000_000))
		result.messages = append(result.messages, msgSend)
	}
	return result
}

func RunBenchmarks() {
	options := baseOptionsBenchmark

	//options.Dynamic = true

	//options.LogTimeseries = true
	//options.TimeSeriesInterval = 86400 * 7
	//options.OracleCompare = true

	results := make([]benchmarkResult, 0, 4)
	results = append(results, runBenchmark(a.RunAggH, options, "AggH"))
	results = append(results, runBenchmark(b.RunMsgH, options, "MsgH"))
	results = append(results, runBenchmark(h.Run, options, h.Name))
	results = append(results, runBenchmark(i.Run, options, i.Name))
	results = append(results, runBenchmark(j.Run, options, j.Name))
	GlobalRelabelingEnabled = true
	results = append(results, runBenchmark(k.Run, options, k.Name))
	GlobalRelabelingEnabled = false
	results = append(results, runBenchmark(k.Run, options, k.Name))
	for _, r := range results {
		log.Info().Msg(fmt.Sprintf("%s - Algorithm message counts: %v", r.name, r.messages))
		log.Info().Msg(fmt.Sprintf("%s - Algorithm runtimes: %v", r.name, r.runtimes))
		log.Info().Msg(fmt.Sprintf("%s - Algorithm runtime average: %v", r.name, utils.Sum(r.runtimes)/int64(len(r.runtimes))))
	}
}
