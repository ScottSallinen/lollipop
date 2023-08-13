package main

import (
	"fmt"
	"runtime"
	"strconv"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/constraints"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/a"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/b"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/h"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/i"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/m"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/n"
)

type benchmarkTestCase struct {
	MaxFlow int64
	Source  uint32
	Sink    uint32
	Graph   TestGraph
}

type benchmarkResult struct {
	name      string
	runtimes  []int64
	latencies []int64
	messages  []uint64
}

var (
	benchmarkOneTestCases = []benchmarkTestCase{
		// These don't need Global Relabeling
		{3, 23505, 18965, HiveComments},
		{1, 629280, 367395, HiveComments},
		{0, 163178, 652920, HiveComments},
		{3, 620597, 441410, HiveComments},
		{0, 573253, 168575, HiveComments},
		{1, 658312, 33793, HiveComments},
		// These need Global Relabeling
		// {48, 54646, 266979, HiveComments},
	}
	benchmarkDynamicCases = []benchmarkTestCase{
		{98018, 5880, 38806, HiveComments},
		{98018, 5880, 38806, HiveComments},
		// {98018, 5880, 38806, HiveComments},
		// {98018, 5880, 38806, HiveComments},
		// {98018, 5880, 38806, HiveComments},

		{2299228, 492, 60, Ethereum},
		{2299228, 492, 60, Ethereum},
		// {2299228, 492, 60, Ethereum},
		// {2299228, 492, 60, Ethereum},
		// {2299228, 492, 60, Ethereum},
	}
	benchmarkCasesTimeseries = []benchmarkTestCase{
		{98018, 5880, 38806, HiveComments},
		// {98018, 5880, 38806, HiveComments},
		// {98018, 5880, 38806, HiveComments},
	}
	benchmarkCasesScalability = []benchmarkTestCase{
		{2299228, 492, 60, Ethereum},
		{2299228, 492, 60, Ethereum},
		{2299228, 492, 60, Ethereum},
	}
	benchmarkBaseOptions = graph.GraphOptions{
		CheckCorrectness:    true,
		AlgTimeIncludeQuery: true,
		QueueMultiplier:     8,
		NumThreads:          uint32(runtime.NumCPU()),
	}
)

func runBenchmark[V graph.VPI[V], E graph.EPI[E], M graph.MVI[M], N any, MF constraints.Integer](
	run func(options graph.GraphOptions) (maxFlow MF, g *graph.Graph[V, E, M, N]),
	options graph.GraphOptions,
	name string,
	testCases []benchmarkTestCase,
) (result benchmarkResult) {

	log.Info().Msg(fmt.Sprintf("%s - Start", name))
	result.name = name
	for _, tc := range testCases {
		options.Name = tc.Graph.Path
		options.TimestampPos = tc.Graph.TimestampPos
		options.WeightPos = tc.Graph.WeightPos

		SourceRawId = graph.RawType(tc.Source)
		SinkRawId = graph.RawType(tc.Sink)
		VertexCountHelper.Reset(1000)
		TimeSeriesReset()

		InitialHeight = MaxHeight

		runtime.GC()
		maxFlow, g := run(options)
		runtime.GC()
		Assert(tc.MaxFlow == int64(maxFlow), "")

		msgSend := uint64(0)
		for t := 0; t < int(g.NumThreads); t++ {
			msgSend += g.GraphThreads[t].MsgSend
		}

		latency := int64(0)
		if len(TsDB) > 0 {
			for _, ts := range TsDB {
				latency += ts.Latency.Milliseconds()
			}
			latency = latency / int64(len(TsDB))
		}

		result.runtimes = append(result.runtimes, int64(g.AlgTimer.Elapsed()/1_000_000))
		result.messages = append(result.messages, msgSend)
		result.latencies = append(result.latencies, latency)
	}
	return result
}

func RunBenchmarks() {
	utils.SetLevel(0)
	BenchmarkMNDynamic()
}

// Static, Old
func BenchmarkOne() {
	options := benchmarkBaseOptions

	results := make([]benchmarkResult, 0)
	results = append(results, runBenchmark(a.RunAggH, options, "AggH", benchmarkOneTestCases))
	results = append(results, runBenchmark(b.RunMsgH, options, "MsgH", benchmarkOneTestCases))
	results = append(results, runBenchmark(h.Run, options, h.Name, benchmarkOneTestCases))
	results = append(results, runBenchmark(i.Run, options, i.Name, benchmarkOneTestCases))

	originalGre := GlobalRelabelingEnabled
	GlobalRelabelingEnabled = true
	results = append(results, runBenchmark(m.Run, options, m.Name, benchmarkOneTestCases))
	GlobalRelabelingEnabled = false
	results = append(results, runBenchmark(m.Run, options, m.Name, benchmarkOneTestCases))
	GlobalRelabelingEnabled = originalGre

	printResults(results)
}

func BenchmarkLRateLimit() {
	options := benchmarkBaseOptions
	results := make([]benchmarkResult, 0)
	options.Dynamic = true
	options.LogTimeseries = true
	options.TimeSeriesInterval = 86400 * 512

	baseRate := 50_000
	for rateMultiplier := 1; rateMultiplier <= 5; rateMultiplier += 1 {
		options.TargetRate = float64(baseRate * rateMultiplier)
		results = append(results, runBenchmark(m.Run, options, m.Name+" with rate "+strconv.Itoa(int(options.TargetRate)), benchmarkCasesTimeseries))
	}

	printResults(results)
}

func BenchmarkLScalability() {
	options := benchmarkBaseOptions
	GlobalRelabelingEnabled = true

	results := make([]benchmarkResult, 0)

	for t := 4; t <= 16; t += 4 {
		options.NumThreads = uint32(t)
		results = append(results, runBenchmark(m.Run, options, m.Name+" with t="+strconv.Itoa(t)+" rate="+strconv.Itoa(int(options.TargetRate)), benchmarkCasesScalability))
	}

	printResults(results)
}

func BenchmarkMNDynamic() {
	options := benchmarkBaseOptions
	options.DebugLevel = 2
	// options.Dynamic = true
	options.NumThreads = 10

	results := make([]benchmarkResult, 0)
	// results = append(results, runBenchmark(m.Run, options, m.Name, benchmarkDynamicCases))
	results = append(results, runBenchmark(n.Run, options, n.Name, benchmarkDynamicCases))

	printResults(results)
}

func printResults(results []benchmarkResult) {
	for _, r := range results {
		log.Info().Msg(fmt.Sprintf("%s - Algorithm message counts: %v", r.name, r.messages))
		log.Info().Msg(fmt.Sprintf("%s - Algorithm runtimes: %v", r.name, r.runtimes))
		log.Info().Msg(fmt.Sprintf("%s - Algorithm latencies: %v", r.name, r.latencies))
		log.Info().Msg(fmt.Sprintf("%s - Algorithm message count average: %v", r.name, utils.Sum(r.messages)/uint64(len(r.messages))))
		log.Info().Msg(fmt.Sprintf("%s - Algorithm runtime average: %v", r.name, utils.Sum(r.runtimes)/int64(len(r.runtimes))))
		log.Info().Msg(fmt.Sprintf("%s - Algorithm latencies average: %v", r.name, utils.Sum(r.latencies)/int64(len(r.latencies))))
	}
}
