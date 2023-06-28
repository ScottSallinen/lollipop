package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/ScottSallinen/lollipop/graph"
)

type TestGraph struct {
	MaxFlow     int64
	Source      uint32
	Sink        uint32
	Filename    string
	VertexCount uint32
}

const (
	CommentGraphSamplePath = "../../data/comment-graph-sample.txt"
	CommentGraphSampleSize = 17500
	MaxThreads             = 8
)

var (
	testGraphs = [...]TestGraph{
		{2, 0, 3, "../../data/small-weighted-1.txt", 4},
		{8, 0, 2, "../../data/small-weighted-2.txt", 3},
		{1, 0, 1, CommentGraphSamplePath, CommentGraphSampleSize},
		{0, 5000, 10000, CommentGraphSamplePath, CommentGraphSampleSize},
		{1, 6786, 7895, CommentGraphSamplePath, CommentGraphSampleSize},
		{0, 15358, 9845, CommentGraphSamplePath, CommentGraphSampleSize},
		{8, 16632, 9492, CommentGraphSamplePath, CommentGraphSampleSize},
		{21, 3709, 7135, CommentGraphSamplePath, CommentGraphSampleSize},
		{137, 11665, 3721, CommentGraphSamplePath, CommentGraphSampleSize},
		{1087, 1568, 363, CommentGraphSamplePath, CommentGraphSampleSize},

		// Too slow without Global Relabeling
		//{162, 6272, 4356, CommentGraphSamplePath, CommentGraphSampleSize},
	}
	baseOptions = graph.GraphOptions{
		CheckCorrectness: true,
	}
)

func assertEqual[C comparable](t *testing.T, expected C, actual C, prefix string) {
	if expected == actual {
		return
	}
	if prefix == "" {
		t.Fatalf("%v != %v", expected, actual)
	} else {
		t.Fatalf("%v - expected %v, got %v", prefix, expected, actual)
	}
}

var RunnerAgg = func(options graph.GraphOptions) int32 {
	alg := new(PushRelabelAgg)
	g := graph.LaunchGraphExecution[*EPropAgg, VPropAgg, EPropAgg, MessageAgg, NoteAgg](alg, options)
	return alg.GetMaxFlowValue(g)
}

var RunnerMsg = func(options graph.GraphOptions) int32 {
	alg := new(PushRelabelMsg)
	g := graph.LaunchGraphExecution[*EPropMsg, VPropMsg, EPropMsg, MessageMsg, NoteMsg](alg, options)
	return alg.GetMaxFlowValue(g)
}

func TestMain(m *testing.M) {
	c := m.Run()
	os.Exit(c)
}

func TestAggAsyncStatic(t *testing.T) {
	options := baseOptions
	RunTestGraphs(t, RunnerAgg, "Aggregate", options)
}

func TestAggIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	RunTestGraphs(t, RunnerAgg, "Aggregate", options)
}

func TestMsgAsyncStatic(t *testing.T) {
	options := baseOptions
	//options.QueueMultiplier = 9
	RunTestGraphs(t, RunnerMsg, "MessagePassing", options)
}

// TODO Dynamic is not yet implemented
//func TestIncrementalMsg(t *testing.T) {
//	options := baseOptions
//	options.QueueMultiplier = 9
//	options.Dynamic = true
//	RunTestGraphs(t, RunnerMsg, "MessagePassing", options)
//}

func RunTestGraphs(t *testing.T, run func(options graph.GraphOptions) int32, prefix string, options graph.GraphOptions) {
	for _, tg := range testGraphs {
		for i := 0; i < 5; i++ {
			options.Name = tg.Filename
			options.NumThreads = uint32(rand.Intn(MaxThreads-1) + 1)
			initialHeight = MaxHeight
			sourceRawId = graph.RawType(tg.Source)
			sinkRawId = graph.RawType(tg.Sink)
			VertexCountHelper.Reset(int64(tg.VertexCount))

			maxFlow := run(options)
			assertEqual(t, tg.MaxFlow, int64(maxFlow), fmt.Sprintf("%s: Graph \"%s\" Max flow", prefix, tg.Filename))
		}
	}
}
