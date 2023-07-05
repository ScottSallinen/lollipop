package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/ScottSallinen/lollipop/graph"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/a"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/b"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/c"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/d"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/e"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/f"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/g"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/h"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/i"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/j"
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
		{1, 6786, 7895, CommentGraphSamplePath, CommentGraphSampleSize},
		{0, 15358, 9845, CommentGraphSamplePath, CommentGraphSampleSize},
		{8, 16632, 9492, CommentGraphSamplePath, CommentGraphSampleSize},
		{21, 3709, 7135, CommentGraphSamplePath, CommentGraphSampleSize},
		{137, 11665, 3721, CommentGraphSamplePath, CommentGraphSampleSize},
		{1087, 1568, 363, CommentGraphSamplePath, CommentGraphSampleSize},

		// Too slow without Global Relabeling
		//{0, 5000, 10000, CommentGraphSamplePath, CommentGraphSampleSize},
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

func TestMain(m *testing.M) {
	c := m.Run()
	os.Exit(c)
}

func TestAggHAsyncStatic(t *testing.T) {
	options := baseOptions
	RunTestGraphs(t, a.RunAggH, "AggregateHashtable", options)
}

func TestAggHIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	RunTestGraphs(t, a.RunAggH, "AggregateHashtable", options)
}

func TestMsgHAsyncStatic(t *testing.T) {
	options := baseOptions
	RunTestGraphs(t, b.RunMsgH, "MessagePassingHashtable", options)
}

func TestMsgHIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	RunTestGraphs(t, b.RunMsgH, "MessagePassingHashtable", options)
}

func TestMsgAAsyncStatic(t *testing.T) {
	options := baseOptions
	RunTestGraphs(t, c.RunMsgA, "MessagePassingArray", options)
}

func TestMsgAIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	RunTestGraphs(t, c.RunMsgA, "MessagePassingArray", options)
}

func TestDAsyncStatic(t *testing.T) {
	options := baseOptions
	options.QueueMultiplier = 2
	RunTestGraphs(t, d.Run, d.Name, options)
}

func TestDIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	options.QueueMultiplier = 2
	RunTestGraphs(t, d.Run, d.Name, options)
}

func TestEAsyncStatic(t *testing.T) {
	options := baseOptions
	options.QueueMultiplier = 2
	RunTestGraphs(t, e.Run, e.Name, options)
}

func TestFAsyncStatic(t *testing.T) {
	options := baseOptions
	options.QueueMultiplier = 2
	RunTestGraphs(t, f.Run, f.Name, options)
}

func TestGAsyncStatic(t *testing.T) {
	options := baseOptions
	options.QueueMultiplier = 2
	RunTestGraphs(t, g.Run, g.Name, options)
}

func TestHAsyncStatic(t *testing.T) {
	options := baseOptions
	options.QueueMultiplier = 2
	RunTestGraphs(t, h.Run, h.Name, options)
}

func TestHIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	options.QueueMultiplier = 2
	RunTestGraphs(t, h.Run, h.Name, options)
}

func TestIAsyncStatic(t *testing.T) {
	options := baseOptions
	options.QueueMultiplier = 2
	RunTestGraphs(t, i.Run, i.Name, options)
}

func TestIIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	options.QueueMultiplier = 2
	RunTestGraphs(t, i.Run, i.Name, options)
}

func TestJAsyncStatic(t *testing.T) {
	options := baseOptions
	options.QueueMultiplier = 2
	RunTestGraphs(t, j.Run, j.Name, options)
}

func TestJIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	options.QueueMultiplier = 2
	RunTestGraphs(t, j.Run, j.Name, options)
}

func RunTestGraphs[V graph.VPI[V], E graph.EPI[E], M graph.MVI[M], N any](t *testing.T,
	run func(options graph.GraphOptions) (maxFlow int32, g *graph.Graph[V, E, M, N]),
	prefix string, options graph.GraphOptions) {

	for _, tg := range testGraphs {
		for i := 0; i < 5; i++ {
			options.Name = tg.Filename
			options.NumThreads = uint32(rand.Intn(MaxThreads-1) + 1)
			InitialHeight = MaxHeight
			SourceRawId = graph.RawType(tg.Source)
			SinkRawId = graph.RawType(tg.Sink)
			VertexCountHelper.Reset(int64(tg.VertexCount))

			maxFlow, _ := run(options)
			assertEqual(t, tg.MaxFlow, int64(maxFlow), fmt.Sprintf("%s: Graph \"%s\" Max flow", prefix, tg.Filename))
		}
	}
}
