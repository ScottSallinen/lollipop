package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"golang.org/x/exp/constraints"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/m"
	"github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/explore/n"
)

type TestCase struct {
	Graph   TestGraph
	Source  uint32
	Sink    uint32
	MaxFlow int64 // -1 to skip verification
}

const (
	CommentGraphSamplePath = "../../data/comment-graph-sample.txt"
	CommentGraphSampleSize = 17500
	MaxThreads             = 8
)

var (
	testGraphs = [...]TestCase{
		{SmallOne, 0, 3, 2},
		{SmallTwo, 0, 2, 8},
		{HiveCommentsSample, 0, 1, 1},
		{HiveCommentsSample, 6786, 7895, 1},
		{HiveCommentsSample, 15358, 9845, 0},
		{HiveCommentsSample, 16632, 9492, 8},
		{HiveCommentsSample, 3709, 7135, 21},
		{HiveCommentsSample, 11665, 3721, 137},
		{HiveCommentsSample, 1568, 363, 1087},

		// Too slow without Global Relabeling
		{HiveCommentsSample, 5000, 10000, 0},
		{HiveCommentsSample, 6272, 4356, 162},
	}
	baseOptions = graph.GraphOptions{
		CheckCorrectness: true,
		QueueMultiplier:  2,
		DebugLevel:       2,
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

// func TestAggHAsyncStatic(t *testing.T) {
// 	options := baseOptions
// 	RunTestGraphs(t, a.RunAggH, "AggregateHashtable", options)
// }

// func TestAggHIncremental(t *testing.T) {
// 	options := baseOptions
// 	options.Dynamic = true
// 	RunTestGraphs(t, a.RunAggH, "AggregateHashtable", options)
// }

// func TestBAsyncStatic(t *testing.T) {
// 	options := baseOptions
// 	RunTestGraphs(t, b.RunMsgH, "MessagePassingHashtable", options)
// }

// func TestBIncremental(t *testing.T) {
// 	options := baseOptions
// 	options.Dynamic = true
// 	RunTestGraphs(t, b.RunMsgH, "MessagePassingHashtable", options)
// }

// func TestFAsyncStatic(t *testing.T) {
// 	options := baseOptions
// 	RunTestGraphs(t, f.Run, f.Name, options)
// }

// func TestGAsyncStatic(t *testing.T) {
// 	options := baseOptions
// 	RunTestGraphs(t, g.Run, g.Name, options)
// }

// func TestHAsyncStatic(t *testing.T) {
// 	options := baseOptions
// 	RunTestGraphs(t, h.Run, h.Name, options)
// }

// func TestHIncremental(t *testing.T) {
// 	options := baseOptions
// 	options.Dynamic = true
// 	RunTestGraphs(t, h.Run, h.Name, options)
// }

// func TestIAsyncStatic(t *testing.T) {
// 	options := baseOptions
// 	RunTestGraphs(t, i.Run, i.Name, options)
// }

// func TestIIncremental(t *testing.T) {
// 	options := baseOptions
// 	options.Dynamic = true
// 	RunTestGraphs(t, i.Run, i.Name, options)
// }

func TestMAsyncStatic(t *testing.T) {
	options := baseOptions
	RunTestGraphs(t, m.Run, m.Name, options)
}

func TestMIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	RunTestGraphs(t, m.Run, m.Name, options)
}

func TestNAsyncStatic(t *testing.T) {
	options := baseOptions
	RunTestGraphs(t, n.Run, n.Name, options)
}

func TestNIncremental(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	options.TargetRate = 1000000
	RunTestGraphs(t, n.Run, n.Name, options)
}

func TestNDeletes(t *testing.T) {
	options := baseOptions
	options.Dynamic = true
	options.TargetRate = 1000000
	options.InsertDeleteOnExpire = (24 * 60 * 60)
	RunTestGraphs(t, n.Run, n.Name, options)
}

func RunTestGraphs[V graph.VPI[V], E graph.EPI[E], M graph.MVI[M], N any, MF constraints.Integer](
	t *testing.T,
	run func(options graph.GraphOptions) (maxFlow MF, g *graph.Graph[V, E, M, N]),
	prefix string, options graph.GraphOptions) {

	for _, tg := range testGraphs {
		options.Name = tg.Graph.Path
		options.TimestampPos = tg.Graph.TimestampPos
		options.WeightPos = tg.Graph.WeightPos
		if options.InsertDeleteOnExpire > 0 && options.TimestampPos == 0 {
			continue
		}
		for i := 0; i < 5; i++ {
			options.NumThreads = uint32(rand.Intn(MaxThreads-1) + 1)
			InitialHeight = MaxHeight
			SourceRawId = graph.RawType(tg.Source)
			SinkRawId = graph.RawType(tg.Sink)
			if options.Dynamic {
				VertexCountHelper.Reset(utils.Min(1000, int64(tg.Graph.VertexCount)))
			} else {
				VertexCountHelper.Reset(int64(tg.Graph.VertexCount))
			}

			maxFlow, _ := run(options)
			if options.InsertDeleteOnExpire == 0 {
				assertEqual(t, tg.MaxFlow, int64(maxFlow), fmt.Sprintf("%s: Graph \"%s\" Max flow", prefix, tg.Graph.Path))
			}
		}
	}
}
