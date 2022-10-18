package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"math/rand"
	"reflect"
	"testing"
)

type TestGraph struct {
	MaxFlow     uint32
	Source      uint32
	Sink        uint32
	Filename    string
	VertexCount uint32
}

var testGraphs = [...]TestGraph{
	{2, 0, 3, "../../data/maxflow/test-1.txt", 4},
	{8, 0, 2, "../../data/maxflow/test-2.txt", 3},
	{1, 0, 1, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
	//{0, 5000, 10000, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500}, // takes 3889976 ms
	{1087, 1568, 363, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
}

func assertEqual(t *testing.T, expected any, actual any, prefix string) {
	if reflect.DeepEqual(expected, actual) {
		return
	}
	if prefix == "" {
		t.Fatalf("%v != %v", expected, actual)
	} else {
		t.Fatalf("%v - expected %v, got %v", prefix, expected, actual)
	}
}

func TestSyncStatic(t *testing.T) {
	for i := range testGraphs {
		testGraph := &testGraphs[i]
		for ti := 0; ti < 10; ti++ {
			graph.THREADS = rand.Intn(8-1) + 1
			g := LaunchGraphExecution(testGraph.Filename, false, false, testGraph.Source, testGraph.Sink, testGraph.VertexCount)
			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
		}
	}
}

func TestAsyncStatic(t *testing.T) {
	for i := range testGraphs {
		testGraph := &testGraphs[i]
		for ti := 0; ti < 10; ti++ {
			graph.THREADS = rand.Intn(8-1) + 1
			g := LaunchGraphExecution(testGraph.Filename, true, false, testGraph.Source, testGraph.Sink, testGraph.VertexCount)
			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
		}
	}
}

func TestAsyncDynamicIncremental(t *testing.T) {
	for i := range testGraphs {
		testGraph := &testGraphs[i]
		for ti := 0; ti < 3; ti++ {
			graph.THREADS = rand.Intn(8-1) + 1
			g := LaunchGraphExecution(testGraph.Filename, true, true, testGraph.Source, testGraph.Sink, testGraph.VertexCount)
			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
		}
	}
}
