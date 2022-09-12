package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"math/rand"
	"reflect"
	"testing"
)

type TestGraph struct {
	MaxFlow  uint32
	Source   uint32
	Sink     uint32
	Filename string
}

var testGraphs = [...]TestGraph{{2, 0, 3, "../../data/maxflow/test-1.txt"}}

func assertEqual(t *testing.T, expected any, actual any, prefix string) {
	if reflect.DeepEqual(expected, actual) {
		return
	}
	if prefix == "" {
		t.Fatalf("%v != %v", expected, actual)
	} else {
		t.Fatalf("%v - %v != %v", prefix, expected, actual)
	}
}

func TestSyncStatic(t *testing.T) {
	for i := range testGraphs {
		testGraph := &testGraphs[i]
		for ti := 0; ti < 10; ti++ {
			graph.THREADS = rand.Intn(8-1) + 1
			g := LaunchGraphExecution(testGraph.Filename, false, false, testGraph.Source, testGraph.Sink)
			g.PrintVertexProperty("Sync maxflow: ")
			// TODO: extract the computed max flow value
			assertEqual(t, testGraph.MaxFlow, 0, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
		}
	}
}
