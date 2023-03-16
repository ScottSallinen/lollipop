package main

import (
	"bufio"
	"fmt"
	"github.com/ScottSallinen/lollipop/framework"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

type TestGraph struct {
	MaxFlow     int64
	Source      uint32
	Sink        uint32
	Filename    string
	VertexCount uint32
}

type DynamicTestGraph struct {
	MaxFlow          int64
	Source           uint32
	Sink             uint32
	VertexCount      uint32
	StructureChanges []graph.StructureChange[EdgeProp]
}

var (
	testGraphs = [...]TestGraph{
		{2, 0, 3, "../../data/maxflow/test-1.txt", 4},
		{8, 0, 2, "../../data/maxflow/test-2.txt", 3},
		{23, 1367, 2361, "../../data/maxflow/comment-graph-sample-merged-shuffled-5percent.txt", 6260},
		{1, 16239, 7662, "../../data/maxflow/comment-graph-sample-merged-shuffled-5percent.txt", 6260},
		{1, 0, 1, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
		{0, 5000, 10000, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
		{1, 6786, 7895, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
		{0, 15358, 9845, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
		{162, 6272, 4356, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
		{8, 16632, 9492, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
		{1087, 1568, 363, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
		{21, 3709, 7135, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
		{137, 11665, 3721, "../../data/maxflow/comment-graph-sample-merged-shuffled.txt", 17500},
	}
	dynamicTestGraphs = [...]DynamicTestGraph{
		// Test INCREASING messages in cycles
		{1, 0, 1, 5, []graph.StructureChange[EdgeProp]{
			{graph.ADD, 0, 1, EdgeProp{1, 0}},
			{graph.ADD, 2, 3, EdgeProp{1, 0}},
			{graph.ADD, 3, 4, EdgeProp{1, 0}},
			{graph.ADD, 4, 2, EdgeProp{1, 0}},
		}},
		// Check if upstream vertices are also updated when a vertex receives a DECREASING event
		{1, 0, 2, 4, []graph.StructureChange[EdgeProp]{
			{graph.ADD, 0, 1, EdgeProp{2, 0}},
			{graph.ADD, 1, 2, EdgeProp{1, 0}},
			{graph.ADD, 0, 3, EdgeProp{1, 0}},
			{graph.ADD, 3, 1, EdgeProp{1, 0}},
			{graph.DEL, 0, 1, EdgeProp{}},
		}},
	}
)

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

//func TestSyncStatic(t *testing.T) {
//	for i := range testGraphs {
//		testGraph := &testGraphs[i]
//		for ti := 0; ti < 10; ti++ {
//			graph.THREADS = rand.Intn(8-1) + 1
//			g := LaunchGraphExecution(testGraph.Filename, false, false, testGraph.Source, testGraph.Sink, testGraph.VertexCount)
//			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
//			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
//		}
//	}
//}

func TestAsyncStatic(t *testing.T) {
	for i := range testGraphs {
		testGraph := &testGraphs[i]
		for ti := 0; ti < 10; ti++ {
			graph.THREADS = rand.Intn(8-1) + 1
			g := LaunchGraphExecution(testGraph.Filename, true, false,
				testGraph.Source, testGraph.Sink, testGraph.VertexCount, 500*time.Millisecond, 0, 0, false)
			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
		}
	}
}

func TestDynamicGraphs(t *testing.T) {
	graph.DEBUG = true
	graph.TARGETRATE = 1 // Set this to prevent edge change events being merged
	graph.THREADS = 1
	for i := range dynamicTestGraphs {
		testGraph := &dynamicTestGraphs[i]
		g := dynamicGraphExecutionFromSC(testGraph.StructureChanges, testGraph.Source, testGraph.Sink)
		maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
		assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("DynamicGraph %v Max flow", i))
	}
	graph.DEBUG = false
	graph.TARGETRATE = 0
}

func TestAsyncDynamicIncremental(t *testing.T) {
	for i := range testGraphs {
		testGraph := &testGraphs[i]
		for ti := 0; ti < 3; ti++ {
			graph.THREADS = rand.Intn(8-1) + 1
			g := LaunchGraphExecution(testGraph.Filename, true, true, testGraph.Source, testGraph.Sink, 0, 500*time.Millisecond, 0, 0, false)
			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
		}
	}
}

func TestAsyncDynamicIncrementalShuffled(t *testing.T) {
	for i := range testGraphs {
		testGraph := &testGraphs[i]
		for ti := 0; ti < 2; ti++ {
			graph.THREADS = rand.Intn(8-1) + 1
			sc := loadAllStructureChanges(testGraph.Filename)
			framework.ShuffleSC(sc)
			g := dynamicGraphExecutionFromSC(sc, testGraph.Source, testGraph.Sink)
			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
		}
	}
}

func TestAsyncDynamicWithDelete(t *testing.T) {
	for i := range testGraphs {
		testGraph := &testGraphs[i]
		for ti := 0; ti < 1; ti++ {
			graph.THREADS = 10
			rawStructureChanges := loadAllStructureChanges(testGraph.Filename)
			adjustedStructureChanges := framework.InjectDeletesRetainFinalStructure(rawStructureChanges, 0.33)
			info(fmt.Sprintf("Number of structure changes with deletes: %v", len(adjustedStructureChanges)))

			g := dynamicGraphExecutionFromSC(adjustedStructureChanges, testGraph.Source, testGraph.Sink)
			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
		}
	}
}

func loadAllStructureChanges(path string) []graph.StructureChange[EdgeProp] {
	file, err := os.Open(path)
	enforce.ENFORCE(err)
	defer enforce.Close(file)

	var watch mathutils.Watch
	var result []graph.StructureChange[EdgeProp]

	watch.Start()
	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
		lineText := scanner.Text()
		if strings.HasPrefix(lineText, "#") {
			continue
		}
		rawEdge := EdgeParser(lineText)
		result = append(result, graph.StructureChange[EdgeProp]{
			Type:         graph.ADD,
			SrcRaw:       rawEdge.SrcRaw,
			DstRaw:       rawEdge.DstRaw,
			EdgeProperty: rawEdge.EdgeProperty,
		})
	}

	info(fmt.Sprintf("Loaded all %v structure changes in %v", lines, watch.Elapsed()))

	return result
}

func dynamicGraphExecutionFromSC(sc []graph.StructureChange[EdgeProp], source, sink uint32) *Graph {
	globalRelabelExit := make(chan bool, 0)
	frame, g := GetFrameworkAndGraph(source, sink, 0, &globalRelabelExit, 0, 0)

	frame.Init(g, true, true)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	var grWg = StartPeriodicGlobalReset(frame, g, 500*time.Millisecond, 500*time.Millisecond, true, &globalRelabelExit)

	go frame.Run(g, &feederWg, &frameWait)

	for _, v := range sc {
		switch v.Type {
		case graph.ADD:
			g.SendAdd(v.SrcRaw, v.DstRaw, v.EdgeProperty)
			//g.SendAdd(v.DstRaw, v.SrcRaw, v.EdgeProperty) // TODO: try undirected graphs?
		case graph.DEL:
			g.SendDel(v.SrcRaw, v.DstRaw)
			//g.SendDel(v.DstRaw, v.SrcRaw) // TODO: try undirected graphs?
		}
	}

	for i := 0; i < graph.THREADS; i++ {
		close(g.ThreadStructureQ[i])
	}
	feederWg.Done()
	frameWait.Wait()
	grWg.Wait()
	return g
}
