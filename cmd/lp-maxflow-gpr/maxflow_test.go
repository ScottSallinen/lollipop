package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
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

//func TestAsyncDynamicIncrementalShuffled(t *testing.T) {
//	for i := range testGraphs {
//		testGraph := &testGraphs[i]
//		for ti := 0; ti < 2; ti++ {
//			graph.THREADS = rand.Intn(8-1) + 1
//			sc := loadAllStructureChanges(testGraph.Filename)
//			framework.ShuffleSC(sc)
//
//			g := dynamicGraphExecutionFromSC(sc, testGraph.Source, testGraph.Sink, testGraph.VertexCount)
//			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
//			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
//		}
//	}
//}

//func TestAsyncDynamicWithDelete(t *testing.T) {
//	for i := range testGraphs {
//		testGraph := &testGraphs[i]
//		for ti := 0; ti < 1; ti++ {
//			graph.THREADS = 10
//			rawStructureChanges := loadAllStructureChanges(testGraph.Filename)
//			adjustedStructureChanges := framework.InjectDeletesRetainFinalStructure(rawStructureChanges, 0.01)
//			info(fmt.Sprintf("Number of structure changes with deletes: %v", len(adjustedStructureChanges)))
//
//			g := dynamicGraphExecutionFromSC(adjustedStructureChanges, testGraph.Source, testGraph.Sink, testGraph.VertexCount)
//			maxFlow := g.Vertices[g.VertexMap[testGraph.Sink]].Property.Excess
//			assertEqual(t, testGraph.MaxFlow, maxFlow, fmt.Sprintf("Graph %s Max flow", testGraph.Filename))
//		}
//	}
//}

func loadAllStructureChanges(path string) []graph.StructureChange[EdgeProperty] {
	file, err := os.Open(path)
	enforce.ENFORCE(err)
	defer enforce.Close(file)

	var watch mathutils.Watch
	var result []graph.StructureChange[EdgeProperty]

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
		result = append(result, graph.StructureChange[EdgeProperty]{
			Type:         graph.ADD,
			SrcRaw:       rawEdge.SrcRaw,
			DstRaw:       rawEdge.DstRaw,
			EdgeProperty: rawEdge.EdgeProperty,
		})
	}

	info(fmt.Sprintf("Loaded all %v structure changes in %v", lines, watch.Elapsed()))

	return result
}

func dynamicGraphExecutionFromSC(sc []graph.StructureChange[EdgeProperty], source, sink, sourceHeight uint32) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
	frame, g := GetFrameworkAndGraph(source, sink, sourceHeight)

	frame.Init(g, true, true)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

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
	return g
}
