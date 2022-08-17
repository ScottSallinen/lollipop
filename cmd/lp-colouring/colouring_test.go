package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/ScottSallinen/lollipop/cmd/common"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

func TestSyncStatic(t *testing.T) {
	for ti := 0; ti < 100; ti++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", false, false)
		g.PrintVertexProperty("Sync colours: ")
	}
}

func TestAsyncStatic(t *testing.T) {
	for ti := 0; ti < 100; ti++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", true, false)
		g.PrintVertexProperty("Async colours: ")
	}
}

func TestAsyncDynamic(t *testing.T) {
	for ti := 0; ti < 10000; ti++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", true, true)
		g.PrintVertexProperty("Dynamic colours: ")
	}
}

func TestAsyncDynamicWithDelete(t *testing.T) {
	for ti := 0; ti < 100; ti++ {
		rawStructureChanges := []graph.StructureChange[EdgeProperty]{
			{graph.ADD, 1, 4, EdgeProperty{}},
			{graph.ADD, 2, 0, EdgeProperty{}},
			{graph.ADD, 2, 1, EdgeProperty{}},
			{graph.ADD, 3, 0, EdgeProperty{}},
			{graph.ADD, 4, 2, EdgeProperty{}},
			{graph.ADD, 4, 3, EdgeProperty{}},
			{graph.ADD, 4, 5, EdgeProperty{}},
			{graph.ADD, 6, 2, EdgeProperty{}},
		}

		adjustedStructureChanges := common.InjectDeletesRetainFinalStructure(rawStructureChanges, 0.33)

		g := DynamicGraphExecutionFromSCUndirected(adjustedStructureChanges)

		maxColour, nColours := ComputeGraphColouringStat(g)
		info(fmt.Sprintf("maxColour=%v, nColours=%v", maxColour, nColours))
	}
}

func TestFindFirstUnused(t *testing.T) {
	assertEqual(t, uint32(0), findFirstUnused([]uint32{}), "[]")
	assertEqual(t, uint32(1), findFirstUnused([]uint32{0}), "[0]")
	assertEqual(t, uint32(0), findFirstUnused([]uint32{1}), "[1]")
	assertEqual(t, uint32(2), findFirstUnused([]uint32{0, 1}), "[0, 1]")
	assertEqual(t, uint32(1), findFirstUnused([]uint32{0, 2}), "[0, 2]")
}

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

func DynamicGraphExecutionFromSCUndirected(sc []graph.StructureChange[EdgeProperty]) *graph.Graph[VertexProperty, EdgeProperty] {
	frame := framework.Framework[VertexProperty, EdgeProperty]{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve

	g := &graph.Graph[VertexProperty, EdgeProperty]{}

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
			g.SendAdd(v.DstRaw, v.SrcRaw, v.EdgeProperty)
		case graph.DEL:
			g.SendDel(v.SrcRaw, v.DstRaw)
			g.SendDel(v.DstRaw, v.SrcRaw)
		}
	}

	for i := 0; i < graph.THREADS; i++ {
		close(g.ThreadStructureQ[i])
	}
	feederWg.Done()
	frameWait.Wait()
	return g
}
