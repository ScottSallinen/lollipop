package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/kelindar/bitmap"
)

const PrintInfo = false

func TestSyncStatic(t *testing.T) {
	for ti := 0; ti < 10; ti++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", false, false)
		if PrintInfo {
			g.PrintVertexProperty("Sync colours: ")
		}
	}
}

func TestAsyncStatic(t *testing.T) {
	for ti := 0; ti < 10; ti++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", true, false)
		if PrintInfo {
			g.PrintVertexProperty("Async colours: ")
		}
	}
}

func TestAsyncDynamic(t *testing.T) {
	for ti := 0; ti < 10; ti++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", true, true)
		if PrintInfo {
			g.PrintVertexProperty("Dynamic colours: ")
		}
	}
}

func TestAsyncDynamicWithDelete(t *testing.T) {
	for ti := 0; ti < 10; ti++ {
		graph.THREADS = rand.Intn(8-1) + 1
		rawStructureChanges := []graph.StructureChange[EdgeProperty]{
			{Type: graph.ADD, SrcRaw: 1, DstRaw: 4, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 0, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 1, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 3, DstRaw: 0, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 2, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 3, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 5, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 6, DstRaw: 2, EdgeProperty: EdgeProperty{}},
		}

		adjustedStructureChanges := framework.InjectDeletesRetainFinalStructure(rawStructureChanges, 0.33)

		g := DynamicGraphExecutionFromSC(adjustedStructureChanges)

		maxColour, nColours := ComputeGraphColouringStat(g)
		if PrintInfo {
			info(fmt.Sprintf("maxColour=%v, nColours=%v", maxColour, nColours))
		}
	}
}

func fillBitmap(toFill []uint32) bitmap.Bitmap {
	var bm bitmap.Bitmap
	for _, j := range toFill {
		bm.Set(j)
	}
	return bm
}

func TestFindFirstUnused(t *testing.T) {
	nbrsTests := [][]uint32{
		{},
		{0},
		{1},
		{0, 1},
		{1, 0},
		{0, 2},
		{0, 1, 2, 3},
		{1, 2, 3},
		{2, 4, 1, 0},
		{12, 0, 2, 2, 2, 3, 0, 1},
		{7, 4, 0, 2, 2, 5, 3, 0, 1, 5, 8},
	}
	nbrsTestsAns := []uint32{
		0,
		1,
		0,
		2,
		2,
		1,
		4,
		0,
		3,
		4,
		6,
	}

	for test := range nbrsTests {
		assertEqual(t, nbrsTestsAns[test], findFirstUnused(fillBitmap(nbrsTests[test])), fmt.Sprintf("%d:", test))
	}
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

func DynamicGraphExecutionFromSC(sc []graph.StructureChange[EdgeProperty]) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
	frame := framework.Framework[VertexProperty, EdgeProperty, MessageValue]{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.OnEdgeAddRev = OnEdgeAddRev
	frame.OnEdgeDelRev = OnEdgeDelRev
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve
	frame.IsMsgEmpty = IsMsgEmpty

	g := &graph.Graph[VertexProperty, EdgeProperty, MessageValue]{}
	g.SourceInit = false
	g.InitVal = 0
	g.EmptyVal = EMPTYVAL

	g.Undirected = true
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
		case graph.DEL:
			g.SendDel(v.SrcRaw, v.DstRaw)
		}
	}

	for i := 0; i < graph.THREADS; i++ {
		close(g.ThreadStructureQ[i])
	}
	feederWg.Done()
	frameWait.Wait()
	return g
}
