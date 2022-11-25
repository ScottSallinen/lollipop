package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

func PrintVertexProps(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], prefix string) {
	top := prefix
	sum := 0.0
	for vidx := range g.Vertices {
		Value := 0.0
		Scratch := 0.0
		for path := range g.Vertices[vidx].Property.Value {
			Value = path.Weight
			break
		}
		for path := range g.Vertices[vidx].Property.Scratch {
			Scratch = path.Weight
			break
		}
		top += fmt.Sprintf("%d:[%.3f,%.3f] ", g.Vertices[vidx].Id, Value, Scratch)
		sum += Value
	}
	info(top + " : " + fmt.Sprintf("%.3f", sum))
}

// Expectation when 1 is src.
// TODO: Test other sources!
func testGraphExpect(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], t *testing.T) {
	//allowedVariance := float64(0.001) // ?????

	expectation0 := make(PathMap)
	expectation0[PathProperty{Weight: 3.5, Timestamp: 27}] = void_member
	expectation0[PathProperty{Weight: 4, Timestamp: 26}] = void_member
	expectation1 := make(PathMap)
	expectation1[PathProperty{Weight: 1, Timestamp: 0}] = void_member
	expectation2 := make(PathMap)
	expectation2[PathProperty{Weight: 3, Timestamp: 23}] = void_member
	expectation3 := make(PathMap)
	expectation3[PathProperty{Weight: 3, Timestamp: 24}] = void_member
	expectation4 := make(PathMap)
	expectation4[PathProperty{Weight: 2, Timestamp: 21}] = void_member
	expectation5 := make(PathMap)
	expectation5[PathProperty{Weight: 3, Timestamp: 25}] = void_member
	expectation6 := make(PathMap)
	expectations := make(map[uint32]MessageValue)
	expectations[0] = MessageValue(expectation0)
	expectations[1] = MessageValue(expectation1)
	expectations[2] = MessageValue(expectation2)
	expectations[3] = MessageValue(expectation3)
	expectations[4] = MessageValue(expectation4)
	expectations[5] = MessageValue(expectation5)
	expectations[6] = MessageValue(expectation6)

	for i := range expectations {
		for item := range expectations[i] {
			_, exists := g.Vertices[g.VertexMap[uint32(i)]].Property.Value[item]
			if !exists {
				t.Error(g.VertexMap[uint32(i)], " is ", g.Vertices[g.VertexMap[uint32(i)]].Property.Value[item], " expected ", 1)
			}
		}
	}
}

func TestAsyncStatic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test-weight-timestamp.txt", true, false, false, false, 1, false)
		PrintVertexProps(g, "")
		testGraphExpect(g, t)
	}
}
func TestSyncStatic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test-weight-timestamp.txt", false, false, false, false, 1, false)
		PrintVertexProps(g, "")
		testGraphExpect(g, t)
	}
}
func TestAsyncDynamic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test-weight-timestamp.txt", true, true, false, false, 1, false)
		testGraphExpect(g, t)
		PrintVertexProps(g, "")
	}
}

func DynamicGraphExecutionFromSC(sc []graph.StructureChange[EdgeProperty], rawSrc uint32) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
	frame := framework.Framework[VertexProperty, EdgeProperty, MessageValue]{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve

	empty_set := make(PathMap)
	empty_set[PathProperty{Weight: EMPTYVAL, Timestamp: EMPTYVAL}] = void_member
	initial_set := make(PathMap)
	initial_set[PathProperty{Weight: 1.0, Timestamp: 0}] = void_member
	initial_map := make(map[uint32]MessageValue)
	initial_map[1] = MessageValue(initial_set)

	g := &graph.Graph[VertexProperty, EdgeProperty, MessageValue]{}
	g.Options = graph.GraphOptions[MessageValue]{
		SourceInit:   true,
		InitMessages:  initial_map,
		EmptyVal:      MessageValue(empty_set),
	}

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
			info("add ", v.SrcRaw, v.DstRaw)
		case graph.DEL:
			g.SendDel(v.SrcRaw, v.DstRaw)
			info("del ", v.SrcRaw, v.DstRaw)
		}
	}

	for i := 0; i < graph.THREADS; i++ {
		close(g.ThreadStructureQ[i])
	}
	feederWg.Done()
	frameWait.Wait()
	return g
}

func CheckGraphStructureEquality(t *testing.T, g1 *graph.Graph[VertexProperty, EdgeProperty, MessageValue], g2 *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) {
	if len(g1.Vertices) != len(g2.Vertices) {
		t.Error("vertex count mismatch", len(g1.Vertices), len(g2.Vertices))
	}

	for vidx := range g1.Vertices {
		g1raw := g1.Vertices[vidx].Id
		g2idx := g2.VertexMap[g1raw]

		//g1values := &g1.Vertices[vidx].Properties
		//g2values := &g2.Vertices[g2idx].Properties

		if len(g1.Vertices[vidx].OutEdges) != len(g2.Vertices[g2idx].OutEdges) {
			log.Println("g1:")
			g1.PrintStructure()
			log.Println("g2:")
			g2.PrintStructure()
			t.Error("edge count mismatch", len(g1.Vertices[vidx].OutEdges), len(g2.Vertices[g2idx].OutEdges))
		}
	}
}

func TestDynamicCreation(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	//allowedVariance := float64(0.001) // ?????

	testFail := false

	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1

		info("TestDynamicCreation ", tcount, " t ", graph.THREADS)

		rawTestGraph := []graph.StructureChange[EdgeProperty]{
			{Type: graph.ADD, SrcRaw: 1, DstRaw: 4, EdgeProperty: EdgeProperty{Weight: 1, Timestamp:21}},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 1, EdgeProperty: EdgeProperty{Weight: 1, Timestamp:22}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 2, EdgeProperty: EdgeProperty{Weight: 1, Timestamp:23}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 3, EdgeProperty: EdgeProperty{Weight: 1, Timestamp:24}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 5, EdgeProperty: EdgeProperty{Weight: 1, Timestamp:25}},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 0, EdgeProperty: EdgeProperty{Weight: 1, Timestamp:26}},
			{Type: graph.ADD, SrcRaw: 3, DstRaw: 0, EdgeProperty: EdgeProperty{Weight: 0.5, Timestamp:27}},
			{Type: graph.ADD, SrcRaw: 6, DstRaw: 2, EdgeProperty: EdgeProperty{Weight: 1, Timestamp:28}},
		}
		framework.ShuffleSC(rawTestGraph)

		gDyn := DynamicGraphExecutionFromSC(rawTestGraph, 1)

		gStatic := LaunchGraphExecution("../../data/test-weight-timestamp.txt", true, false, false, false, 1, false)

		//a := make([]float64, len(gDyn.Vertices))
		//b := make([]float64, len(gStatic.Vertices))

		CheckGraphStructureEquality(t, gDyn, gStatic)

		for vidx := range gDyn.Vertices {
			g1raw := gDyn.Vertices[vidx].Id
			g2idx := gStatic.VertexMap[g1raw]

			g1values := &gDyn.Vertices[vidx]
			g2values := &gStatic.Vertices[g2idx]

			//a[vidx] = g1values.Property.Value
			//b[vidx] = g2values.Property.Value

			if IsAlbeUpdate(g1values.Property.Value, g2values.Property.Value) || IsAlbeUpdate(g2values.Property.Value, g1values.Property.Value) {
				PrintVertexProps(gStatic, "S ")
				PrintVertexProps(gDyn, "D ")
				t.Error("Value not equal", g1raw, g1values.Property.Value, g2values.Property.Value, "iteration", tcount)
				testFail = true
			}
		}
		enforce.ENFORCE(!testFail)
	}
}
