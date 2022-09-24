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
	"github.com/ScottSallinen/lollipop/mathutils"
)

func PrintVertexProps(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], prefix string) {
	top := prefix
	sum := uint32(0)
	for vidx := range g.Vertices {
		top += fmt.Sprintf("%d:[%d,%d] ", g.Vertices[vidx].Id, g.Vertices[vidx].Property.Value, g.Vertices[vidx].Property.Scratch)
		sum += g.Vertices[vidx].Property.Value
	}
	info(top + " : " + fmt.Sprintf("%d", sum))
}

// Expectation when 1 is src.
// TODO: Test other sources!
func testGraphExpect(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], t *testing.T) {
	expectations := []uint32{0, 1, 1, 0, 1, 1, 1, 0, 0, 0}
	for i := range expectations {
		if g.Vertices[g.VertexMap[uint32(i)]].Property.Value != expectations[i] {
			t.Error(g.VertexMap[uint32(i)], " is ", g.Vertices[g.VertexMap[uint32(i)]].Property.Value, " expected ", expectations[i])
		}
	}
}

func TestAsyncStatic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test_multiple_components.txt", true, false, false, false)
		PrintVertexProps(g, "")
		testGraphExpect(g, t)
	}
}
func TestSyncStatic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test_multiple_components.txt", false, false, false, false)
		PrintVertexProps(g, "")
		testGraphExpect(g, t)
	}
}
func TestAsyncDynamic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test_multiple_components.txt", true, true, false, false)
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

	g := &graph.Graph[VertexProperty, EdgeProperty, MessageValue]{}
	g.SourceInit = false
	g.InitVal = EMPTYVAL
	g.EmptyVal = EMPTYVAL

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
			info("add ", v.SrcRaw, v.DstRaw)
		case graph.DEL:
			g.SendDel(v.SrcRaw, v.DstRaw)
			g.SendDel(v.DstRaw, v.SrcRaw)
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

func shuffleSC(sc []graph.StructureChange[EdgeProperty]) {
	for i := range sc {
		j := rand.Intn(i + 1)
		sc[i], sc[j] = sc[j], sc[i]
	}
}

func TestDynamicCreation(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	allowedVariance := float64(0.001) // ?????

	testFail := false

	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1

		info("TestDynamicCreation ", tcount, " t ", graph.THREADS)

		rawTestGraph := []graph.StructureChange[EdgeProperty]{
			{Type: graph.ADD, SrcRaw: 1, DstRaw: 4},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 0},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 1},
			{Type: graph.ADD, SrcRaw: 3, DstRaw: 0},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 2},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 3},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 5},
			{Type: graph.ADD, SrcRaw: 6, DstRaw: 2},
		}
		shuffleSC(rawTestGraph)

		gDyn := DynamicGraphExecutionFromSC(rawTestGraph)

		gStatic := LaunchGraphExecution("../../data/test_multiple_components.txt", true, false, false, false)

		a := make([]uint32, len(gDyn.Vertices))
		b := make([]uint32, len(gStatic.Vertices))

		CheckGraphStructureEquality(t, gDyn, gStatic)

		for vidx := range gDyn.Vertices {
			g1raw := gDyn.Vertices[vidx].Id
			g2idx := gStatic.VertexMap[g1raw]

			g1values := &gDyn.Vertices[vidx]
			g2values := &gStatic.Vertices[g2idx]

			a[vidx] = g1values.Property.Value
			b[vidx] = g2values.Property.Value

			if !mathutils.FloatEquals(float64(g1values.Property.Value), float64(g2values.Property.Value), allowedVariance) {
				PrintVertexProps(gStatic, "S ")
				PrintVertexProps(gDyn, "D ")
				t.Error("Value not equal", g1raw, g1values.Property.Value, g2values.Property.Value, "iteration", tcount)
				testFail = true
			}
		}
		enforce.ENFORCE(!testFail)
	}
}
