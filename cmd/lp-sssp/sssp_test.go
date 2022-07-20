package main

import (
	"log"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

// Expectation when 1 is src.
func testGraphExpect(g *graph.Graph, t *testing.T) {
	expectations := []float64{4.0, 1.0, 3.0, 3.0, 2.0, 3.0, g.EmptyVal}
	for i := range expectations {
		if g.Vertices[g.VertexMap[uint32(i)]].Value != expectations[i] {
			t.Error(g.VertexMap[uint32(i)], " is ", g.Vertices[g.VertexMap[uint32(i)]].Value, " expected ", expectations[i])
		}
	}
}

func TestAsyncStatic(t *testing.T) {
	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", true, false, false, 1)
		g.PrintVertexProps("")
		testGraphExpect(g, t)
	}
}
func TestSyncStatic(t *testing.T) {
	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", false, false, false, 1)
		g.PrintVertexProps("")
		testGraphExpect(g, t)
	}
}
func TestAsyncDynamic(t *testing.T) {
	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		g := LaunchGraphExecution("../../data/test.txt", true, true, false, 1)
		testGraphExpect(g, t)
		g.PrintVertexProps("")
	}
}

type StructureChange struct {
	change graph.VisitType
	srcRaw uint32
	dstRaw uint32
	weight float64
}

func DynamicGraphExecutionFromSC(sc []StructureChange, rawSrc uint32) *graph.Graph {
	frame := framework.Framework{}
	frame.OnInitVertex = OnInitVertex
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = OnFinish
	frame.OnCheckCorrectness = OnCheckCorrectness
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve

	g := &graph.Graph{}
	g.SourceInit = true
	g.SourceInitVal = 1.0
	g.EmptyVal = math.MaxFloat64
	g.SourceVertex = rawSrc

	frame.Init(g, true, true)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	go frame.Run(g, &feederWg, &frameWait)

	for _, v := range sc {
		switch v.change {
		case graph.ADD:
			g.SendAdd(v.srcRaw, v.dstRaw, v.weight)
			info("add ", v.srcRaw, v.dstRaw, v.weight)
		case graph.DEL:
			g.SendDel(v.srcRaw, v.dstRaw)
			info("del ", v.srcRaw, v.dstRaw)
		}
	}

	for i := 0; i < graph.THREADS; i++ {
		close(g.ThreadStructureQ[i])
	}
	feederWg.Done()
	frameWait.Wait()
	return g
}

func CheckGraphStructureEquality(t *testing.T, g1 *graph.Graph, g2 *graph.Graph) {
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

func shuffleSC(sc []StructureChange) {
	for i := range sc {
		j := rand.Intn(i + 1)
		sc[i], sc[j] = sc[j], sc[i]
	}
}

func TestDynamicCreation(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	allowedVariance := float64(0.001) // ?????

	testFail := false

	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1

		info("TestDynamicCreation ", tcount, " t ", graph.THREADS)

		rawTestGraph := []StructureChange{
			{graph.ADD, 1, 4, 1.0},
			{graph.ADD, 2, 0, 1.0},
			{graph.ADD, 2, 1, 1.0},
			{graph.ADD, 3, 0, 1.0},
			{graph.ADD, 4, 2, 1.0},
			{graph.ADD, 4, 3, 1.0},
			{graph.ADD, 4, 5, 1.0},
			{graph.ADD, 6, 2, 1.0},
		}
		shuffleSC(rawTestGraph)

		gDyn := DynamicGraphExecutionFromSC(rawTestGraph, 1)

		gStatic := LaunchGraphExecution("../../data/test.txt", true, false, false, 1)

		a := make([]float64, len(gDyn.Vertices))
		b := make([]float64, len(gStatic.Vertices))

		CheckGraphStructureEquality(t, gDyn, gStatic)

		for vidx := range gDyn.Vertices {
			g1raw := gDyn.Vertices[vidx].Id
			g2idx := gStatic.VertexMap[g1raw]

			g1values := &gDyn.Vertices[vidx]
			g2values := &gStatic.Vertices[g2idx]

			a[vidx] = g1values.Value
			b[vidx] = g2values.Value

			if !mathutils.FloatEquals(g1values.Value, g2values.Value, allowedVariance) {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				t.Error("Value not equal", g1raw, g1values.Value, g2values.Value, "iteration", tcount)
				testFail = true
			}
		}
		enforce.ENFORCE(!testFail)
	}
}