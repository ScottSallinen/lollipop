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
	sum := 0.0
	for vidx := range g.Vertices {
		top += fmt.Sprintf("%d:[%.3f,%.3f,%.3f] ", g.Vertices[vidx].Id, g.Vertices[vidx].Property.Value, g.Vertices[vidx].Property.Residual, g.Vertices[vidx].Property.Scratch)
		sum += g.Vertices[vidx].Property.Value
	}
	info(top + " : " + fmt.Sprintf("%.3f", sum))
}

func TestAsyncDynamic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		LaunchGraphExecution("../../data/test.txt", true, true, false, false, false)
	}
}
func TestAsyncStatic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		LaunchGraphExecution("../../data/test.txt", true, false, false, false, false)
	}
}
func TestSyncStatic(t *testing.T) {
	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		LaunchGraphExecution("../../data/test.txt", false, false, false, false, false)
	}
}

func DynamicGraphExecutionFromSC(sc []graph.StructureChange[EdgeProperty], undirected bool) *graph.Graph[VertexProperty, EdgeProperty, MessageValue] {
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
	g.EmptyVal = EMPTYVAL
	g.InitVal = INITMASS

	frame.Init(g, true, true)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	go frame.Run(g, &feederWg, &frameWait)

	for _, v := range sc {
		switch v.Type {
		case graph.ADD:
			g.SendAdd(v.SrcRaw, v.DstRaw, EdgeProperty{})
			if undirected {
				g.SendAdd(v.DstRaw, v.SrcRaw, EdgeProperty{})
			}
			info("add ", v.SrcRaw, v.DstRaw)
		case graph.DEL:
			g.SendDel(v.SrcRaw, v.DstRaw)
			if undirected {
				g.SendDel(v.DstRaw, v.SrcRaw)
			}
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

func TestDynamicCreationDirected(t *testing.T) {
	DynamicCreation(false, t)
}

func TestDynamicCreationUnDirected(t *testing.T) {
	DynamicCreation(true, t)
}

func DynamicCreation(undirected bool, t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	EPSILON = 0.00001
	allowedVariance := EPSILON * float64(100) // ?????

	testFail := false

	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1

		info("TestDynamicCreation ", tcount, " t ", graph.THREADS)

		rawTestGraph := []graph.StructureChange[EdgeProperty]{
			{Type: graph.ADD, SrcRaw: 1, DstRaw: 4, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 0, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 1, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 3, DstRaw: 0, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 2, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 3, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 5, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 6, DstRaw: 2, EdgeProperty: EdgeProperty{}},
		}
		framework.ShuffleSC(rawTestGraph)

		gDyn := DynamicGraphExecutionFromSC(rawTestGraph, undirected)

		gStatic := LaunchGraphExecution("../../data/test.txt", true, false, false, true, undirected)

		a := make([]float64, len(gDyn.Vertices))
		b := make([]float64, len(gStatic.Vertices))

		CheckGraphStructureEquality(t, gDyn, gStatic)

		for vidx := range gDyn.Vertices {
			g1raw := gDyn.Vertices[vidx].Id
			g2idx := gStatic.VertexMap[g1raw]

			g1values := &gDyn.Vertices[vidx]
			g2values := &gStatic.Vertices[g2idx]

			a[vidx] = g1values.Property.Value
			b[vidx] = g2values.Property.Value

			if !mathutils.FloatEquals(g1values.Property.Value, g2values.Property.Value, allowedVariance) {
				PrintVertexProps(gStatic, "S ")
				PrintVertexProps(gDyn, "D ")
				t.Error("Value not equal", g1raw, g1values.Property.Value, g2values.Property.Value, "iteration", tcount)
				testFail = true
			}
			if !mathutils.FloatEquals(g1values.Property.Residual, g2values.Property.Residual, allowedVariance) {
				PrintVertexProps(gStatic, "S ")
				PrintVertexProps(gDyn, "D ")
				t.Error("Residual not equal", g1raw, g1values.Property.Value, g2values.Property.Value, "iteration", tcount)
				testFail = true
			}
		}

		largestDiff := graph.ResultCompare(a, b)

		if largestDiff > allowedVariance*100 { // is percent
			t.Error("largestDiff", largestDiff, "iteration", tcount)
			testFail = true
		}
		enforce.ENFORCE(!testFail)
	}
}

func TestDynamicWithDeleteDirected(t *testing.T) {
	DynamicWithDelete(false, t)
}

func TestDynamicWithDeleteUnDirected(t *testing.T) {
	DynamicWithDelete(true, t)
}

func DynamicWithDelete(undirected bool, t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	EPSILON = 0.00001
	allowedVariance := EPSILON * float64(100) // ?????

	testFail := false

	for tcount := 0; tcount < 10; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1

		rawTestGraph := []graph.StructureChange[EdgeProperty]{
			{Type: graph.ADD, SrcRaw: 1, DstRaw: 4, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 0, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 2, DstRaw: 1, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 3, DstRaw: 0, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 2, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 3, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 4, DstRaw: 5, EdgeProperty: EdgeProperty{}},
			{Type: graph.ADD, SrcRaw: 6, DstRaw: 2, EdgeProperty: EdgeProperty{}},
		}

		adjustedGraph := framework.InjectDeletesRetainFinalStructure(rawTestGraph, 0.33)

		gDyn := DynamicGraphExecutionFromSC(adjustedGraph, undirected)

		gStatic := LaunchGraphExecution("../../data/test.txt", true, false, false, true, undirected)

		CheckGraphStructureEquality(t, gDyn, gStatic)

		a := make([]float64, len(gDyn.Vertices))
		b := make([]float64, len(gStatic.Vertices))

		for vidx := range gDyn.Vertices {
			g1raw := gDyn.Vertices[vidx].Id
			g2idx := gStatic.VertexMap[g1raw]

			g1values := &gDyn.Vertices[vidx]
			g2values := &gStatic.Vertices[g2idx]

			a[vidx] = g1values.Property.Value
			b[vidx] = g2values.Property.Value
			if !mathutils.FloatEquals(g1values.Property.Value, g2values.Property.Value, allowedVariance) {
				PrintVertexProps(gStatic, "S ")
				PrintVertexProps(gDyn, "D ")
				t.Error("Value not equal", g1raw, g1values.Property.Value, g2values.Property.Value, "iteration", tcount)
				testFail = true
			}
			if !mathutils.FloatEquals(g1values.Property.Residual, g2values.Property.Residual, allowedVariance) {
				PrintVertexProps(gStatic, "S ")
				PrintVertexProps(gDyn, "D ")
				t.Error("Residual not equal", g1raw, g1values.Property.Value, g2values.Property.Value, "iteration", tcount)
				testFail = true
			}
		}

		largestDiff := graph.ResultCompare(a, b)

		if largestDiff > allowedVariance*100 { // is percent
			t.Error("largestDiff", largestDiff)
			testFail = true
		}
		enforce.ENFORCE(!testFail)
	}
}
