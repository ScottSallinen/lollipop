package main

import (
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

func TestAsyncDynamic(t *testing.T) {
	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		LaunchGraphExecution("../../data/test.txt", true, true, false)
	}
}
func TestAsyncStatic(t *testing.T) {
	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		LaunchGraphExecution("../../data/test.txt", true, false, false)
	}
}
func TestSyncStatic(t *testing.T) {
	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1
		LaunchGraphExecution("../../data/test.txt", false, false, false)
	}
}

type StructureChange struct {
	change graph.VisitType
	srcRaw uint32
	dstRaw uint32
}

func DynamicGraphExecutionFromSC(sc []StructureChange) *graph.Graph {
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

	frame.Init(g, true, true)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	go frame.Run(g, &feederWg, &frameWait)

	for _, v := range sc {
		switch v.change {
		case graph.ADD:
			g.SendAdd(v.srcRaw, v.dstRaw, 0.0)
			info("add ", v.srcRaw, v.dstRaw)
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
	EPSILON = 0.00001
	allowedVariance := EPSILON * float64(100) // ?????

	testFail := false

	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1

		info("TestDynamicCreation ", tcount, " t ", graph.THREADS)

		rawTestGraph := []StructureChange{
			{graph.ADD, 1, 4},
			{graph.ADD, 2, 0},
			{graph.ADD, 2, 1},
			{graph.ADD, 3, 0},
			{graph.ADD, 4, 2},
			{graph.ADD, 4, 3},
			{graph.ADD, 4, 5},
			{graph.ADD, 6, 2},
		}
		shuffleSC(rawTestGraph)

		gDyn := DynamicGraphExecutionFromSC(rawTestGraph)

		gStatic := LaunchGraphExecution("../../data/test.txt", true, false, false)

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
			if !mathutils.FloatEquals(g1values.Residual, g2values.Residual, allowedVariance) {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				t.Error("Residual not equal", g1raw, g1values.Value, g2values.Value, "iteration", tcount)
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

func InjectDeletesRetainFinalStructure(sc []StructureChange, chance float64) []StructureChange {
	availableAdds := make([]StructureChange, len(sc))
	previousAdds := []StructureChange{}
	returnSC := []StructureChange{}

	copy(availableAdds, sc)
	shuffleSC(availableAdds)

	for len(availableAdds) > 0 {
		if len(previousAdds) > 0 && rand.Float64() < chance {
			//chance for del
			shuffleSC(previousAdds)
			idx := len(previousAdds) - 1
			injDel := StructureChange{graph.DEL, previousAdds[idx].srcRaw, previousAdds[idx].dstRaw}
			returnSC = append(returnSC, injDel)
			availableAdds = append(availableAdds, previousAdds[idx])
			previousAdds = previousAdds[:idx]
		} else {
			shuffleSC(availableAdds)
			idx := len(availableAdds) - 1
			returnSC = append(returnSC, availableAdds[idx])
			previousAdds = append(previousAdds, availableAdds[idx])
			availableAdds = availableAdds[:idx]
		}
	}
	log.Println(returnSC)
	return returnSC
}

func TestDynamicWithDelete(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	EPSILON = 0.00001
	allowedVariance := EPSILON * float64(100) // ?????

	testFail := false

	for tcount := 0; tcount < 100; tcount++ {
		graph.THREADS = rand.Intn(8-1) + 1

		rawTestGraph := []StructureChange{
			{graph.ADD, 1, 4},
			{graph.ADD, 2, 0},
			{graph.ADD, 2, 1},
			{graph.ADD, 3, 0},
			{graph.ADD, 4, 2},
			{graph.ADD, 4, 3},
			{graph.ADD, 4, 5},
			{graph.ADD, 6, 2},
		}

		adjustedGraph := InjectDeletesRetainFinalStructure(rawTestGraph, 0.33)

		gDyn := DynamicGraphExecutionFromSC(adjustedGraph)

		gStatic := LaunchGraphExecution("../../data/test.txt", true, false, false)

		CheckGraphStructureEquality(t, gDyn, gStatic)

		a := make([]float64, len(gDyn.Vertices))
		b := make([]float64, len(gStatic.Vertices))

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
			if !mathutils.FloatEquals(g1values.Residual, g2values.Residual, allowedVariance) {
				gStatic.PrintVertexProps("S")
				gDyn.PrintVertexProps("D")
				t.Error("Residual not equal", g1raw, g1values.Value, g2values.Value, "iteration", tcount)
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
