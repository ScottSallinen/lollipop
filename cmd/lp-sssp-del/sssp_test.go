package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Expectation when 1 is src.
// TODO: Test other sources!
func testGraphExpect(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], t *testing.T) {
	expectations := []float64{3.0, 0.0, 2.0, 2.0, 1.0, 2.0, EmptyVal}
	for i := range expectations {
		internal, _ := g.NodeVertexFromRaw(graph.AsRawType(i))
		prop := g.NodeVertexProperty(internal)
		if prop.Distance != expectations[i] {
			g.PrintVertexProps("")
			t.Fatalf("internalId %v, rawId %v, is %v, expected %v", internal, i, prop.Distance, expectations[i])
		}
	}
}

var testInitMsgs = map[graph.RawType]Mail{graph.AsRawType(1): {}}

var baseOptions = graph.GraphOptions{
	Name:             "../../data/test.txt",
	CheckCorrectness: true,
	DebugLevel:       1,
}

func TestAsyncStatic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = false
		myOpts.Dynamic = true
		alg := new(SSSP)
		alg.SourceVertex = graph.AsRawType(1)
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](alg, myOpts, testInitMsgs, nil)
		testGraphExpect(g, t)
	}
}
func TestSyncStatic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = true
		myOpts.Dynamic = false
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(SSSP), myOpts, testInitMsgs, nil)
		testGraphExpect(g, t)
	}
}
func TestDynamic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = false
		myOpts.Dynamic = true
		source := "1"
		alg, g := Run(myOpts, &source)
		graph.Launch(alg, g)
		testGraphExpect(g, t)
	}
}

func TestDynamicCreation(t *testing.T) {
	rand.NewSource(time.Now().UTC().UnixNano())
	allowedVariance := float64(0.001) // ?????
	source := "1"

	for count := 0; count < 10; count++ {
		THREADS := uint32(rand.Intn(8-1) + 1)

		t.Logf("TestDynamicCreation %v t %v", count, THREADS)
		basicEdge := EdgeProperty{}
		basicEdge.Weight = 1.0

		rawTestGraph := []graph.TopologyEvent[EdgeProperty]{
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(1), DstRaw: graph.AsRawType(4), EdgeProperty: basicEdge},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(0), EdgeProperty: basicEdge},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(1), EdgeProperty: basicEdge},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(3), DstRaw: graph.AsRawType(0), EdgeProperty: basicEdge},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(2), EdgeProperty: basicEdge},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(3), EdgeProperty: basicEdge},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(5), EdgeProperty: basicEdge},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(6), DstRaw: graph.AsRawType(2), EdgeProperty: basicEdge},
		}
		utils.Shuffle(rawTestGraph)

		gDynOptions := graph.GraphOptions{
			// No name; doing special load.
			NumThreads:       THREADS,
			CheckCorrectness: true,
			Dynamic:          true,
		}

		algDyn, gDyn := Run(gDynOptions, &source)
		graph.DynamicGraphExecutionFromTestEvents(algDyn, gDyn, rawTestGraph)

		myOpts := baseOptions
		myOpts.NumThreads = THREADS

		algStatic, gStatic := Run(myOpts, &source)
		graph.Launch(algStatic, gStatic)

		graph.CheckGraphStructureEquality(gDyn, gStatic)

		a := make([]float64, gDyn.NodeVertexCount())
		b := make([]float64, gStatic.NodeVertexCount())

		gDyn.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) {
			g1raw := gDyn.NodeVertexRawID(v)
			g2v, _ := gStatic.NodeVertexFromRaw(g1raw)
			g2prop := gStatic.NodeVertexProperty(g2v)

			a[i] = prop.Distance
			b[i] = g2prop.Distance

			if !utils.FloatEquals(prop.Distance, g2prop.Distance, allowedVariance) {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				t.Fatalf("Distance not equal. raw: %v value: %v vs: %v iteration %v", g1raw, prop.Distance, g2prop.Distance, count)
			}
		})
	}
}
