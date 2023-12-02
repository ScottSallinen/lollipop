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
	expectations := []float64{4.0, 1.0, 3.0, 3.0, 2.0, 3.0, EMPTY_VAL}
	for i := range expectations {
		internal, _ := g.NodeVertexFromRaw(graph.AsRawType(i))
		prop := g.NodeVertexProperty(internal)
		if prop.Value != expectations[i] {
			g.PrintVertexProps("")
			t.Fatalf("internalId %v, rawId %v, is %v, expected %v", internal, i, prop.Value, expectations[i])
		}
	}
}

var testInitMsgs = map[graph.RawType]Mail{graph.AsRawType(1): 1.0}

var baseOptions = graph.GraphOptions{
	Name:             "../../data/test.txt",
	CheckCorrectness: true,
}

func TestAsyncStatic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = false
		myOpts.Dynamic = false
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(SSSP), myOpts, testInitMsgs, nil)
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
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(SSSP), myOpts, testInitMsgs, nil)
		testGraphExpect(g, t)
	}
}

func TestDynamicCreation(t *testing.T) {
	rand.NewSource(time.Now().UTC().UnixNano())
	allowedVariance := float64(0.001) // ?????

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

		gDyn := &graph.Graph[VertexProperty, EdgeProperty, Mail, Note]{}
		gDyn.Options = graph.GraphOptions{
			// No name; doing special load.
			NumThreads:       THREADS,
			CheckCorrectness: true,
			Dynamic:          true,
		}
		gDyn.InitMails = testInitMsgs

		graph.DynamicGraphExecutionFromTestEvents(new(SSSP), gDyn, rawTestGraph)

		myOpts := baseOptions
		myOpts.NumThreads = THREADS

		gStatic := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(SSSP), myOpts, testInitMsgs, nil)

		graph.CheckGraphStructureEquality(gDyn, gStatic)

		a := make([]float64, gDyn.NodeVertexCount())
		b := make([]float64, gStatic.NodeVertexCount())

		gDyn.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) {
			g1raw := gDyn.NodeVertexRawID(v)
			g2v, _ := gStatic.NodeVertexFromRaw(g1raw)
			g2prop := gStatic.NodeVertexProperty(g2v)

			a[i] = prop.Value
			b[i] = g2prop.Value

			if !utils.FloatEquals(prop.Value, g2prop.Value, allowedVariance) {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				t.Fatalf("Value not equal. raw: %v value: %v vs: %v iteration %v", g1raw, prop.Value, g2prop.Value, count)
			}
		})
	}
}
