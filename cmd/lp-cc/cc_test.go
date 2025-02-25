package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Expect two connected components.
func testGraphExpect(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], t *testing.T) {
	expectations := []uint32{0, 1, 1, 0, 1, 1, 1, 0, 0, 0}
	for i := range expectations {
		internal, _ := g.NodeVertexFromRaw(graph.AsRawType(i))
		gVprop := g.NodeVertexProperty(internal)
		if gVprop.Value != expectations[i] {
			g.PrintVertexProps("")
			t.Fatal("vidx ", internal, " rawId ", i, " is ", gVprop.Value, " expected ", expectations[i])
		}
	}
}

var baseOptions = graph.GraphOptions{
	Name:             "../../data/test_multiple_components.txt",
	Undirected:       true, // undirected should always be true.
	CheckCorrectness: true,
}

func TestAsyncStatic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = false
		myOpts.Dynamic = false
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(CC), myOpts, nil, nil)
		testGraphExpect(g, t)
	}
}
func TestSyncStatic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = true
		myOpts.Dynamic = false
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(CC), myOpts, nil, nil)
		testGraphExpect(g, t)
	}
}
func TestDynamic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Dynamic = true
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(CC), myOpts, nil, nil)
		testGraphExpect(g, t)
	}
}

func TestDynamicCreation(t *testing.T) {
	rand.NewSource(time.Now().UTC().UnixNano())

	for count := 0; count < 10; count++ {
		threads := uint32(rand.Intn(8-1) + 1)

		t.Log("TestDynamicCreation ", count, " t ", threads)

		rawTestGraph := []graph.InputEvent[EdgeProperty]{
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(1), DstRaw: graph.AsRawType(4)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(7), DstRaw: graph.AsRawType(0)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(1)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(3), DstRaw: graph.AsRawType(0)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(2)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(8), DstRaw: graph.AsRawType(3)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(5)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(6), DstRaw: graph.AsRawType(2)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(7), DstRaw: graph.AsRawType(3)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(8), DstRaw: graph.AsRawType(9)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(9), DstRaw: graph.AsRawType(0)},
		}
		utils.Shuffle(rawTestGraph)

		gDyn := &graph.Graph[VertexProperty, EdgeProperty, Mail, Note]{}
		gDyn.Options = graph.GraphOptions{
			NumThreads:       threads,
			Dynamic:          true,
			Undirected:       true, // undirected should always be true.
			CheckCorrectness: true,
		}

		graph.DynamicGraphExecutionFromTestEvents(new(CC), gDyn, rawTestGraph)

		myOpts := baseOptions
		myOpts.NumThreads = threads

		gStatic := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(CC), myOpts, nil, nil)

		graph.CheckGraphStructureEquality(gDyn, gStatic)

		a := make([]uint32, gDyn.NodeVertexCount())
		b := make([]uint32, gStatic.NodeVertexCount())

		gDyn.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) {
			g1raw := gDyn.NodeVertexRawID(v)
			g2v, _ := gStatic.NodeVertexFromRaw(g1raw)
			g2prop := gStatic.NodeVertexProperty(g2v)

			a[i] = prop.Value
			b[i] = g2prop.Value

			if prop.Value != g2prop.Value {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				t.Fatal("Value not equal ", g1raw, " ", prop.Value, " ", g2prop.Value, " iteration ", count)
			}
		})
	}
}
