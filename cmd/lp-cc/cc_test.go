package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Expect two connected components.
func testGraphExpect(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], t *testing.T) {
	expectations := []uint32{0, 1, 1, 0, 1, 1, 1, 0, 0, 0}
	for i := range expectations {
		internal, gV := g.NodeVertexFromRaw(graph.AsRawType(i))
		if gV.Property.Value != expectations[i] {
			g.PrintVertexProps("")
			t.Fatal("vidx ", internal, " rawId ", i, " is ", gV.Property.Value, " expected ", expectations[i])
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
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Message, Note](new(CC), myOpts)
		testGraphExpect(g, t)
	}
}
func TestSyncStatic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = true
		myOpts.Dynamic = false
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Message, Note](new(CC), myOpts)
		testGraphExpect(g, t)
	}
}
func TestDynamic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Dynamic = true
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Message, Note](new(CC), myOpts)
		testGraphExpect(g, t)
	}
}

func TestDynamicCreation(t *testing.T) {
	rand.NewSource(time.Now().UTC().UnixNano())

	for count := 0; count < 10; count++ {
		threads := uint32(rand.Intn(8-1) + 1)

		t.Log("TestDynamicCreation ", count, " t ", threads)

		rawTestGraph := []graph.TopologyEvent[EdgeProperty]{
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

		gDyn := &graph.Graph[VertexProperty, EdgeProperty, Message, Note]{}
		gDyn.Options = graph.GraphOptions{
			NumThreads:       threads,
			Dynamic:          true,
			Undirected:       true, // undirected should always be true.
			CheckCorrectness: true,
		}

		graph.DynamicGraphExecutionFromTestEvents(new(CC), gDyn, rawTestGraph)

		myOpts := baseOptions
		myOpts.NumThreads = threads

		gStatic := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Message, Note](new(CC), myOpts)

		graph.CheckGraphStructureEquality(gDyn, gStatic)

		a := make([]uint32, gDyn.NodeVertexCount())
		b := make([]uint32, gStatic.NodeVertexCount())

		gDyn.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
			g1raw := gDyn.NodeVertexRawID(v)
			_, g2values := gStatic.NodeVertexFromRaw(g1raw)

			a[i] = vertex.Property.Value
			b[i] = g2values.Property.Value

			if vertex.Property.Value != g2values.Property.Value {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				t.Fatal("Value not equal ", g1raw, " ", vertex.Property.Value, " ", g2values.Property.Value, " iteration ", count)
			}
		})
	}
}
