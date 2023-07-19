package main

import (
	"math/rand"
	"testing"

	"github.com/ScottSallinen/lollipop/graph"
)

var graphOptions = graph.GraphOptions{
	Name:             "../../data/test.txt",
	Undirected:       true,
	CheckCorrectness: true,
}

func TestSyncStatic(t *testing.T) {
	for ti := 0; ti < 10; ti++ {
		myOpts := graphOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Dynamic = false
		myOpts.Sync = true
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(Colouring), myOpts, nil, nil)
		g.PrintVertexProps("Sync colours: ")
	}
}

func TestAsyncStatic(t *testing.T) {
	for ti := 0; ti < 10; ti++ {
		myOpts := graphOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Dynamic = false
		myOpts.Sync = false
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(Colouring), myOpts, nil, nil)
		g.PrintVertexProps("Async colours: ")
	}
}

func TestAsyncDynamic(t *testing.T) {
	for ti := 0; ti < 10; ti++ {
		myOpts := graphOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Dynamic = true
		myOpts.Sync = false
		g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(Colouring), myOpts, nil, nil)
		g.PrintVertexProps("Dynamic colours: ")
	}
}

func TestAsyncDynamicWithDelete(t *testing.T) {
	for ti := 0; ti < 10; ti++ {
		rawStructureChanges := []graph.TopologyEvent[EdgeProperty]{
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(1), DstRaw: graph.AsRawType(4), EdgeProperty: EdgeProperty{}},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(0), EdgeProperty: EdgeProperty{}},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(1), EdgeProperty: EdgeProperty{}},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(3), DstRaw: graph.AsRawType(0), EdgeProperty: EdgeProperty{}},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(2), EdgeProperty: EdgeProperty{}},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(3), EdgeProperty: EdgeProperty{}},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(5), EdgeProperty: EdgeProperty{}},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(6), DstRaw: graph.AsRawType(2), EdgeProperty: EdgeProperty{}},
		}

		adjustedStructureChanges := graph.InjectDeletesRetainFinalStructure(rawStructureChanges, 0.33)

		THREADS := uint32(rand.Intn(8-1) + 1)

		g := &graph.Graph[VertexProperty, EdgeProperty, Mail, Note]{}
		g.Options = graph.GraphOptions{
			NumThreads:       THREADS,
			Undirected:       true,
			CheckCorrectness: true,
			Dynamic:          true,
		}

		graph.DynamicGraphExecutionFromTestEvents(new(Colouring), g, adjustedStructureChanges)

		ComputeGraphColouringStat(g)
	}
}
