package main

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

var baseOptions = graph.GraphOptions{
	Name:             "../../data/test.txt",
	CheckCorrectness: true,
}

func TestDynamic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = false
		myOpts.Dynamic = true
		graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(PageRank), myOpts)
	}
}
func TestAsyncStatic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = false
		myOpts.Dynamic = false
		graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(PageRank), myOpts)
	}
}
func TestSyncStatic(t *testing.T) {
	for tCount := 0; tCount < 10; tCount++ {
		myOpts := baseOptions
		myOpts.NumThreads = uint32(rand.Intn(8-1) + 1)
		myOpts.Sync = true
		myOpts.Dynamic = false
		graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(PageRank), myOpts)
	}
}

func TestDynamicCreationDirected(t *testing.T) {
	DynamicCreation(false, t)
}

func TestDynamicCreationUnDirected(t *testing.T) {
	DynamicCreation(true, t)
}

func DynamicCreation(undirected bool, t *testing.T) {
	rand.NewSource(time.Now().UTC().UnixNano())
	allowedVariance := EPSILON * float64(7) // an epsilon for each vertex I believe would be possible

	for tCount := 0; tCount < 10; tCount++ {
		THREADS := uint32(rand.Intn(8-1) + 1)

		t.Log("TestDynamicCreation ", tCount, " t ", THREADS)

		rawTestGraph := []graph.TopologyEvent[EdgeProperty]{
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(1), DstRaw: graph.AsRawType(4)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(0)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(1)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(3), DstRaw: graph.AsRawType(0)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(2)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(3)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(5)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(6), DstRaw: graph.AsRawType(2)},
		}
		utils.Shuffle(rawTestGraph)

		gDyn := &graph.Graph[VertexProperty, EdgeProperty, Mail, Note]{}
		gDyn.Options = graph.GraphOptions{
			NumThreads:       THREADS,
			Undirected:       undirected,
			Dynamic:          true,
			CheckCorrectness: true,
		}
		graph.DynamicGraphExecutionFromTestEvents(new(PageRank), gDyn, rawTestGraph)

		myOpts := baseOptions
		myOpts.NumThreads = THREADS
		myOpts.Undirected = undirected
		gStatic := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(PageRank), myOpts)

		graph.CheckGraphStructureEquality(gDyn, gStatic)

		gDyn.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
			g1raw := gDyn.NodeVertexRawID(v)
			_, g2values := gStatic.NodeVertexFromRaw(g1raw)

			if !utils.FloatEquals(vertex.Property.Mass, g2values.Property.Mass, allowedVariance) {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				PrintTopN(gStatic, 10)
				PrintTopN(gDyn, 10)
				t.Fatal("Value not equal", g1raw, vertex.Property.Mass, g2values.Property.Mass, "iteration", tCount)
			}
			if !utils.FloatEquals(vertex.Property.InFlow, g2values.Property.InFlow, allowedVariance) {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				PrintTopN(gStatic, 10)
				PrintTopN(gDyn, 10)
				t.Fatal("InFlow not equal", g1raw, vertex.Property.InFlow, g2values.Property.InFlow, "iteration", tCount)
			}
		})
	}
}

func TestDynamicWithDeleteDirected(t *testing.T) {
	DynamicWithDelete(false, t)
}

func TestDynamicWithDeleteUnDirected(t *testing.T) {
	DynamicWithDelete(true, t)
}

func DynamicWithDelete(undirected bool, t *testing.T) {
	rand.NewSource(time.Now().UTC().UnixNano())
	allowedVariance := EPSILON * 2 * float64(7) // 2*epsilon for each vertex I believe would be possible

	for tCount := 0; tCount < 10; tCount++ {
		THREADS := uint32(rand.Intn(8-1) + 1)

		rawTestGraph := []graph.TopologyEvent[EdgeProperty]{
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(1), DstRaw: graph.AsRawType(4)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(0)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(2), DstRaw: graph.AsRawType(1)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(3), DstRaw: graph.AsRawType(0)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(2)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(3)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(4), DstRaw: graph.AsRawType(5)},
			{TypeAndEventIdx: uint64(graph.ADD), SrcRaw: graph.AsRawType(6), DstRaw: graph.AsRawType(2)},
		}

		adjustedGraph := graph.InjectDeletesRetainFinalStructure(rawTestGraph, 0.33)

		gDyn := &graph.Graph[VertexProperty, EdgeProperty, Mail, Note]{}
		gDyn.Options = graph.GraphOptions{
			NumThreads:       THREADS,
			Undirected:       undirected,
			Dynamic:          true,
			CheckCorrectness: true,
		}
		gDyn.InitMail = nil
		graph.DynamicGraphExecutionFromTestEvents(new(PageRank), gDyn, adjustedGraph)

		mGraphOptions := graph.GraphOptions{
			Name:             "../../data/test.txt",
			NumThreads:       THREADS,
			Undirected:       undirected,
			CheckCorrectness: true,
		}

		gStatic := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(PageRank), mGraphOptions)

		graph.CheckGraphStructureEquality(gDyn, gStatic)

		gDyn.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
			g1raw := gDyn.NodeVertexRawID(v)
			_, g2values := gStatic.NodeVertexFromRaw(g1raw)

			if !utils.FloatEquals(vertex.Property.Mass, g2values.Property.Mass, allowedVariance) {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				PrintTopN(gStatic, 10)
				PrintTopN(gDyn, 10)
				t.Fatal("Value not equal", g1raw, vertex.Property.Mass, g2values.Property.Mass, "iteration", tCount)
			}
			if !utils.FloatEquals(vertex.Property.InFlow, g2values.Property.InFlow, allowedVariance) {
				gStatic.PrintVertexProps("S ")
				gDyn.PrintVertexProps("D ")
				PrintTopN(gStatic, 10)
				PrintTopN(gDyn, 10)
				t.Fatal("InFlow not equal", g1raw, vertex.Property.InFlow, g2values.Property.InFlow, "iteration", tCount)
			}
		})
	}
}
