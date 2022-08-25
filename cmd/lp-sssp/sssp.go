package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

type VertexProperty struct {
	Value float64
}

type EdgeProperty struct{}

func MessageAggregator(target *graph.Vertex[VertexProperty, EdgeProperty], data float64) (newInfo bool) {
	target.Mutex.Lock()
	tmp := target.Scratch
	target.Scratch = math.Min(target.Scratch, data)
	newInfo = tmp != target.Scratch
	target.Mutex.Unlock()
	return newInfo
}

func AggregateRetrieve(target *graph.Vertex[VertexProperty, EdgeProperty]) float64 {
	// We can leave Scratch alone, since we are monotonicly decreasing.
	target.Mutex.Lock()
	tmp := target.Scratch
	target.Mutex.Unlock()
	return tmp
}

func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty], vidx uint32) {
	g.Vertices[vidx].Property.Value = g.EmptyVal
	g.Vertices[vidx].Scratch = g.EmptyVal
}

func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty], sidx uint32, didxs map[uint32]int, data float64) {
	if OnVisitVertex(g, sidx, data) > 0 {
		// do nothing, we had messaged all edges
	} else {
		src := &g.Vertices[sidx]
		if src.Property.Value < g.EmptyVal { // Only useful if we are connected
			// Latest edge is len(src.OutEdges)-1
			// New: didx maps to the real index in the edge array
			for didx, eidx := range didxs {
				g.OnQueueVisit(g, sidx, didx, src.Property.Value+src.OutEdges[eidx].GetWeight())
			}
		}
	}
}

func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty], sidx uint32, didx uint32, data float64) {
	enforce.ENFORCE(false, "Incremental only algorithm")
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty], vidx uint32, data float64) int {
	src := &g.Vertices[vidx]
	// Only act on an improvement to shortest path.
	if src.Property.Value > data {
		// Update our own value.
		src.Property.Value = data
		// Send an update to all neighbours.
		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Target
			g.OnQueueVisit(g, vidx, target, src.Property.Value+src.OutEdges[eidx].GetWeight())
		}
		return len(src.OutEdges)
	}
	return 0
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty]) error {
	return nil
}
