package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

func MessageAggregator(target *graph.Vertex, data float64) (newInfo bool) {
	target.Mutex.Lock()
	tmp := target.Scratch
	target.Scratch = math.Min(target.Scratch, data)
	newInfo = tmp != target.Scratch
	target.Mutex.Unlock()
	return newInfo
}

func AggregateRetrieve(target *graph.Vertex) float64 {
	// We can leave Scratch alone, since we are monotonicly decreasing.
	target.Mutex.Lock()
	tmp := target.Scratch
	target.Mutex.Unlock()
	return tmp
}

func OnInitVertex(g *graph.Graph, vidx uint32) {
	g.Vertices[vidx].Value = g.EmptyVal
	g.Vertices[vidx].Scratch = g.EmptyVal
}

func OnEdgeAdd(g *graph.Graph, sidx uint32, didx uint32, data float64) {
	if OnVisitVertex(g, sidx, data) > 0 {
		// do nothing, we had messaged all edges
	} else {
		src := &g.Vertices[sidx]
		if src.Value < g.EmptyVal { // Only useful if we are connected
			// Latest edge is len(src.OutEdges)-1
			g.OnQueueVisit(g, sidx, didx, (src.Value + src.OutEdges[len(src.OutEdges)-1].Weight))
		}
	}
}

func OnEdgeDel(g *graph.Graph, sidx uint32, didx uint32, data float64) {
	enforce.ENFORCE(false, "Incremental only algorithm")
}

func OnVisitVertex(g *graph.Graph, vidx uint32, data float64) int {
	src := &g.Vertices[vidx]
	// Only act on an improvement to shortest path.
	if src.Value > data {
		// Update our own value.
		src.Value = data
		// Send an update to all neighbours.
		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Target
			g.OnQueueVisit(g, vidx, target, (src.Value + src.OutEdges[eidx].Weight))
		}
		return len(src.OutEdges)
	}
	return 0
}

func OnFinish(g *graph.Graph) error {
	return nil
}
