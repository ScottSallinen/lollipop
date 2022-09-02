package main

import (
	"fmt"
	"math"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

type VertexProperty struct {
	Value   float64
	Scratch float64 // Intermediary accumulator
}

type EdgeProperty struct {
	Weight float64
}

func (p *VertexProperty) String() string {
	return fmt.Sprintf("%.4f", p.Value)
}

func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, data float64) (newInfo bool) {
	dst.Mutex.Lock()
	tmp := dst.Property.Scratch
	dst.Property.Scratch = math.Min(dst.Property.Scratch, data)
	newInfo = tmp != dst.Property.Scratch
	dst.Mutex.Unlock()
	return newInfo
}

func AggregateRetrieve(target *graph.Vertex[VertexProperty, EdgeProperty]) float64 {
	// We can leave Scratch alone, since we are monotonicly decreasing.
	target.Mutex.Lock()
	tmp := target.Property.Scratch
	target.Mutex.Unlock()
	return tmp
}

func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty], vidx uint32) {
	g.Vertices[vidx].Property.Value = g.EmptyVal
	g.Vertices[vidx].Property.Scratch = g.EmptyVal
}

// OnEdgeAdd: Function called upon a new edge add (which also bundes a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: didxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty], sidx uint32, didxStart int, data float64) {
	if OnVisitVertex(g, sidx, data) > 0 {
		// do nothing, we had messaged all edges
	} else {
		src := &g.Vertices[sidx]
		if src.Property.Value < g.EmptyVal { // Only useful if we are connected
			// Message only new edges.
			for eidx := didxStart; eidx < len(src.OutEdges); eidx++ {
				target := src.OutEdges[eidx].Destination
				g.OnQueueVisit(g, sidx, target, src.Property.Value+src.OutEdges[eidx].Property.Weight)
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
			target := src.OutEdges[eidx].Destination
			g.OnQueueVisit(g, vidx, target, src.Property.Value+src.OutEdges[eidx].Property.Weight)
		}
		return len(src.OutEdges)
	}
	return 0
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty]) error {
	return nil
}
