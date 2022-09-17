package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

type VertexProperty struct {
	Value float64
}

func MessageAggregator(target *graph.Vertex[VertexProperty], data float64) (newInfo bool) {
	target.Mutex.Lock()
	tmp := target.Scratch
	target.Scratch = math.Min(target.Scratch, data)
	newInfo = tmp != target.Scratch
	target.Mutex.Unlock()
	return newInfo
}

func AggregateRetrieve(target *graph.Vertex[VertexProperty]) float64 {
	// We can leave Scratch alone, since we are monotonicly decreasing.
	target.Mutex.Lock()
	tmp := target.Scratch
	target.Mutex.Unlock()
	return tmp
}

// set vidx as label
func OnInitVertex(g *graph.Graph[VertexProperty], vidx uint32) {
	g.Vertices[vidx].Property.Value = float64(g.Vertices[vidx].Id)
	//g.Vertices[vidx].Property.Value = float64(vidx)
	g.Vertices[vidx].Scratch = float64(g.Vertices[vidx].Id)
	//g.Vertices[vidx].Scratch = float64(vidx)
}

func OnEdgeAdd(g *graph.Graph[VertexProperty], sidx uint32, didxs map[uint32]int, data float64) {
	if OnVisitVertex(g, sidx, data) > 0 {
		// do nothing, we had messaged all edges
	} else {
		src := &g.Vertices[sidx]
		if src.Property.Value < g.EmptyVal { // Do not need ? // Only useful if we are connected
			// Latest edge is len(src.OutEdges)-1
			// New: didx maps to the real index in the edge array
			for didx := range didxs {
				info("on edge add # sidx = ", sidx, " didx = ", didx, " value = ", src.Property.Value)
				g.OnQueueVisit(g, sidx, didx, src.Property.Value)
			}
		}
	}
}

func OnEdgeDel(g *graph.Graph[VertexProperty], sidx uint32, didx uint32, data float64) {
	enforce.ENFORCE(false, "Incremental only algorithm")
}

func OnVisitVertex(g *graph.Graph[VertexProperty], vidx uint32, data float64) int {
	src := &g.Vertices[vidx]
	// Only act on an improvement to shortest path.
	if src.Property.Value > data {
		// Update our own value.
		info("on visit vertex & update # vidx = ", vidx, " Id = ", g.Vertices[vidx].Id , " value befor = ", src.Property.Value, " after = ", data)
		src.Property.Value = data
		// Send an update to all neighbours.
		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Target
			info("on visit vertex & send # vidx = ", vidx, " target = ", target, " value = ", src.Property.Value)
			g.OnQueueVisit(g, vidx, target, src.Property.Value)
		}
		return len(src.OutEdges)
	}
	return 0
}

func OnFinish(g *graph.Graph[VertexProperty]) error {
	return nil
}
