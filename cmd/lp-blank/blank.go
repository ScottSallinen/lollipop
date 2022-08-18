package main

import (
	"github.com/ScottSallinen/lollipop/graph"
)

// Defines the properties stored per vertex. Can be used below within the algorithm.
type VertexProperty struct {
	Value float64
}

// When multiple messages are for a vertex, how should we aggregate the info?
// At a basic level, we typically wish to lock the target, perform a function
// on the vertex's scratch data, and then unlock.
func MessageAggregator(target *graph.Vertex[VertexProperty], data float64) (newInfo bool) {
	return false
}

// When we need the scratch data, this needs to also be thread safe.
// This can be as simple as a lock and retrieve of the scratch data from the target vertex.
func AggregateRetrieve(target *graph.Vertex[VertexProperty]) float64 {
	return 0.0
}

// How should a vertex's initial state be defined.
func OnInitVertex(g *graph.Graph[VertexProperty], vidx uint32) {
	g.Vertices[vidx].Property.Value = 0.0
}

// The function that is called when a group of edges are added to a vertex.
// The view here is **post** addition (the edges are already appended to the edge list)
// The input didxs argument is a map of the new edge didx (target) to the raw index in the vertice's edge list.
func OnEdgeAdd(g *graph.Graph[VertexProperty], sidx uint32, didxs map[uint32]int, data float64) {

}

// This function is to be called on a single edge deletion event.
func OnEdgeDel(g *graph.Graph[VertexProperty], sidx uint32, didx uint32, data float64) {

}

// The main function for basic algorithm behaviour, and is the entry point.
// The data is pulled from the scratch (using aggregate retrieve above) before being
// handed to this function.
func OnVisitVertex(g *graph.Graph[VertexProperty], vidx uint32, data float64) int {
	return 0
}

// A function to be called after the processing is complete; in case any finalization step is needed
func OnFinish(g *graph.Graph[VertexProperty]) error {
	return nil
}
