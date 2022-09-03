package main

import (
	"fmt"

	"github.com/ScottSallinen/lollipop/graph"
)

// Defines the properties stored per vertex. Can be used below within the algorithm.
type VertexProperty struct {
	Value float64
}

type EdgeProperty struct{}

type MessageValue float64

// Defines how the VertexProperty is printed
func (p *VertexProperty) String() string {
	return fmt.Sprintf("%.4f", p.Value)
}

// When multiple messages are for a vertex, how should we aggregate the info?
// At a basic level, we typically wish to lock the target, perform a function
// on the vertex's scratch data, and then unlock.
func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, data MessageValue) (newInfo bool) {
	return false
}

// When we need the scratch data, this needs to also be thread safe.
// This can be as simple as a lock and retrieve of the scratch data from the target vertex.
func AggregateRetrieve(target *graph.Vertex[VertexProperty, EdgeProperty]) MessageValue {
	return 0.0
}

// How should a vertex's initial state be defined.
func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) {
	g.Vertices[vidx].Property.Value = 0.0
}

// OnEdgeAdd: Function called upon a new edge add (which also bundes a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: didxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxStart int, data MessageValue) {

}

// This function is to be called on a single edge deletion event.
func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didx uint32, data MessageValue) {

}

// The main function for basic algorithm behaviour, and is the entry point.
// The data is pulled from the scratch (using aggregate retrieve above) before being
// handed to this function.
func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, data MessageValue) int {
	return 0
}

// A function to be called after the processing is complete; in case any finalization step is needed
func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
