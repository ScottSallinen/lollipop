package main

import (
	"fmt"
	"math"

	"github.com/ScottSallinen/lollipop/graph"
)

// Defines when a message is deemed empty, uninitialized, etc.
const EMPTYVAL = math.MaxFloat64

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
func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, VisitMsg MessageValue) (newInfo bool) {
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
// The VisitMsg is pulled from AggregateRetrieve before calling this function (allowing one to merge a visit call here)
func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxStart int, VisitMsg MessageValue) (RevData []MessageValue) {
	src := &g.Vertices[sidx]
	// Do algorithm...
	// If desired, produce a message for the edge destinations that were just created (must enable g.SendRevMsgs, or g.Undirected)
	RevData = make([]MessageValue, len(src.OutEdges)-didxStart)
	return RevData
}

// This function is to be called on a single edge deletion event.
// We are sidx, we lost an edge to didx.
func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didx uint32, VisitMsg MessageValue) (RevData MessageValue) {
	// If desired, produce a message for the edge destination that we just removed (must enable g.SendRevMsgs)
	return EMPTYVAL
}

// If g.SendRevMsgs or g.Undirected were enabled, this will be called on the reverse of the edge change.
// sidx is us, didx is them, HOWEVER the edge that was added was didx->sidx (unless undirected, in which case our matching edge was also deleted)
// The VisitMsg is pulled from AggregateRetrieve before calling this function (allowing one to merge a visit call here)
// The SourceMsgs are produced in OnEdgeAdd from didx (one per newly added edge).
func OnEdgeAddRev(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxStart int, VisitMsg MessageValue, SourceMsgs []MessageValue) {

}

// If g.SendRevMsgs or g.Undirected were enabled, this will be called on the reverse of the edge change.
// sidx is us, didx is them, HOWEVER the edge that was deleted was didx->sidx (unless undirected, in which case our matching edge was also deleted)
// The message was produced in OnEdgeDel from didx.
func OnEdgeDelRev(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didx uint32, VisitMsg MessageValue) {

}

// The main function for basic algorithm behaviour, and is the entry point.
// The VisitMsg is pulled from the aggregation (using AggregateRetrieve above) before being handed to this function.
func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, data MessageValue) int {
	return 0
}

// A function to be called after the processing is complete; in case any finalization step is needed
func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
