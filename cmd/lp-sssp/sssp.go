package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type SSSP struct{}

const EMPTY_VAL = math.MaxFloat64

type VertexProperty struct {
	Value float64
}

type EdgeProperty struct {
	graph.WeightedEdge
}

type Message float64

type Note struct{}

func (VertexProperty) New() VertexProperty {
	return VertexProperty{EMPTY_VAL}
}

func (Message) New() Message {
	return EMPTY_VAL
}

func (*SSSP) MessageMerge(incoming Message, _ uint32, existing *Message) (newInfo bool) {
	return incoming < utils.AtomicMinFloat64(existing, incoming)
}

func (*SSSP) MessageRetrieve(existing *Message, _ *graph.Vertex[VertexProperty, EdgeProperty]) Message {
	return utils.AtomicLoadFloat64(existing)
}

// Function called for a vertex update.
func (alg *SSSP) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], src *graph.Vertex[VertexProperty, EdgeProperty], n graph.Notification[Note], m Message) (sent uint64) {
	// Only act on an improvement to shortest path.
	if src.Property.Value <= float64(m) {
		return 0
	}

	// Update our own value.
	src.Property.Value = float64(m)

	// Send an update to all neighbours.
	for _, e := range src.OutEdges {
		vtm, tidx := g.NodeVertexMessages(e.Didx)
		if alg.MessageMerge(Message(src.Property.Value+e.Property.Weight), n.Target, &vtm.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[Note]{Target: e.Didx}, vtm, tidx))
		}
	}
	return sent
}

// OnEdgeAdd: Function called upon a new edge add (which also bundles a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: eidxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func (alg *SSSP) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, eidxStart int, m Message) (sent uint64) {
	// Do nothing if we had messaged all edges, otherwise message just the new edges.
	if sent = alg.OnUpdateVertex(g, src, graph.Notification[Note]{Target: sidx}, m); sent != 0 {
		return sent
	}
	if src.Property.Value < EMPTY_VAL { // Only useful if we are connected
		// Message only new edges.
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			target := src.OutEdges[eidx].Didx
			vtm, tidx := g.NodeVertexMessages(target)
			if alg.MessageMerge(Message(src.Property.Value+src.OutEdges[eidx].Property.Weight), sidx, &vtm.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: target}, vtm, tidx))
			}
		}
	}
	return sent
}

// Not used in this algorithm.
func (*SSSP) OnEdgeDel(*graph.Graph[VertexProperty, EdgeProperty, Message, Note], *graph.Vertex[VertexProperty, EdgeProperty], uint32, []graph.Edge[EdgeProperty], Message) (sent uint64) {
	panic("Incremental only algorithm")
}
