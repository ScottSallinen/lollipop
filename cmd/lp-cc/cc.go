package main

import (
	"math"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type CC struct{}

const EMPTY_VAL = math.MaxUint32

type VertexProperty struct {
	Value uint32
}

type EdgeProperty struct {
	graph.EmptyEdge
}

type Message uint32

type Note struct{}

func (VertexProperty) New() VertexProperty {
	return VertexProperty{EMPTY_VAL}
}

func (m Message) New() Message {
	return EMPTY_VAL
}

// For connected components, a vertex can accept their own id as a potential label.
// Note that such a starting label should be unique, so raw identifier works for this (as it is uniquely defined externally).
func (*CC) InitAllMessage(g *graph.Vertex[VertexProperty, EdgeProperty], internalId uint32, rawId graph.RawType) Message {
	return Message(rawId.Integer())
}

func (*CC) MessageMerge(incoming Message, _ uint32, existing *Message) (newInfo bool) {
	return uint32(incoming) < utils.AtomicMinUint32((*uint32)(existing), uint32(incoming))
}

func (*CC) MessageRetrieve(existing *Message, _ *graph.Vertex[VertexProperty, EdgeProperty]) Message {
	return Message(atomic.LoadUint32((*uint32)(existing)))
}

// Function called for a vertex update.
func (alg *CC) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], src *graph.Vertex[VertexProperty, EdgeProperty], notif graph.Notification[Note], message Message) (sent uint64) {
	// Only act on an improvement to component.
	if src.Property.Value <= uint32(message) {
		return 0
	}

	// Update our own value.
	src.Property.Value = uint32(message)

	// Send an update to all neighbours.
	for _, e := range src.OutEdges {
		vtm, tidx := g.NodeVertexMessages(e.Didx)
		if alg.MessageMerge(Message(src.Property.Value), notif.Target, &vtm.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(notif.Target, graph.Notification[Note]{Target: e.Didx}, vtm, tidx))
		}
	}
	return sent
}

// Function called upon a new edge add.
func (alg *CC) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, eidxStart int, message Message) (sent uint64) {
	// Do nothing more if we update; we already messaged all edges.
	if sent = alg.OnUpdateVertex(g, src, graph.Notification[Note]{Target: sidx}, message); sent != 0 {
		return sent
	}

	// Otherwise, we need to message just the new edges.
	for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
		vtm, tidx := g.NodeVertexMessages(src.OutEdges[eidx].Didx)
		if alg.MessageMerge(Message(src.Property.Value), sidx, &vtm.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: src.OutEdges[eidx].Didx}, vtm, tidx))
		}
	}

	return sent
}

func (*CC) OnEdgeDel(*graph.Graph[VertexProperty, EdgeProperty, Message, Note], *graph.Vertex[VertexProperty, EdgeProperty], uint32, []graph.Edge[EdgeProperty], Message) (sent uint64) {
	panic("Incremental only algorithm")
}
