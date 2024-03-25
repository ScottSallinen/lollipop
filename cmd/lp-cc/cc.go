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

type Mail uint32

type Note struct{}

func (VertexProperty) New() VertexProperty {
	return VertexProperty{EMPTY_VAL}
}

func (m Mail) New() Mail {
	return EMPTY_VAL
}

// For connected components, a vertex can accept their own id as a potential label.
// Note that such a starting label should be unique, so raw identifier works for this (as it is uniquely defined externally).
// TODO: "Integer" was done for compatibility with tests -- don't think this would work for string graphs? Maybe use hash instead?
func (*CC) InitAllMail(g *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, internalId uint32, rawId graph.RawType) Mail {
	return Mail(rawId.Integer())
}

func (*CC) MailMerge(incoming Mail, _ uint32, existing *Mail) (newInfo bool) {
	return uint32(incoming) < utils.AtomicMinUint32((*uint32)(existing), uint32(incoming))
}

func (*CC) MailRetrieve(existing *Mail, _ *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) Mail {
	return Mail(atomic.LoadUint32((*uint32)(existing)))
}

// Function called for a vertex update.
func (alg *CC) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, notif graph.Notification[Note], m Mail) (sent uint64) {
	// Only act on an improvement to component.
	if prop.Value <= uint32(m) {
		return 0
	}

	// Update our own value.
	prop.Value = uint32(m)

	// Send an update to all neighbours.
	for _, e := range src.OutEdges {
		mailbox, tidx := g.NodeVertexMailbox(e.Didx)
		g.UpdateMsgStat(uint32(gt.Tidx), tidx)
		if alg.MailMerge(Mail(prop.Value), notif.Target, &mailbox.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(notif.Target, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
		}
	}
	return sent
}

// Function called upon a new edge add.
func (alg *CC) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	// Do nothing more if we update; we already targeted all edges.
	if sent = alg.OnUpdateVertex(g, gt, src, prop, graph.Notification[Note]{Target: sidx}, m); sent != 0 {
		return sent
	}

	// Otherwise, we need to target just the new edges.
	for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
		mailbox, tidx := g.NodeVertexMailbox(src.OutEdges[eidx].Didx)
		g.UpdateMsgStat(uint32(gt.Tidx), tidx)
		if alg.MailMerge(Mail(prop.Value), sidx, &mailbox.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: src.OutEdges[eidx].Didx}, mailbox, tidx))
		}
	}

	return sent
}

func (*CC) OnEdgeDel(*graph.Graph[VertexProperty, EdgeProperty, Mail, Note], *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], *graph.Vertex[VertexProperty, EdgeProperty], *VertexProperty, uint32, []graph.Edge[EdgeProperty], Mail) (sent uint64) {
	panic("Incremental only algorithm")
}
