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
	graph.WithWeight
	graph.NoTimestamp
	graph.NoRaw
}

type Mail float64

type Note struct{}

func (VertexProperty) New() VertexProperty {
	return VertexProperty{EMPTY_VAL}
}

func (Mail) New() Mail {
	return EMPTY_VAL
}

func (*SSSP) MailMerge(incoming Mail, _ uint32, existing *Mail) (newInfo bool) {
	return incoming < utils.AtomicMinFloat64(existing, incoming)
}

func (*SSSP) MailRetrieve(existing *Mail, _ *graph.Vertex[VertexProperty, EdgeProperty]) Mail {
	return utils.AtomicLoadFloat64(existing)
}

// Function called for a vertex update.
func (alg *SSSP) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], n graph.Notification[Note], m Mail) (sent uint64) {
	// Only act on an improvement to shortest path.
	if src.Property.Value <= float64(m) {
		return 0
	}

	// Update our own value.
	src.Property.Value = float64(m)

	// Send an update to all neighbours.
	for _, e := range src.OutEdges {
		mailbox, tidx := g.NodeVertexMailbox(e.Didx)
		if alg.MailMerge(Mail(src.Property.Value+e.Property.Weight), n.Target, &mailbox.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
		}
	}
	return sent
}

// OnEdgeAdd: Function called upon a new edge add (which also bundles a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: eidxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func (alg *SSSP) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, eidxStart int, m Mail) (sent uint64) {
	// Do nothing if we had targeted all edges, otherwise target just the new edges.
	if sent = alg.OnUpdateVertex(g, gt, src, graph.Notification[Note]{Target: sidx}, m); sent != 0 {
		return sent
	}
	if src.Property.Value < EMPTY_VAL { // Only useful if we are connected
		// Target only new edges.
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			target := src.OutEdges[eidx].Didx
			mailbox, tidx := g.NodeVertexMailbox(target)
			if alg.MailMerge(Mail(src.Property.Value+src.OutEdges[eidx].Property.Weight), sidx, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: target}, mailbox, tidx))
			}
		}
	}
	return sent
}

// Not used in this algorithm.
func (*SSSP) OnEdgeDel(*graph.Graph[VertexProperty, EdgeProperty, Mail, Note], *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], *graph.Vertex[VertexProperty, EdgeProperty], uint32, []graph.Edge[EdgeProperty], Mail) (sent uint64) {
	panic("Incremental only algorithm")
}
