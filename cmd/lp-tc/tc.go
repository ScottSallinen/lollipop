package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Basic, un-optimized, incremental triangle counting algorithm via message passing. For counting undirected triangles.
// Closes triangles via a wedge that has an opposing edge that came earlier than either wedge edge.
// To match results of prior work, "discards" self edges and multi-edges during construction (done live, no pre-processing needed).
type TC struct{}

const EMPTY = math.MaxUint32
const FLAG = 1 << 31
const MASK = (1 << 31) - 1

type VertexProp struct {
	NumLeader     uint32
	UpdateAt      uint32
	MapEdgesToPos map[graph.RawType]uint32
	// MapChecks      uint64 // Debug info: Number of map lookups. Should be a constant for a given topology.
}

type EdgeProp struct {
	graph.WithRaw     // Present algorithm strategy uses the raw ID of the vertex in the map; and we need to store it on the edges.
	graph.NoTimestamp // We only require logical time / relative order for this algorithm.
	graph.NoWeight
}

func (*EdgeProp) ParseProperty([]string, int32, int32) {}

type Mail struct{}

type Note struct {
	NewEdges []graph.Edge[EdgeProp]
	Pos      uint32
}

func (VertexProp) New() (vp VertexProp) {
	vp.MapEdgesToPos = make(map[graph.RawType]uint32)
	return vp
}

// We have some structural data that an oracle graph copy would need (built during OnInEdgeAdd, and is 'read-only' after setting).
func (VertexProp) CopyForOracle(oracle *VertexProp, given *VertexProp) {
	oracle.MapEdgesToPos = given.MapEdgesToPos
}

// Unused (this is a message passing algorithm without mail/aggregation).
func (Mail) New() (m Mail)                                                       { return m }
func (*TC) MailMerge(_ Mail, _ uint32, _ *Mail) bool                             { return true }
func (*TC) MailRetrieve(_ *Mail, _ *graph.Vertex[VertexProp, EdgeProp]) (m Mail) { return m }

// Initialization notification.
func (*TC) InitAllNote(*graph.Vertex[VertexProp, EdgeProp], uint32, graph.RawType) (n Note) {
	return n // Will just activate the vertex.
}

// Function called for a vertex update.
func (alg *TC) OnUpdateVertex(g *graph.Graph[VertexProp, EdgeProp, Mail, Note], gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note], src *graph.Vertex[VertexProp, EdgeProp], notif graph.Notification[Note], _ Mail) (sent uint64) {
	// Check if we lead a triangle: in logical time closed by them (notifier) to us, using any vertex with an edge they have (a wedge) -- such that we already had an edge with that vertex (the lesser edge of the wedge).
	// Based on the messages that are sent to us, the notifier will only ask us to check their wedges that occur before our edge with them.
	for wedge := 0; wedge < len(notif.Note.NewEdges); wedge++ {
		if wedgeRaw := (notif.Note.NewEdges[wedge].Property.GetRaw()); wedgeRaw != EMPTY { // Ignore discarded edges (the edges they share may include some "holes"; no problem, just skip these).
			// src.Property.MapChecks++ // Have to do a map check.
			if edgePos, in := src.Property.MapEdgesToPos[(wedgeRaw & MASK)]; !in {
				continue // Not a triangle (we didn't already have an edge with their wedge).
			} else if edgePos < notif.Note.Pos { // The found edge has to have existed before the sender's edge, for us to be the leader.
				src.Property.NumLeader++ // We lead a triangle.
			}
		}
	}
	notif.Note.NewEdges = nil

	// Wait for no further activity into this vertex (i.e., for now we believe there are no more enqueued notifications to this vertex).
	// Then, send an update: checking first if there is range that we have yet to send out.
	// The range we send to existing edges starts at our UpdateAt, and ends at the end of our edge data.
	if (notif.Activity == 0) && (int(src.Property.UpdateAt) < len(src.OutEdges)) {
		start := int(src.Property.UpdateAt)
		src.Property.UpdateAt = uint32(len(src.OutEdges))

		for e := start + 1; e < len(src.OutEdges); e++ {
			if src.OutEdges[e].Property.GetRaw()&FLAG != 0 { // EMPTY has FLAG
				continue // Ignore discarded and even events, they will never lead a triangle.
			}
			mailbox, tidx := g.NodeVertexMailbox(src.OutEdges[e].Didx)
			// If we have edges that come in time logically after this edge, the destination should not form a triangle with it. We end at e.
			sent += g.EnsureSend(g.ActiveNotification(notif.Target, graph.Notification[Note]{Target: src.OutEdges[e].Didx, Note: Note{src.OutEdges[start:e], src.OutEdges[e].Pos}}, mailbox, tidx))
		}
	}
	return sent
}

// Function called when an in-edge is first observed at the destination vertex. This happens before the edge is given to the source vertex as an out-edge.
func (*TC) OnInEdgeAdd(_ *graph.Graph[VertexProp, EdgeProp, Mail, Note], _ *graph.GraphThread[VertexProp, EdgeProp, Mail, Note], dst *graph.Vertex[VertexProp, EdgeProp], _ uint32, pos uint32, topEvent *graph.TopologyEvent[EdgeProp]) {
	if topEvent.DstRaw == topEvent.SrcRaw {
		topEvent.EdgeProperty.Raw = EMPTY // Self edge. For this algorithm, we discard these. (Note this creates a "hole" in the edges.)
	} else if _, in := dst.Property.MapEdgesToPos[topEvent.SrcRaw]; !in { // Update my (in) edges, mapping them to their logical position.
		dst.Property.MapEdgesToPos[topEvent.SrcRaw] = pos // We don't actually need to store an event id itself, just the position; we can tell relative order from that.
		if (topEvent.EventIdx() % 2) == 0 {
			topEvent.EdgeProperty.Raw |= FLAG // If this is an even event, it will never lead a triangle. Don't discard, but flag to not send it edges to check.
		}
	} else { // Already in our edge set: the input graph must be a multi-graph. For this algorithm, we will discard duplicates; we only consider the first edge between two vertices. (Again a "hole".)
		topEvent.EdgeProperty.Raw = EMPTY
	}
}

// Function called upon a new edge add.
func (alg *TC) OnEdgeAdd(g *graph.Graph[VertexProp, EdgeProp, Mail, Note], gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note], src *graph.Vertex[VertexProp, EdgeProp], sidx uint32, eidxStart int, _ Mail) (sent uint64) {
	// If we don't have waiting notifications, we must update old edges now (i.e., we can't be sure OnUpdateVertex will be called later).
	if gt.VertexMailbox(sidx).Activity == 0 {
		start := int(src.Property.UpdateAt)
		src.Property.UpdateAt = uint32(len(src.OutEdges))

		// Only before new edge(s), since we send additional data to them.
		for e := start + 1; e < eidxStart; e++ {
			if src.OutEdges[e].Property.GetRaw()&FLAG != 0 { // EMPTY has FLAG
				continue // Ignore discarded and even events, they will never lead a triangle.
			}
			mailbox, tidx := g.NodeVertexMailbox(src.OutEdges[e].Didx)
			// If we have edges that come in time logically after this edge, the destination should not form a triangle with it. We end at e.
			sent += g.EnsureSend(g.ActiveNotification(sidx, graph.Notification[Note]{Target: src.OutEdges[e].Didx, Note: Note{src.OutEdges[start:e], src.OutEdges[e].Pos}}, mailbox, tidx))
		}
	}
	// Update new edges. The range we send to new edges starts at 0, and ends at our UpdateAt.
	for e := eidxStart; e < len(src.OutEdges); e++ {
		if src.OutEdges[e].Property.GetRaw()&FLAG != 0 { // EMPTY has FLAG
			continue // Ignore discarded and even events, they will never lead a triangle.
		}
		mailbox, tidx := g.NodeVertexMailbox(src.OutEdges[e].Didx)
		end := utils.Min(int(src.Property.UpdateAt), e) // If we updated above, we must take the smaller.
		sent += g.EnsureSend(g.ActiveNotification(sidx, graph.Notification[Note]{Target: src.OutEdges[e].Didx, Note: Note{src.OutEdges[:end], src.OutEdges[e].Pos}}, mailbox, tidx))
	}
	return sent
}

func (*TC) OnEdgeDel(*graph.Graph[VertexProp, EdgeProp, Mail, Note], *graph.GraphThread[VertexProp, EdgeProp, Mail, Note], *graph.Vertex[VertexProp, EdgeProp], uint32, []graph.Edge[EdgeProp], Mail) (sent uint64) {
	panic("Incremental only algorithm")
}
