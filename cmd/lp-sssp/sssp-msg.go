package main

import (
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
)

// A message-passing only variant of SSSP.
// This is just for reference, as it is not as efficient as the mailbox strategy.
type SSSPM struct{}

type VPMsg struct {
	Value      float64
	WillUpdate bool
}

type EPMsg struct {
	graph.WeightedEdge
}

type MailMsg struct{}

type NoteMsg float64

func (VPMsg) New() VPMsg {
	return VPMsg{EMPTY_VAL, false}
}

func (MailMsg) New() (m MailMsg) {
	return m
}

func (*SSSPM) MailMerge(_ MailMsg, _ uint32, _ *MailMsg) (newInfo bool) {
	return true // For a pure-message-passing algorithm, tell the framework we always want to update.
}

func (*SSSPM) MailRetrieve(_ *MailMsg, _ *graph.Vertex[VPMsg, EPMsg]) (m MailMsg) {
	return m // Unused with this strategy.
}

// The initialization note will begin the algorithm if we get that (Note value is zero).
func (*SSSPM) OnUpdateVertex(g *graph.Graph[VPMsg, EPMsg, MailMsg, NoteMsg], src *graph.Vertex[VPMsg, EPMsg], n graph.Notification[NoteMsg], _ MailMsg) (sent uint64) {
	if src.Property.Value > float64(n.Note) { // Only act on an improvement to shortest path.
		src.Property.Value = float64(n.Note)
		src.Property.WillUpdate = true
	}

	if n.Activity > 0 {
		return 0 // There are still queued notifications, just wait for the final one.
	}
	if !src.Property.WillUpdate {
		return 0 // No update, no need to send.
	}
	src.Property.WillUpdate = false

	for _, e := range src.OutEdges { // Send an update to all neighbours.
		mailbox, tidx := g.NodeVertexMailbox(e.Didx)
		message := NoteMsg(src.Property.Value + e.Property.Weight)
		sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsg]{Note: message, Target: e.Didx}, mailbox, tidx))
	}
	return sent
}

func (*SSSPM) OnEdgeAdd(g *graph.Graph[VPMsg, EPMsg, MailMsg, NoteMsg], src *graph.Vertex[VPMsg, EPMsg], sidx uint32, eidxStart int, _ MailMsg) (sent uint64) {
	if src.Property.Value == EMPTY_VAL {
		return 0 // Only bother if we are connected.
	}
	eidx := eidxStart // Default: send to only new edges.
	if src.Property.WillUpdate {
		src.Property.WillUpdate = false
		eidx = 0 // Send the update to all neighbours.
	}
	for ; eidx < len(src.OutEdges); eidx++ {
		mailbox, tidx := g.NodeVertexMailbox(src.OutEdges[eidx].Didx)
		message := NoteMsg(src.Property.Value + src.OutEdges[eidx].Property.Weight)
		sent += g.EnsureSend(g.ActiveNotification(sidx, graph.Notification[NoteMsg]{Note: message, Target: src.OutEdges[eidx].Didx}, mailbox, tidx))
	}
	return sent
}

// Compatibility stuff below.

func (*SSSPM) OnEdgeDel(*graph.Graph[VPMsg, EPMsg, MailMsg, NoteMsg], *graph.Vertex[VPMsg, EPMsg], uint32, []graph.Edge[EPMsg], MailMsg) (sent uint64) {
	panic("Incremental only algorithm")
}

func (*SSSPM) OnCheckCorrectness(g *graph.Graph[VPMsg, EPMsg, MailMsg, NoteMsg]) {
	maxValue := make([]float64, g.NumThreads)
	g.NodeParallelFor(func(_, _ uint32, gt *graph.GraphThread[VPMsg, EPMsg, MailMsg, NoteMsg]) int {
		tidx := gt.Tidx
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			vertex := &gt.Vertices[i]
			ourValue := vertex.Property.Value
			if ourValue < EMPTY_VAL {
				maxValue[tidx] = utils.Max(maxValue[tidx], (ourValue))
			}

			if _, ok := g.InitNotes[gt.VertexRawID(i)]; ok {
				if ourValue != float64(0) {
					log.Panic().Msg("Expected rawId " + utils.V(gt.VertexRawID(i)) + " to have init, but has " + utils.V(ourValue))
				}
			}
			if ourValue == EMPTY_VAL {
				// we were never visited.
			} else {
				for eidx := range vertex.OutEdges {
					targetProp := g.NodeVertex(vertex.OutEdges[eidx].Didx).Property.Value
					// Should not be worse than what we could provide.
					if targetProp > (ourValue + vertex.OutEdges[eidx].Property.Weight) {
						log.Panic().Msg("Unexpected neighbour weight: " + utils.V(targetProp) + ", vs our weight: " + utils.V(ourValue) + " with edge weight: " + utils.V(vertex.OutEdges[eidx].Property.Weight))
					}
				}
			}
		}
		return 0
	})
	log.Info().Msg("MaxValue (longest shortest path): " + utils.V(utils.MaxSlice(maxValue)))
}

func (*SSSPM) OnOracleCompare(g *graph.Graph[VPMsg, EPMsg, MailMsg, NoteMsg], oracle *graph.Graph[VPMsg, EPMsg, MailMsg, NoteMsg]) {
	graph.OracleGenericCompareValues(g, oracle, func(vp VPMsg) float64 { return vp.Value })
}
