package main

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/ScottSallinen/lollipop/graph"
)

type SSSP struct{}

const EMPTY_VAL = math.MaxFloat64

type MapVertexDistance map[uint32]float64

type VertexProperty struct {
	Value           float64
	PrevDistanceMap MapVertexDistance
}

type EdgeProperty struct {
	graph.WithWeight
	graph.NoTimestamp
	graph.NoRaw
}

type Mail struct {
	distanceMap MapVertexDistance
}

type Note struct{}

func (VertexProperty) New() VertexProperty {
	return VertexProperty{EMPTY_VAL, MapVertexDistance{}}
}

func (Mail) New() Mail {
	return Mail{make(map[uint32]float64)}
}

func (*SSSP) MailMerge(incoming Mail, sidx uint32, existing *Mail) (newInfo bool) {
	if prevValue, keyExists := existing.distanceMap[sidx]; keyExists && prevValue == incoming.distanceMap[sidx] {
		newInfo = false
	} else {
		existing.distanceMap[sidx] = incoming.distanceMap[sidx]
		newInfo = true
	}
	return newInfo
}

func (*SSSP) MailRetrieve(existing *Mail, _ *graph.Vertex[VertexProperty, EdgeProperty], _ *VertexProperty) Mail {
	// Atomically load the value of existing
	atomicMail := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&existing)))
	// Convert the atomic pointer back to *Mail
	return *(*Mail)(atomicMail)
}

// Function called for a vertex update.
func (alg *SSSP) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	log.Debug().Msg(fmt.Sprintf("Updating Verted %v with mail: %v", n.Target, m))
	prevPropValue := prop.Value
	newPropValue := EMPTY_VAL
	for prevId, newValue := range m.distanceMap {
		prop.PrevDistanceMap[prevId] = newValue
	}
	for _, val := range prop.PrevDistanceMap {
		newPropValue = min(newPropValue, val)
	}

	// Only act on an improvement to shortest path.
	if newPropValue == prevPropValue {
		return 0
	}

	prop.Value = newPropValue

	// Send an update to all neighbours.
	for _, e := range src.OutEdges {
		mailbox, tidx := g.NodeVertexMailbox(e.Didx)
		if alg.MailMerge(Mail{MapVertexDistance{n.Target: prop.Value + e.Property.Weight}}, n.Target, &mailbox.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
		}
	}
	return sent
}

// OnEdgeAdd: Function called upon a new edge add (which also bundles a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: eidxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func (alg *SSSP) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	log.Debug().Msg("Called OnEdgeAdd")
	// Do nothing if we had targeted all edges, otherwise target just the new edges.
	if sent = alg.OnUpdateVertex(g, gt, src, prop, graph.Notification[Note]{Target: sidx}, m); sent != 0 {
		return sent
	}
	if prop.Value < EMPTY_VAL { // Only useful if we are connected
		// Target only new edges.
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			target := src.OutEdges[eidx].Didx
			mailbox, tidx := g.NodeVertexMailbox(target)
			if alg.MailMerge(
				Mail{MapVertexDistance{sidx: prop.Value + src.OutEdges[eidx].Property.Weight}},
				sidx, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: target}, mailbox, tidx))
			}
		}
	}
	return sent
}

// Not used in this algorithm.
func (alg *SSSP) OnEdgeDel(
	g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note],
	gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note],
	src *graph.Vertex[VertexProperty, EdgeProperty],
	prop *VertexProperty,
	sidx uint32,
	delEdges []graph.Edge[EdgeProperty],
	m Mail) (sent uint64) {
	log.Debug().Msg(fmt.Sprintf("Called OnEdgeDel! %v - %v - %v", src.OutEdges, delEdges, m))

	var currentMinWeights map[uint32]float64
	for _, edge := range src.OutEdges {
		if val, exists := currentMinWeights[edge.Didx]; !exists {
			currentMinWeights[edge.Didx] = val
		} else {
			currentMinWeights[edge.Didx] = min(currentMinWeights[edge.Didx], val)
		}
	}
	for _, delE := range delEdges {
		if currw, exists := currentMinWeights[delE.Didx]; !exists || currw > delE.Property.Weight {
			if !exists {
				currw = EMPTY_VAL
			}
			log.Debug().Msg(fmt.Sprintf("Sending notif to %v", delE.Didx))
			mailbox, tidx := g.NodeVertexMailbox(delE.Didx)
			if alg.MailMerge(
				Mail{MapVertexDistance{sidx: prop.Value + currw}},
				sidx, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: delE.Didx}, mailbox, tidx))
			}
		}
	}
	log.Debug().Msg("Completed OnEdgeDel")
	return sent

	//panic("Not implemented!")
	//
	//var newEdges []graph.Edge[EdgeProperty]
	//newValue := EMPTY_VAL
	//for _, ep := range delEdges {
	//	for _, delEp := range delEdges {
	//		if ep.Didx != delEp.Didx {
	//			newValue = min(newValue, )
	//			newEdges = append(newEdges, ep)
	//		}
	//	}
	//}
	//src.OutEdges = newEdges

}
