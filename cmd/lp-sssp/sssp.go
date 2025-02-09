package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
	"math"
	"reflect"
)

type Predecessor struct {
	PrevList      map[uint32]struct{}
	TotalDistance float64
}

func NewEmptyPredecessor() Predecessor {
	return Predecessor{
		map[uint32]struct{}{}, EMPTY_VAL,
	}
}

func AddToPredecessor(p Predecessor, newVertex uint32, newEdgeWeight float64) Predecessor {
	newPrevList := make(map[uint32]struct{})
	for k, v := range p.PrevList {
		newPrevList[k] = v
	}
	newPrevList[newVertex] = struct{}{}
	return Predecessor{
		newPrevList,
		p.TotalDistance + newEdgeWeight,
	}
}

func (p Predecessor) IsInPredecessor(v uint32) bool {
	_, exist := p.PrevList[v]
	return exist
}

type SSSP struct{}

const EMPTY_VAL = math.MaxFloat64

// This maps every node, to it's tree of predecessors to source
//
//	for example: {
//				s: [1, ...., s],
//				v: [] -> No path to 1
//			}
type MapVertexDistance map[uint32]Predecessor

type VertexProperty struct {
	Predecessor     Predecessor
	PrevDistanceMap MapVertexDistance
}

type EdgeProperty struct {
	graph.WithWeight
	graph.NoTimestamp
	graph.NoRaw
}

type Mail struct {
	distanceMap *ConcurrentMap[uint32, Predecessor]
}

type Note struct{}

func (VertexProperty) New() VertexProperty {
	return VertexProperty{NewEmptyPredecessor(), MapVertexDistance{}}
}

func (Mail) New() Mail {
	return Mail{NewConcurrentMap[uint32, Predecessor]()}
}

func (*SSSP) MailMerge(incoming Mail, sidx uint32, existing *Mail) (newInfo bool) {
	prevValue, keyExists := existing.distanceMap.Get(sidx)
	newValue, newExists := incoming.distanceMap.Get(sidx)
	if keyExists && newExists && prevValue.TotalDistance == newValue.TotalDistance && reflect.DeepEqual(prevValue.PrevList, newValue.PrevList) {
		newInfo = false
	} else {
		log.Debug().Msg(fmt.Sprintf("Log merged %v: %v vs %v", sidx, newValue, prevValue))
		existing.distanceMap.Set(sidx, newValue)
		newInfo = true
	}
	return newInfo
}

func (*SSSP) MailRetrieve(existing *Mail, _ *graph.Vertex[VertexProperty, EdgeProperty], _ *VertexProperty) Mail {
	// Atomically load the value of existing
	mail := make(map[uint32]Predecessor)
	for k, v := range existing.distanceMap.Content() {
		mail[k] = v
	}
	//existing.distanceMap.Clear()
	return Mail{distanceMap: NewConcurrentMapFromMap(mail)}
}

// Function called for a vertex update.
func (alg *SSSP) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	//log.Debug().Msg(fmt.Sprintf("Updating Verted %v with mail: %v", n.Target, m))
	//prevPropValue := prop.Predecessor
	//changed := false
	for prevId, newValue := range alg.MailRetrieve(&m, src, prop).distanceMap.Content() {
		//if !reflect.DeepEqual(prop.PrevDistanceMap[prevId], newValue) {
		//	prop.PrevDistanceMap[prevId] = newValue
		//	changed = true
		//}
		prop.PrevDistanceMap[prevId] = newValue
	}

	//if !changed {
	//	return 0
	//}

	prevDist := prop.Predecessor.TotalDistance
	newPropDistance := EMPTY_VAL
	newPrevVertex := ^uint32(0) // 0xFFFFFFFF

	for prevVertex, prevVertexPreds := range prop.PrevDistanceMap {
		if !prevVertexPreds.IsInPredecessor(n.Target) && prevVertexPreds.TotalDistance < newPropDistance {
			newPrevVertex = prevVertex
			newPropDistance = prevVertexPreds.TotalDistance
		}
	}

	log.Debug().Msg(fmt.Sprintf("Distance of %v(%v) changed to %v from %v - %v", n.Target, g.NodeVertexRawID(n.Target), newPropDistance, prevDist, prop.PrevDistanceMap))

	if newPrevVertex == ^uint32(0) {
		prop.Predecessor = NewEmptyPredecessor()
	} else {
		prop.Predecessor = prop.PrevDistanceMap[newPrevVertex]
	}

	// Send an update to all neighbours.
	for _, e := range src.OutEdges {
		mailbox, tidx := g.NodeVertexMailbox(e.Didx)
		newDist := NewEmptyPredecessor()
		if prop.Predecessor.TotalDistance < EMPTY_VAL {
			newDist = AddToPredecessor(prop.Predecessor, n.Target, e.Property.Weight)
		}
		if alg.MailMerge(Mail{distanceMap: NewConcurrentMapFromMap(map[uint32]Predecessor{n.Target: newDist})}, n.Target, &mailbox.Inbox) {
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
	if prop.Predecessor.TotalDistance < EMPTY_VAL { // Only useful if we are connected
		// Target only new edges.
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			target := src.OutEdges[eidx].Didx
			mailbox, tidx := g.NodeVertexMailbox(target)
			if alg.MailMerge(
				Mail{distanceMap: NewConcurrentMapFromMap(map[uint32]Predecessor{sidx: AddToPredecessor(prop.Predecessor, sidx, src.OutEdges[eidx].Property.Weight)})},
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
	//log.Debug().Msg(fmt.Sprintf("Called OnEdgeDel! %v - %v - %v", src.OutEdges, delEdges, m))
	for prevId, newValue := range alg.MailRetrieve(&m, src, prop).distanceMap.Content() {
		prop.PrevDistanceMap[prevId] = newValue
	}

	newPropDistance := EMPTY_VAL
	newPrevVertex := ^uint32(0) // 0xFFFFFFFF

	for prevVertex, prevVertexPreds := range prop.PrevDistanceMap {
		if !prevVertexPreds.IsInPredecessor(sidx) && prevVertexPreds.TotalDistance < newPropDistance {
			newPrevVertex = prevVertex
			newPropDistance = prevVertexPreds.TotalDistance
		}
	}

	if newPrevVertex == ^uint32(0) {
		prop.Predecessor = NewEmptyPredecessor()
	} else {
		prop.Predecessor = prop.PrevDistanceMap[newPrevVertex]
	}

	currentMinWeights := make(map[uint32]float64)
	for _, edge := range src.OutEdges { // find the min weight for edges between this vertex and neighbours
		if val, exists := currentMinWeights[edge.Didx]; !exists {
			currentMinWeights[edge.Didx] = val
		} else {
			currentMinWeights[edge.Didx] = min(currentMinWeights[edge.Didx], val)
		}
	}
	for _, delE := range delEdges {
		if g.NodeVertexRawID(delE.Didx) == 1 {
			continue
		}
		if currw, exists := currentMinWeights[delE.Didx]; !exists || currw > delE.Property.Weight {
			newDist := NewEmptyPredecessor()
			if exists {
				newDist = AddToPredecessor(prop.Predecessor, sidx, currw)
			}
			log.Debug().Msg(fmt.Sprintf("Sending mail to %v(%v) to update its distance %v", g.NodeVertexRawID(delE.Didx), delE.Didx, newDist))
			mailbox, tidx := g.NodeVertexMailbox(delE.Didx)
			if alg.MailMerge(
				Mail{distanceMap: NewConcurrentMapFromMap(map[uint32]Predecessor{sidx: newDist})},
				sidx, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: delE.Didx}, mailbox, tidx))
			}
		}
	}
	log.Debug().Msg(fmt.Sprintf("Completed OnEdgeDel %v", sidx))
	return sent
}
