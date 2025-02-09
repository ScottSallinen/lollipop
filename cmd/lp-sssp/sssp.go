package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
	"math"
	"reflect"
	"sync"
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

const (
	ADD graph.EventType = iota // Implicit, 0, means add.
	DEL
)

type SafeMail struct {
	mu          sync.RWMutex
	distanceMap map[uint32]Predecessor
	trigger     graph.EventType
}

func NewSafeMail() *SafeMail {
	return &SafeMail{
		mu:          sync.RWMutex{},
		distanceMap: make(map[uint32]Predecessor),
		trigger:     ADD,
	}
}

func SourceSafeMail(s string) *SafeMail {
	return &SafeMail{
		mu:          sync.RWMutex{},
		distanceMap: map[uint32]Predecessor{math.MaxUint32: {TotalDistance: 0}},
		trigger:     ADD,
	}
}

func NewSafeMailFromEvent(dm map[uint32]Predecessor, ev graph.EventType) *SafeMail {
	newMap := make(map[uint32]Predecessor)
	for k, v := range dm {
		newMap[k] = v
	}
	return &SafeMail{
		mu:          sync.RWMutex{},
		distanceMap: newMap,
		trigger:     ev,
	}
}

func CopySafeMail(prev *SafeMail) *SafeMail {
	newMap := make(map[uint32]Predecessor)
	prev.mu.RLock()
	trigger := prev.trigger
	for k, v := range prev.distanceMap {
		newMap[k] = v
	}
	prev.mu.RUnlock()
	return &SafeMail{
		mu:          sync.RWMutex{},
		distanceMap: newMap,
		trigger:     trigger,
	}
}

// Set adds or updates a key-value pair
func (sm *SafeMail) Set(key uint32, value Predecessor) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.distanceMap[key] = value
}

// Get retrieves a value safely
func (sm *SafeMail) Get(key uint32) (Predecessor, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	val, exists := sm.distanceMap[key]
	return val, exists
}

func (sm *SafeMail) Update(newSm *SafeMail, sidx uint32) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var newInfo bool
	prevValue, keyExists := sm.distanceMap[sidx]
	newValue, newExists := newSm.distanceMap[sidx]
	if keyExists && newExists && prevValue.TotalDistance == newValue.TotalDistance && reflect.DeepEqual(prevValue.PrevList, newValue.PrevList) {
		newInfo = false
	} else {
		log.Debug().Msg(fmt.Sprintf("Log merged %v: %v vs %v", sidx, newValue, prevValue))
		sm.distanceMap[sidx] = newValue
		newInfo = true
	}
	sm.trigger = newSm.trigger
	return newInfo
}

type Mail struct {
	inner *SafeMail
}

type Note struct{}

func (VertexProperty) New() VertexProperty {
	return VertexProperty{NewEmptyPredecessor(), MapVertexDistance{}}
}

func (Mail) New() Mail {
	return Mail{NewSafeMail()}
}

func (*SSSP) MailMerge(incoming Mail, sidx uint32, existing *Mail) (newInfo bool) {
	return existing.inner.Update(incoming.inner, sidx)
	//prevValue, keyExists := existing.inner.Get(sidx)
	//newValue, newExists := incoming.inner.Get(sidx)
	//if keyExists && newExists && prevValue.TotalDistance == newValue.TotalDistance && reflect.DeepEqual(prevValue.PrevList, newValue.PrevList) {
	//	newInfo = false
	//} else {
	//	log.Debug().Msg(fmt.Sprintf("Log merged %v: %v vs %v", sidx, newValue, prevValue))
	//	existing.inner.Set(sidx, newValue)
	//	newInfo = true
	//}
	//return newInfo
}

func (*SSSP) MailRetrieve(existing *Mail, _ *graph.Vertex[VertexProperty, EdgeProperty], _ *VertexProperty) Mail {
	// Atomically load the value of existing
	return Mail{inner: CopySafeMail(existing.inner)}
}

// Function called for a vertex update.
func (alg *SSSP) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	//log.Debug().Msg(fmt.Sprintf("Updating Vertex %v with mail: %v", n.Target, m))
	//prevPropValue := prop.Predecessor
	//changed := false
	retreived := alg.MailRetrieve(&m, src, prop)
	for prevId, newValue := range retreived.inner.distanceMap {
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
		if alg.MailMerge(Mail{inner: NewSafeMailFromEvent(map[uint32]Predecessor{n.Target: newDist}, retreived.inner.trigger)}, n.Target, &mailbox.Inbox) {
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
				Mail{inner: NewSafeMailFromEvent(map[uint32]Predecessor{sidx: AddToPredecessor(prop.Predecessor, sidx, src.OutEdges[eidx].Property.Weight)}, ADD)},
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
	retrieved := alg.MailRetrieve(&m, src, prop)
	for prevId, newValue := range retrieved.inner.distanceMap {
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
				Mail{inner: NewSafeMailFromEvent(map[uint32]Predecessor{sidx: newDist}, DEL)},
				sidx, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: delE.Didx}, mailbox, tidx))
			}
		}
	}
	log.Debug().Msg(fmt.Sprintf("Completed OnEdgeDel %v", sidx))
	return sent
}
