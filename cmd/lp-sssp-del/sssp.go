package main

import (
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
	"math"
	"strconv"
	"sync/atomic"
)

type Phase int

const (
	Normal = iota
	SetAllToInfinity
)

type SSSP struct {
	Phase        Phase
	SourceVertex graph.RawType
}

const EmptyVal = math.MaxFloat64
const EmptyVertex = math.MaxUint32

type VertexProperty struct {
	Distance          float64
	PredecessorVertex graph.RawType
	IncomingVertices  map[graph.RawType]bool // Hashset
	SuccessorVertices map[graph.RawType]bool // Hashset
	MarkedAsInfinity  bool
}

type EdgeProperty struct {
	graph.WithWeight
	graph.NoTimestamp
	graph.NoRaw
}

type NotificationType int

const (
	EMPTY NotificationType = iota
	DistanceQuery
	DistanceUpdate
	SetToInfinity
	RemoveFromIncoming
	AddToSuccessor
	RemoveFromSuccessor
)

func (n *NotificationType) toString() string {
	switch *n {
	case EMPTY:
		return "EMPTY"
	case DistanceQuery:
		return "DistanceQuery"
	case DistanceUpdate:
		return "DistanceUpdate"
	case SetToInfinity:
		return "SetToInfinity"
	case RemoveFromIncoming:
		return "RemoveFromIncoming"
	case AddToSuccessor:
		return "AddToSuccessor"
	case RemoveFromSuccessor:
		return "RemoveFromSuccessor"
	}
	return "Unknown"
}

type Mail struct{}

type Note struct {
	Type     NotificationType
	Sender   uint32
	Distance float64 // For DistanceUpdate
}

type Graph = graph.Graph[VertexProperty, EdgeProperty, Mail, Note]
type GraphThread = graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]
type Vertex = graph.Vertex[VertexProperty, EdgeProperty]
type Edge = graph.Edge[EdgeProperty]

func (VertexProperty) New() VertexProperty {
	return VertexProperty{Distance: EmptyVal, PredecessorVertex: EmptyVertex, IncomingVertices: make(map[graph.RawType]bool), SuccessorVertices: make(map[graph.RawType]bool), MarkedAsInfinity: false}
}

func (Mail) New() Mail {
	return Mail{}
}

func (*SSSP) MailMerge(incoming Mail, _ uint32, existing *Mail) (newInfo bool) { return true }

func (*SSSP) MailRetrieve(existing *Mail, _ *Vertex, _ *VertexProperty) Mail {
	return Mail{}
}

func Run(options graph.GraphOptions, sourceInit *string) (alg *SSSP, g *Graph) {
	// Create Alg
	alg = new(SSSP)
	alg.SourceVertex = graph.AsRawTypeString(*sourceInit)

	// Create Graph
	g = new(Graph)
	g.Options = options

	return alg, g
}

func (*SSSP) InitAllNote(_ *Vertex, _ *VertexProperty, _ uint32, _ graph.RawType) (initialNote Note) {
	return Note{Type: EMPTY}
}

func (alg *SSSP) BaseVertexMailbox(v *Vertex, vp *VertexProperty, internalId uint32, s *graph.VertexStructure) (m Mail) {
	if s.RawId == alg.SourceVertex {
		vp.Distance = 0
	} else {
		vp.Distance = EmptyVal
	}
	return m
}

func triggerUpdateDistance(g *Graph, vp *VertexProperty, vertexRawId graph.RawType) (sent uint64) {
	vertexId, _ := g.NodeVertexFromRaw(vertexRawId)
	for incomingRawId, _ := range vp.IncomingVertices {
		incomingId, _ := g.NodeVertexFromRaw(incomingRawId)
		notificationToOldParent := graph.Notification[Note]{
			Target: incomingId,
			Note:   Note{Type: DistanceQuery, Sender: vertexId},
		}
		incomingMailbox, incomingIdx := g.NodeVertexMailbox(incomingId)
		sent += g.EnsureSend(g.ActiveNotification(vertexId, notificationToOldParent, incomingMailbox, incomingIdx))
	}
	return sent
}

func setAllToInfinityFinished(g *Graph) (sent uint64) {

	log.Debug().Msg("setAllToInfinityFinished called")
	g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) (accumulated int) {
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			vp := gt.VertexProperty(i)
			if !vp.MarkedAsInfinity {
				continue
			}
			atomic.AddUint64(&sent, triggerUpdateDistance(g, vp, gt.VertexRawID(i)))
			vp.MarkedAsInfinity = false
		}
		return 0
	})
	return sent
}

func (alg *SSSP) OnSuperStepConverged(g *Graph) (sent uint64) {
	log.Debug().Msg("OnSuperStepConverged called")
	switch alg.Phase {
	case Normal:
		return 0
	case SetAllToInfinity:
		sent += setAllToInfinityFinished(g)
		alg.Phase = Normal
	}
	return sent
}

func onDistanceUpdate(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	currentVertex := n.Target
	log.Debug().Msg("onDistanceUpdate: " + g.NodeVertexRawID(n.Target).String() + " from " + g.NodeVertexRawID(n.Note.Sender).String())
	prop.IncomingVertices[g.NodeVertexRawID(n.Note.Sender)] = true // add the sender to the incoming vertices
	if prop.Distance > n.Note.Distance {
		prop.Distance = n.Note.Distance

		// Tell current predecessor to remove use from its successor list
		if prop.PredecessorVertex != EmptyVertex {
			oldParent, _ := g.NodeVertexFromRaw(prop.PredecessorVertex)
			notificationToOldParent := graph.Notification[Note]{
				Target: oldParent,
				Note:   Note{Type: RemoveFromSuccessor, Sender: currentVertex},
			}
			oldParentMailbox, oldParentIdx := g.NodeVertexMailbox(oldParent)
			sent += g.EnsureSend(g.ActiveNotification(currentVertex, notificationToOldParent, oldParentMailbox, oldParentIdx))
		}

		// Update the predecessor vertex
		prop.PredecessorVertex = g.NodeVertexRawID(n.Note.Sender)
		notificationToNewParent := graph.Notification[Note]{
			Target: n.Note.Sender,
			Note:   Note{Type: AddToSuccessor, Sender: currentVertex},
		}
		senderMailbox, senderIdx := g.NodeVertexMailbox(n.Note.Sender)
		sent += g.EnsureSend(g.ActiveNotification(currentVertex, notificationToNewParent, senderMailbox, senderIdx))

		for _, edge := range src.OutEdges {
			mailbox, tidx := g.NodeVertexMailbox(edge.Didx)
			notification := graph.Notification[Note]{
				Target: edge.Didx, Note: Note{Type: DistanceUpdate, Sender: currentVertex, Distance: prop.Distance + edge.Property.Weight},
			}
			sent += g.EnsureSend(g.ActiveNotification(currentVertex, notification, mailbox, tidx))
		}
	}
	return sent
}

func onAddToSuccessor(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	log.Debug().Msg(g.NodeVertexRawID(n.Target).String() + " Received AddToSuccessor: " + g.NodeVertexRawID(n.Note.Sender).String())
	prop.SuccessorVertices[g.NodeVertexRawID(n.Note.Sender)] = true
	return sent
}

func onRemoveFromSuccessor(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	log.Debug().Msg(g.NodeVertexRawID(n.Target).String() + " Received RemoveFromSuccessor: " + g.NodeVertexRawID(n.Note.Sender).String())
	delete(prop.SuccessorVertices, g.NodeVertexRawID(n.Note.Sender))
	return sent
}

func onSetToInfinity(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	log.Debug().Msg("onSetToInfinity: " + g.NodeVertexRawID(n.Target).String() + " from " + g.NodeVertexRawID(n.Note.Sender).String())
	prop.MarkedAsInfinity = true
	prop.Distance = EmptyVal
	prop.PredecessorVertex = EmptyVertex
	for successor, _ := range prop.SuccessorVertices {
		successorId, _ := g.NodeVertexFromRaw(successor)
		mailbox, successorIdx := g.NodeVertexMailbox(successorId)
		notification := graph.Notification[Note]{
			Target: successorId, Note: Note{Type: SetToInfinity, Sender: n.Target},
		}
		sent += g.EnsureSend(g.ActiveNotification(n.Target, notification, mailbox, successorIdx))
	}
	prop.SuccessorVertices = make(map[graph.RawType]bool)
	return sent
}

func onDistanceQuery(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	log.Debug().Msg("onDistanceQuery: " + g.NodeVertexRawID(n.Target).String() + " from " + g.NodeVertexRawID(n.Note.Sender).String())
	for _, edge := range src.OutEdges {
		if edge.Didx == n.Note.Sender {
			mailbox, senderIdx := g.NodeVertexMailbox(n.Note.Sender)
			notification := graph.Notification[Note]{
				Target: n.Note.Sender, Note: Note{Type: DistanceUpdate, Sender: n.Target, Distance: prop.Distance + edge.Property.Weight},
			}
			sent += g.EnsureSend(g.ActiveNotification(n.Target, notification, mailbox, senderIdx))
		}
	}
	if sent == 0 {
		log.Warn().Msg("Could not find edge to sender.")
	}
	return sent
}

func onRemoveFromIncoming(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) uint64 {
	log.Debug().Msg("onRemoveFromIncoming: " + g.NodeVertexRawID(n.Target).String() + " from " + g.NodeVertexRawID(n.Note.Sender).String())
	delete(prop.IncomingVertices, g.NodeVertexRawID(n.Note.Sender))
	return 0
}

// Function called for a vertex update.
func (alg *SSSP) OnUpdateVertex(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	log.Debug().Msg("OnUpdateVertex: " + g.NodeVertexRawID(n.Target).String() + " " + n.Note.Type.toString())
	switch n.Note.Type {
	case DistanceUpdate:
		return onDistanceUpdate(g, gt, src, prop, n, m)
	case AddToSuccessor:
		return onAddToSuccessor(g, gt, src, prop, n, m)
	case RemoveFromSuccessor:
		return onRemoveFromSuccessor(g, gt, src, prop, n, m)
	case SetToInfinity:
		return onSetToInfinity(g, gt, src, prop, n, m)
	case DistanceQuery:
		return onDistanceQuery(g, gt, src, prop, n, m)
	case RemoveFromIncoming:
		return onRemoveFromIncoming(g, gt, src, prop, n, m)
	case EMPTY:
		return 0
	default:
		log.Warn().Msg(string("Unexpected notification type: " + n.Note.Type.toString() + " " + strconv.Itoa(int(n.Note.Type))))
	}
	return sent
}

// OnEdgeAdd: Function called upon a new edge add (which also bundles a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: eidxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func (alg *SSSP) OnEdgeAdd(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
		edge := src.OutEdges[eidx]
		log.Debug().Msg("onEdgeAdd: " + g.NodeVertexRawID(sidx).String() + "->" + g.NodeVertexRawID(edge.Didx).String())
		mailbox, tidx := g.NodeVertexMailbox(edge.Didx)
		notification := graph.Notification[Note]{
			Target: edge.Didx, Note: Note{Type: DistanceUpdate, Sender: sidx, Distance: prop.Distance + edge.Property.Weight},
		}
		sent += g.EnsureSend(g.ActiveNotification(sidx, notification, mailbox, tidx))
	}
	return sent
}

// Not used in this algorithm.
func (alg *SSSP) OnEdgeDel(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, sidx uint32, delEdges []Edge, mail Mail) (sent uint64) {
	log.Debug().Msg("onEdgeDel: " + g.NodeVertexRawID(sidx).String())
	for _, deletedEdge := range delEdges {
		log.Debug().Msg("onEdgeDel: " + g.NodeVertexRawID(sidx).String() + "->" + g.NodeVertexRawID(deletedEdge.Didx).String())
		mailbox, tidx := g.NodeVertexMailbox(deletedEdge.Didx)
		notification := graph.Notification[Note]{
			Target: deletedEdge.Didx, Note: Note{Type: RemoveFromIncoming, Sender: sidx},
		}
		sent += g.EnsureSend(g.ActiveNotification(sidx, notification, mailbox, tidx))
		isSourceOnShortestPath, _ := prop.SuccessorVertices[g.NodeVertexRawID(deletedEdge.Didx)]
		if isSourceOnShortestPath {
			delete(prop.SuccessorVertices, g.NodeVertexRawID(deletedEdge.Didx))
			notification = graph.Notification[Note]{
				Target: deletedEdge.Didx, Note: Note{Type: SetToInfinity, Sender: sidx},
			}
			sent += g.EnsureSend(g.ActiveNotification(sidx, notification, mailbox, tidx))
			alg.Phase = SetAllToInfinity
		}
	}
	return sent
}
