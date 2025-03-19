package main

import (
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
	"math"
	"strconv"
	"sync/atomic"
)

const (
	Normal           = int32(0)
	SetAllToInfinity = int32(1)
)

type SSSP struct {
	Phase        int32
	SourceVertex graph.RawType
}

const EmptyVal = math.MaxFloat64
const EmptyVertex = math.MaxUint32

type VertexProperty struct {
	Distance             float64
	PredecessorVertex    uint32
	IncomingVertices     map[uint32]bool // Hashset
	SuccessorVertices    map[uint32]bool // Hashset
	MarkedAsInfinity     bool
	MarkedAsInfinityTime uint64
	InboxHistory         []Note
	OutboxHistory        []graph.Notification[Note]
}

type EdgeProperty struct {
	graph.WithWeight
	graph.WithTimestamp
	graph.NoRaw
}

func (ep *EdgeProperty) ParseProperty(fields []string, _ int32, tPos int32) {
	ts, _ := strconv.Atoi(fields[tPos])
	ep.Ts = uint64(ts)
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
	Ts       uint64
}

type Graph = graph.Graph[VertexProperty, EdgeProperty, Mail, Note]
type GraphThread = graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]
type Vertex = graph.Vertex[VertexProperty, EdgeProperty]
type Edge = graph.Edge[EdgeProperty]

func (VertexProperty) New() VertexProperty {
	return VertexProperty{Distance: EmptyVal, PredecessorVertex: EmptyVertex, IncomingVertices: make(map[uint32]bool), SuccessorVertices: make(map[uint32]bool), MarkedAsInfinity: false, InboxHistory: make([]Note, 0), OutboxHistory: make([]graph.Notification[Note], 0)}
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

func (*SSSP) InitAllNote(_ *Vertex, _ *VertexProperty, _ uint32, _ uint32) (initialNote Note) {
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

func triggerUpdateDistance(g *Graph, vp *VertexProperty, vertexId uint32, ts uint64) (sent uint64) {
	for incomingId, _ := range vp.IncomingVertices {
		notificationToOldParent := graph.Notification[Note]{
			Target: incomingId,
			Note:   Note{Type: DistanceQuery, Sender: vertexId, Ts: ts},
		}
		incomingMailbox, incomingIdx := g.NodeVertexMailbox(incomingId)
		sent += g.EnsureSend(g.ActiveNotification(vertexId, notificationToOldParent, incomingMailbox, incomingIdx))
		vp.OutboxHistory = append(vp.OutboxHistory, notificationToOldParent)
	}
	return sent
}

func setAllToInfinityFinished(g *Graph) (sent uint64) {

	//log.Debug().Msg("setAllToInfinityFinished called")
	g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) (accumulated int) {
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			vp := gt.VertexProperty(i)
			if !vp.MarkedAsInfinity {
				continue
			}
			atomic.AddUint64(&sent, triggerUpdateDistance(g, vp, threadOffset|i, vp.MarkedAsInfinityTime))
			vp.MarkedAsInfinity = false
			vp.MarkedAsInfinityTime = 0
		}
		return 0
	})
	return sent
}

func (alg *SSSP) OnSuperStepConverged(g *Graph) (sent uint64) {
	//log.Debug().Msg("OnSuperStepConverged called")
	switch alg.Phase {
	case Normal:
		//alg.OnCheckCorrectness(g)

		sent = 0
	case SetAllToInfinity:
		sent += setAllToInfinityFinished(g)
		alg.Phase = Normal
	}
	return sent
}

func onDistanceUpdate(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	currentVertex := n.Target
	//log.Debug().Msg("onDistanceUpdate: " + g.NodeVertexRawID(n.Target).String() + " from " + g.NodeVertexRawID(n.Note.Sender).String())
	prop.IncomingVertices[n.Note.Sender] = true // add the sender to the incoming vertices
	if prop.Distance > n.Note.Distance {
		prop.Distance = n.Note.Distance

		// Tell current predecessor to remove use from its successor list
		if prop.PredecessorVertex != EmptyVertex {
			notificationToOldParent := graph.Notification[Note]{
				Target: prop.PredecessorVertex,
				Note:   Note{Type: RemoveFromSuccessor, Sender: currentVertex, Ts: n.Note.Ts},
			}
			oldParentMailbox, oldParentIdx := g.NodeVertexMailbox(prop.PredecessorVertex)
			sent += g.EnsureSend(g.ActiveNotification(currentVertex, notificationToOldParent, oldParentMailbox, oldParentIdx))
			prop.OutboxHistory = append(prop.OutboxHistory, notificationToOldParent)
		}

		// Update the predecessor vertex
		prop.PredecessorVertex = n.Note.Sender
		notificationToNewParent := graph.Notification[Note]{
			Target: n.Note.Sender,
			Note:   Note{Type: AddToSuccessor, Sender: currentVertex, Ts: n.Note.Ts},
		}
		senderMailbox, senderIdx := g.NodeVertexMailbox(n.Note.Sender)
		sent += g.EnsureSend(g.ActiveNotification(currentVertex, notificationToNewParent, senderMailbox, senderIdx))
		prop.OutboxHistory = append(prop.OutboxHistory, notificationToNewParent)

		for _, edge := range src.OutEdges {
			mailbox, tidx := g.NodeVertexMailbox(edge.Didx)
			notification := graph.Notification[Note]{
				Target: edge.Didx, Note: Note{Type: DistanceUpdate, Sender: currentVertex, Distance: prop.Distance + edge.Property.Weight, Ts: n.Note.Ts},
			}
			sent += g.EnsureSend(g.ActiveNotification(currentVertex, notification, mailbox, tidx))
			prop.OutboxHistory = append(prop.OutboxHistory, notification)
		}
	}
	return sent
}

func onAddToSuccessor(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	//log.Debug().Msg(g.NodeVertexRawID(n.Target).String() + " Received AddToSuccessor: " + g.NodeVertexRawID(n.Note.Sender).String())
	prop.SuccessorVertices[n.Note.Sender] = true
	return sent
}

func onRemoveFromSuccessor(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	//log.Debug().Msg(g.NodeVertexRawID(n.Target).String() + " Received RemoveFromSuccessor: " + g.NodeVertexRawID(n.Note.Sender).String())
	delete(prop.SuccessorVertices, n.Note.Sender)
	return sent
}

func onSetToInfinity(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	//log.Debug().Msg("onSetToInfinity: " + g.NodeVertexRawID(n.Target).String() + " from " + g.NodeVertexRawID(n.Note.Sender).String())
	prop.MarkedAsInfinity = true
	prop.Distance = EmptyVal
	prop.PredecessorVertex = EmptyVertex
	for successor, _ := range prop.SuccessorVertices {
		mailbox, successorIdx := g.NodeVertexMailbox(successor)
		notification := graph.Notification[Note]{
			Target: successor, Note: Note{Type: SetToInfinity, Sender: n.Target, Ts: n.Note.Ts},
		}
		sent += g.EnsureSend(g.ActiveNotification(n.Target, notification, mailbox, successorIdx))
		prop.OutboxHistory = append(prop.OutboxHistory, notification)
	}
	prop.SuccessorVertices = make(map[uint32]bool)
	return sent
}

func onDistanceQuery(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	// log.Debug().Msg("onDistanceQuery: " + g.NodeVertexRawID(n.Target).String() + " from " + g.NodeVertexRawID(n.Note.Sender).String())
	for _, edge := range src.OutEdges {
		if edge.Didx == n.Note.Sender {
			mailbox, senderIdx := g.NodeVertexMailbox(n.Note.Sender)
			notification := graph.Notification[Note]{
				Target: n.Note.Sender, Note: Note{Type: DistanceUpdate, Sender: n.Target, Distance: prop.Distance + edge.Property.Weight, Ts: n.Note.Ts},
			}
			sent += g.EnsureSend(g.ActiveNotification(n.Target, notification, mailbox, senderIdx))
			prop.OutboxHistory = append(prop.OutboxHistory, notification)
		}
	}
	if sent == 0 {
		log.Warn().Msg("Could not find edge to sender.")
	}
	return sent
}

func onRemoveFromIncoming(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) uint64 {
	//log.Debug().Msg("onRemoveFromIncoming: " + g.NodeVertexRawID(n.Target).String() + " from " + g.NodeVertexRawID(n.Note.Sender).String())
	delete(prop.IncomingVertices, n.Note.Sender)
	return 0
}

// Function called for a vertex update.
func (alg *SSSP) OnUpdateVertex(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	//log.Debug().Msg("OnUpdateVertex: " + g.NodeVertexRawID(n.Target).String() + " " + n.Note.Type.toString())
	prop.InboxHistory = append(prop.InboxHistory, n.Note)
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
		log.Warn().Msg("Unexpected notification type: " + n.Note.Type.toString() + " " + strconv.Itoa(int(n.Note.Type)))
	}
	return sent
}

// OnEdgeAdd: Function called upon a new edge add (which also bundles a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: eidxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func (alg *SSSP) OnEdgeAdd(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
		edge := src.OutEdges[eidx]
		//log.Debug().Msg("onEdgeAdd: " + g.NodeVertexRawID(sidx).String() + "->" + g.NodeVertexRawID(edge.Didx).String())
		mailbox, tidx := g.NodeVertexMailbox(edge.Didx)
		notification := graph.Notification[Note]{
			Target: edge.Didx, Note: Note{Type: DistanceUpdate, Sender: sidx, Distance: prop.Distance + edge.Property.Weight, Ts: edge.Property.Ts},
		}
		sent += g.EnsureSend(g.ActiveNotification(sidx, notification, mailbox, tidx))
		prop.OutboxHistory = append(prop.OutboxHistory, notification)
	}
	return sent
}

// Not used in this algorithm.
func (alg *SSSP) OnEdgeDel(g *Graph, gt *GraphThread, src *Vertex, prop *VertexProperty, sidx uint32, delEdges []Edge, mail Mail) (sent uint64) {
	//log.Debug().Msg("onEdgeDel: " + g.NodeVertexRawID(sidx).String())
	for _, deletedEdge := range delEdges {
		//log.Debug().Msg("onEdgeDel: " + g.NodeVertexRawID(sidx).String() + "->" + g.NodeVertexRawID(deletedEdge.Didx).String())
		mailbox, tidx := g.NodeVertexMailbox(deletedEdge.Didx)
		notification := graph.Notification[Note]{
			Target: deletedEdge.Didx, Note: Note{Type: RemoveFromIncoming, Sender: sidx, Ts: deletedEdge.Property.Ts},
		}
		sent += g.EnsureSend(g.ActiveNotification(sidx, notification, mailbox, tidx))
		prop.OutboxHistory = append(prop.OutboxHistory, notification)
		isSourceOnShortestPath, _ := prop.SuccessorVertices[deletedEdge.Didx]
		if isSourceOnShortestPath {
			delete(prop.SuccessorVertices, deletedEdge.Didx)
			notification = graph.Notification[Note]{
				Target: deletedEdge.Didx, Note: Note{Type: SetToInfinity, Sender: sidx, Ts: deletedEdge.Property.Ts},
			}
			sent += g.EnsureSend(g.ActiveNotification(sidx, notification, mailbox, tidx))
			prop.OutboxHistory = append(prop.OutboxHistory, notification)
			atomic.SwapInt32(&alg.Phase, SetAllToInfinity)
		}
	}
	return sent
}
