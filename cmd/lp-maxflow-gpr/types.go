package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"math"
)

type VertexType uint8
type MessageType uint8
type Message struct {
	Source uint32
	Type   MessageType
	Height uint32 // TODO: maybe there is no need to carry both height and value in a message
	Value  uint32
}
type Neighbour struct {
	Height           uint32
	ResidualCapacity uint32
}

type VertexProperty struct {
	MessageBuffer []Message

	Type       VertexType
	Excess     uint32
	Height     uint32
	InitHeight uint32 // Or distance to sink, set in breadth first search way, or 0 in GPR

	Neighbours map[uint32]Neighbour
}
type EdgeProperty struct {
	Capacity uint32
}
type MessageValue []Message

type Graph = graph.Graph[VertexProperty, EdgeProperty, MessageValue]
type Vertex = graph.Vertex[VertexProperty, EdgeProperty]
type Edge = graph.Edge[EdgeProperty]

const (
	PrintNewHeight bool = false

	Normal VertexType = 0
	Source VertexType = 1
	Sink   VertexType = 2

	MessageTypes             = 9
	Unspecified  MessageType = 0
	InitSource   MessageType = 1
	InitSink     MessageType = 2
	InitHeight   MessageType = 3 // TODO: implement 2PPR instead of GPR
	NewHeight    MessageType = 4
	PushRequest  MessageType = 5
	RejectPush   MessageType = 6
	Increasing   MessageType = 7
	Decreasing   MessageType = 8
)

var MessageCounter = make([]uint32, MessageTypes)

func (t VertexType) String() string {
	switch t {
	case Normal:
		return "Normal"
	case Source:
		return "Source"
	case Sink:
		return "Sink"
	default:
		return fmt.Sprintf("%d", t)
	}
}

func (t MessageType) String() string {
	switch t {
	case Unspecified:
		return "Unspecified"
	case InitSource:
		return "InitSource"
	case InitSink:
		return "InitSink"
	case InitHeight:
		return "InitHeight"
	case NewHeight:
		return "NewHeight"
	case PushRequest:
		return "PushRequest"
	case RejectPush:
		return "RejectPush"
	case Increasing:
		return "Increasing"
	case Decreasing:
		return "Decreasing"
	default:
		return fmt.Sprintf("%d", t)
	}
}

func (n Neighbour) String() string {
	return fmt.Sprintf("{%d,%d}", n.Height, n.ResidualCapacity)
}

func (p *VertexProperty) String() string {
	s := fmt.Sprintf("{%v,%v,%v,%v,[", p.Type, p.Excess, p.Height, p.InitHeight)
	for k, v := range p.Neighbours {
		s += fmt.Sprintf("%d:%v,", k, v)
	}
	return s + "]}"
}

func CountMessage(m *Message) {
	MessageCounter[m.Type] += 1
}

func ResetMessageCounts() {
	for i := uint32(0); i < uint32(len(MessageCounter)); i++ {
		MessageCounter[i] = 0
	}
}

func PrintMessageCounts() {
	for i := uint32(0); i < uint32(len(MessageCounter)); i++ {
		info(fmt.Sprintf("%11v: %v", MessageType(i), MessageCounter[i]))
	}
}

func (m *Message) String(g *Graph, v *Vertex, vertexIndex uint32) string {
	if m.Source != math.MaxUint32 {
		return fmt.Sprintf("OnVisitVertex RawID=%v->%v Index=%v->%v: m.Type=%v m.Height=%v m.Value=%v", g.Vertices[m.Source].Id, v.Id, m.Source, vertexIndex, m.Type, m.Height, m.Value)
	} else {
		return fmt.Sprintf("OnVisitVertex RawID=non->%v Index=none->%v: m.Type=%v m.Height=%v m.Value=%v", v.Id, vertexIndex, m.Type, m.Height, m.Value)
	}
}

func (m *Message) PrintIfNeeded(g *Graph, v *Vertex, vertexIndex uint32) {
	if true {
		if m.Type != NewHeight || PrintNewHeight {
			info(m.String(g, v, vertexIndex))
		}
	}
}
