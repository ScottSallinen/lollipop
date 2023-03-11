package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"math"
)

type VertexType uint8
type MessageType uint8
type Nbr struct {
	Height int64
	ResCap int64
}

type VertexProp struct {
	MessageBuffer []Message

	Type   VertexType
	Excess int64
	Height int64
	Nbrs   map[uint32]Nbr
}

type Message struct {
	Type   MessageType
	Source uint32
	Height int64
	Value  int64
}

type EdgeProp struct {
	Capacity  uint32
	Timestamp uint64
}

type MessageValue []Message

type Framework = framework.Framework[VertexProp, EdgeProp, MessageValue]
type Graph = graph.Graph[VertexProp, EdgeProp, MessageValue]
type Vertex = graph.Vertex[VertexProp, EdgeProp]
type Edge = graph.Edge[EdgeProp]

const (
	EmptyValue    = 0
	InitialHeight = math.MaxUint32

	Normal VertexType = 0
	Source VertexType = 1
	Sink   VertexType = 2

	Unspecified       MessageType = 0
	Init              MessageType = 1
	Flow              MessageType = 2
	ZeroFlow          MessageType = 3
	NewMaxVertexCount MessageType = 4
	MessageTypesCount             = 5
)

var resetPhase = false
var bfsPhase = false
var MessageCounter = make([]uint64, MessageTypesCount) // FIXME: race condition

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
	case Init:
		return "Init"
	case Flow:
		return "Flow"
	case ZeroFlow:
		return "ZeroFlow"
	case NewMaxVertexCount:
		return "NewMaxVertexCount"
	default:
		return fmt.Sprintf("%d", t)
	}
}

func (n Nbr) String() string {
	return fmt.Sprintf("{%d,%d}", n.Height, n.ResCap)
}

func (p *VertexProp) String() string {
	s := fmt.Sprintf("{%v,%v,%v,[", p.Type, p.Excess, p.Height)
	for k, v := range p.Nbrs {
		s += fmt.Sprintf("%d:%v,", k, v)
	}
	return s + "]}"
}

func (e EdgeProp) String() string {
	return fmt.Sprintf("{%d,%d}", e.Capacity, e.Timestamp)
}

func GetTimestamp(e EdgeProp) uint64 {
	return e.Timestamp
}

func SetTimestamp(e *EdgeProp, ts uint64) {
	e.Timestamp = ts
}

func CountMessage(m *Message) {
	if m.Type == Flow && m.Value == 0 {
		MessageCounter[ZeroFlow] += 1
	} else {
		MessageCounter[m.Type] += 1
	}
}

func ResetMessageCounts() {
	for i := uint32(0); i < uint32(len(MessageCounter)); i++ {
		MessageCounter[i] = 0
	}
}

func PrintMessageCounts() {
	for i := uint32(0); i < uint32(len(MessageCounter)); i++ {
		info(fmt.Sprintf("%17v: %v", MessageType(i), MessageCounter[i]))
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
	if graph.DEBUG {
		if m.Value != 0 {
			info(m.String(g, v, vertexIndex))
		}
	}
}
