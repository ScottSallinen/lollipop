package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
	"math"
	"sync/atomic"
)

// TODO: handle self loop

type MessageType uint8
type VertexType uint8

const (
	Normal VertexType = 0
	Source VertexType = 1
	Sink   VertexType = 2

	Unspecified MessageType = 0
	InitSource  MessageType = 1
	InitSink    MessageType = 2
	InitHeight  MessageType = 3 // TODO: implement 2PPR instead of GPR
	NewHeight   MessageType = 4
	PushRequest MessageType = 5 // (PUSH-REQUEST-ANS , δ, NOK)
	RejectPush  MessageType = 6 // (PUSH-REQUEST-ANS , δ, NOK)
)

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

type EdgeProperty struct {
	Capacity uint32
}

type Message struct {
	Source uint32
	Type   MessageType
	Height uint32 // TODO: maybe there is no need to carry both height and value in a message
	Value  uint32 // flow?
}

type MessageValue []Message

func initPush(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) int {
	v := &g.Vertices[vidx]
	enforce.ENFORCE(v.Property.Type == Source)
	excess := uint32(0)
	for i := range v.OutEdges {
		edge := &v.OutEdges[i]
		excess += edge.Property.Capacity
	}
	v.Property.Excess = excess
	return push(g, vidx)
}

func discharge(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) int {
	v := &g.Vertices[vidx]
	if v.Property.Excess == 0 || v.Property.Type == Sink {
		return 0
	}
	// TODO: it might be more efficient if we combine lift and push and use a heap, new_height and push_request can be combined as well
	nMessages := push(g, vidx)
	if v.Property.Type != Normal {
		return nMessages
	}
	for v.Property.Excess > 0 {
		nMessages += lift(g, vidx)
		nMessages += push(g, vidx)
	}
	return nMessages
}

func push(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) int {
	v := &g.Vertices[vidx]
	if v.Property.Type == Sink || v.Property.Excess == 0 {
		return 0
	}
	nMessages := 0
	for neighbourIndex, neighbourProperty := range v.Property.Neighbours {
		// TODO: what happens if we prioritize vertices with lower height?
		if v.Property.Height > neighbourProperty.Height && neighbourProperty.ResidualCapacity > 0 {
			additionalFlow := mathutils.Min(neighbourProperty.ResidualCapacity, v.Property.Excess)
			v.Property.Excess -= additionalFlow
			neighbourProperty.ResidualCapacity -= additionalFlow
			v.Property.Neighbours[neighbourIndex] = neighbourProperty

			g.OnQueueVisit(g, vidx, neighbourIndex, []Message{{Source: vidx, Type: PushRequest, Height: v.Property.Height, Value: additionalFlow}})
			nMessages += 1
		}
		if v.Property.Excess == 0 {
			break
		}
	}
	return nMessages
}

func lift(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) int {
	v := &g.Vertices[vidx]
	enforce.ENFORCE(v.Property.Type == Normal)
	// TODO: this is different from the one proposed by Pham et al
	minHeight := uint32(math.MaxUint32)
	for _, neighbourProperty := range v.Property.Neighbours {
		if neighbourProperty.ResidualCapacity > 0 && neighbourProperty.Height < minHeight {
			minHeight = neighbourProperty.Height
		}
	}
	v.Property.Height = minHeight + 1
	// TODO send new height
	for neighbourIndex := range v.Property.Neighbours {
		g.OnQueueVisit(g, vidx, neighbourIndex, []Message{{Source: vidx, Type: NewHeight, Height: v.Property.Height}})
	}
	return len(v.Property.Neighbours)
}

func onPushRequest(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx, source, height, flow uint32) int {
	v := &g.Vertices[vidx]
	if height <= v.Property.Height {
		g.OnQueueVisit(g, vidx, source, []Message{{Source: vidx, Type: RejectPush, Height: v.Property.Height, Value: flow}})
		return 1
	}
	v.Property.Neighbours[source] = Neighbour{
		Height:           height,
		ResidualCapacity: v.Property.Neighbours[source].ResidualCapacity + flow,
	}
	v.Property.Excess += flow
	return discharge(g, vidx)
}

func onPushRejected(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx, source, height, flow uint32) int {
	v := &g.Vertices[vidx]
	v.Property.Excess += flow
	v.Property.Neighbours[source] = Neighbour{height, v.Property.Neighbours[source].ResidualCapacity + flow}
	return discharge(g, vidx)
}

func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) {
	v := &g.Vertices[vidx]

	v.Property.MessageBuffer = make([]Message, 0)

	v.Property.Type = Normal
	v.Property.Excess = 0
	v.Property.Height = 0
	v.Property.InitHeight = 0

	v.Property.Neighbours = make(map[uint32]Neighbour)
	for i := range v.OutEdges {
		edge := &v.OutEdges[i]
		v.Property.Neighbours[edge.Destination] = Neighbour{0, edge.Property.Capacity}
	}
}

func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, VisitMsg MessageValue) (newInfo bool) {
	enforce.ENFORCE(len(VisitMsg) == 1)
	enforce.ENFORCE(sidx == VisitMsg[0].Source)

	dst.Mutex.Lock()
	dst.Property.MessageBuffer = append(dst.Property.MessageBuffer, VisitMsg[0])
	dst.Mutex.Unlock()

	// Ensure we only notify the target once.
	// TODO: why inside MessageAggregator?
	active := atomic.SwapInt32(&dst.IsActive, 1) == 0
	return active
}

func AggregateRetrieve(target *graph.Vertex[VertexProperty, EdgeProperty]) MessageValue {
	atomic.StoreInt32(&target.IsActive, 0) // TODO: why necessary
	target.Mutex.Lock()
	ret := target.Property.MessageBuffer
	target.Property.MessageBuffer = make([]Message, 0)
	target.Mutex.Unlock()
	return ret
}

func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxStart int, VisitMsg MessageValue) {
	enforce.ENFORCE("Not implemented")
}

func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxs []uint32, VisitMsg MessageValue) {
	enforce.ENFORCE("Not implemented")
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, VisitMsg MessageValue) int {
	v := &g.Vertices[vidx]
	nMessages := 0
	for messageIndex := range VisitMsg {
		m := &VisitMsg[messageIndex]
		switch m.Type {
		case Unspecified:
			enforce.ENFORCE(false)
		case InitSource:
			v.Property.Type = Source
			v.Property.Height = m.Value
			v.Property.InitHeight = m.Value
			nMessages += initPush(g, vidx)
		case InitSink:
			v.Property.Type = Sink
			v.Property.Height = 0
			v.Property.InitHeight = 0
		case NewHeight:
			v.Property.Neighbours[m.Source] = Neighbour{m.Height, v.Property.Neighbours[m.Source].ResidualCapacity}
		case PushRequest:
			nMessages += onPushRequest(g, vidx, m.Source, m.Height, m.Value)
		case RejectPush:
			nMessages += onPushRejected(g, vidx, m.Source, m.Height, m.Value)
		}
	}
	return nMessages
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
