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

	Increasing MessageType = 7
	Decreasing MessageType = 8
)

var MessageCounter = make([]uint32, 8)

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

// TODO: rename to sourceInit
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
	// TODO: broadcasting height is optional
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

func onIncreasing(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx, source, height uint32) int {
	v := &g.Vertices[vidx]
	if v.Property.Type == Sink {
		return 0
	}

	neighbour := v.Property.Neighbours[source]

	v.Property.Neighbours[source] = Neighbour{
		Height:           height,
		ResidualCapacity: neighbour.ResidualCapacity,
	}

	if neighbour.ResidualCapacity == 0 {
		// No flow can be pushed to this neighbour
		return 0
	}
	return fillNeighbours(g, vidx)
}

func onDecreasing(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx, source, height, retiringFlow uint32) int {
	v := &g.Vertices[vidx]

	neighbour := v.Property.Neighbours[source]
	enforce.ENFORCE(neighbour.ResidualCapacity > retiringFlow)
	neighbour.Height = height
	neighbour.ResidualCapacity -= retiringFlow
	v.Property.Neighbours[source] = neighbour

	v.Property.Height = v.Property.InitHeight

	// Note: this is different from Pham's
	min := mathutils.Min(v.Property.Excess, retiringFlow)
	v.Property.Excess -= min
	retiringFlow -= min
	if retiringFlow == 0 {
		return 0
	}

	enforce.ENFORCE(v.Property.Type != Sink)
	nMessages := 0
	for ni, n := range v.Property.Neighbours {
		if n.ResidualCapacity == 0 {
			continue
		}
		min = mathutils.Min(n.ResidualCapacity, retiringFlow)
		n.ResidualCapacity -= min
		retiringFlow -= min
		g.OnQueueVisit(g, vidx, ni, []Message{{Source: vidx, Type: Decreasing, Height: v.Property.Height, Value: min}})
		nMessages += 1
		if retiringFlow == 0 {
			break
		}
	}
	return nMessages
}

func fillNeighbours(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) int {
	v := &g.Vertices[vidx]
	enforce.ENFORCE(v.Property.Type != Sink)

	nMessages := 0

	if v.Property.Excess > 0 {
		nMessages += discharge(g, vidx)
	}

	if v.Property.Type != Source {
		// TODO: Optimize. If all neighbours are saturated, there is no need to get more flow
		// If v.Property.Height == v.Property.InitHeight, then we haven't pushed back any flow, so there is no point to
		// "pull" flows from other vertices. This significantly improves the performance.
		// TODO: this optimization is not present in Pham's paper, so it is still not clear if it is correct.
		if v.Property.Height > v.Property.InitHeight {
			v.Property.Height = v.Property.InitHeight
			for neighbourIndex := range v.Property.Neighbours {
				g.OnQueueVisit(g, vidx, neighbourIndex, []Message{{Source: vidx, Type: Increasing, Height: v.Property.Height}})
			}
			nMessages += len(v.Property.Neighbours)
		}
	}

	return nMessages
}

func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, vertexType VertexType, initHeight uint32) {
	v := &g.Vertices[vidx]
	//if graph.DEBUG {
	//	info(fmt.Sprintf("OnInitVertex id=%v vidx=%v: vertexType=%v initHeight=%v", v.Id, vidx, vertexType, initHeight))
	//}

	v.Property.MessageBuffer = make([]Message, 0)

	v.Property.Type = vertexType
	v.Property.Excess = 0
	v.Property.Height = initHeight
	v.Property.InitHeight = initHeight

	v.Property.Neighbours = make(map[uint32]Neighbour)
	for i := range v.OutEdges {
		edge := &v.OutEdges[i]
		v.Property.Neighbours[edge.Destination] = Neighbour{0, edge.Property.Capacity}
	}
}

func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, VisitMsg MessageValue) (newInfo bool) {
	enforce.ENFORCE(len(VisitMsg) == 1)
	if VisitMsg[0].Type == InitSource {
		enforce.ENFORCE(dst.Property.Type == Source)
	} else {
		enforce.ENFORCE(sidx == VisitMsg[0].Source)
	}

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
	source := &g.Vertices[sidx]

	for eidx := didxStart; eidx < len(source.OutEdges); eidx++ {
		dstIndex := source.OutEdges[eidx].Destination
		neighbour := source.Property.Neighbours[dstIndex]
		neighbour.ResidualCapacity += source.OutEdges[eidx].Property.Capacity
		source.Property.Neighbours[dstIndex] = neighbour
	}

	OnVisitVertex(g, sidx, VisitMsg)

	if source.Property.Type == Sink {
		return
	}

	// TODO: Optimize. If the new arcs are not saturated, there is no need to get more flow

	if source.Property.Type == Source {
		for eidx := didxStart; eidx < len(source.OutEdges); eidx++ {
			source.Property.Excess += source.OutEdges[eidx].Property.Capacity
		}
	}

	fillNeighbours(g, sidx)
}

func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, deletedEdges []graph.Edge[EdgeProperty], VisitMsg MessageValue) {
	OnVisitVertex(g, sidx, VisitMsg)

	source := &g.Vertices[sidx]

	if source.Property.Type == Sink {
		return
	}

	type retiringFlow struct {
		Destination uint32
		Flow        uint32
	}
	retiringFlows := make([]retiringFlow, 0, len(deletedEdges))

	for _, e := range deletedEdges {
		destination := source.Property.Neighbours[e.Destination]
		min := mathutils.Min(destination.ResidualCapacity, e.Property.Capacity)
		destination.ResidualCapacity -= min
		source.Property.Neighbours[e.Destination] = destination
		// TODO: should I remove neighbours with 0 residual capacity?
		if left := e.Property.Capacity - min; left != 0 {
			retiringFlows = append(retiringFlows, retiringFlow{e.Destination, left})
		}
	}

	if len(retiringFlows) == 0 {
		return
	}

	if source.Property.Type != Source {
		source.Property.Height = source.Property.InitHeight // discharge will broadcast the new height
		for i := range retiringFlows {
			source.Property.Excess += retiringFlows[i].Flow
		}
		discharge(g, sidx)
	}

	for i := range retiringFlows {
		g.OnQueueVisit(g, sidx, retiringFlows[i].Destination, []Message{{Source: sidx, Type: Decreasing, Height: source.Property.Height, Value: retiringFlows[i].Flow}})
	}
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, VisitMsg MessageValue) int {
	v := &g.Vertices[vidx]
	nMessages := 0
	for messageIndex := range VisitMsg {
		m := &VisitMsg[messageIndex]
		MessageCounter[m.Type] += 1
		if graph.DEBUG {
			info(fmt.Sprintf("OnVisitVertex id=%v vidx=%v: m.Type=%v m.Source=%v m.Height=%v m.Value=%v", v.Id, vidx, m.Type, m.Source, m.Height, m.Value))
		}
		switch m.Type {
		case Unspecified:
			enforce.ENFORCE(false)
		case InitSource:
			enforce.ENFORCE(v.Property.Type == Source)
			nMessages += initPush(g, vidx)
		case InitSink:
			enforce.ENFORCE(v.Property.Type == Sink)
		case NewHeight:
			v.Property.Neighbours[m.Source] = Neighbour{m.Height, v.Property.Neighbours[m.Source].ResidualCapacity}
		case PushRequest:
			nMessages += onPushRequest(g, vidx, m.Source, m.Height, m.Value)
		case RejectPush:
			nMessages += onPushRejected(g, vidx, m.Source, m.Height, m.Value)
		case Decreasing:
			nMessages += onDecreasing(g, vidx, m.Source, m.Height, m.Value)
		}
	}
	aggregatedMessage := MaxFlowMessageAggregator(g, vidx, VisitMsg)
	nMessages += ProcessAggregatedMessage(&aggregatedMessage, g, vidx)
	return nMessages
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
