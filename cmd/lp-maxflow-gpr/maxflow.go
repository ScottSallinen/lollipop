package main

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

// TODO: handle self loop and multi-graphs

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

var MessageCounter = make([]uint32, 9)

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
	//tryCount := 0
	for v.Property.Excess > 0 {
		nMessages += lift(g, vidx)
		nMessages += push(g, vidx)
		//tryCount++
		//enforce.ENFORCE(tryCount < 1000000, "Might be an infinite loop")
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
	if minHeight == math.MaxUint32 {
		return 0
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
	//info(fmt.Sprintf("Push accepted: vidx=%v, v.Id=%v, source=%v, source.Id=%v, height=%v, flow=%v", vidx, v.Id, source, g.Vertices[source].Id, height, flow))
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

func onDecreasing(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx, source, height, retiringFlow uint32, deletedEdges []graph.Edge[EdgeProperty]) int {
	v := &g.Vertices[vidx]
	nMessages := 0

	neighbour := v.Property.Neighbours[source]
	// Check if this flow still exists, as it might be rejected/pushed back
	if neighbour.ResidualCapacity < retiringFlow {
		// TODO: might be inefficient
		g.OnQueueVisit(g, vidx, source, []Message{{Source: vidx, Type: Decreasing, Height: v.Property.Height, Value: retiringFlow}})
		nMessages += 1
		return nMessages
	}

	neighbour.Height = height
	neighbour.ResidualCapacity -= retiringFlow
	v.Property.Neighbours[source] = neighbour

	if v.Property.Height != v.Property.InitHeight {
		v.Property.Height = v.Property.InitHeight
		for neighbourIndex := range v.Property.Neighbours {
			g.OnQueueVisit(g, vidx, neighbourIndex, []Message{{Source: vidx, Type: NewHeight, Height: v.Property.Height}})
		}
		nMessages += len(v.Property.Neighbours)
	}

	// Note: this is different from Pham's
	min := mathutils.Min(v.Property.Excess, retiringFlow)
	v.Property.Excess -= min
	retiringFlow -= min
	if retiringFlow == 0 {
		return nMessages
	}

	enforce.ENFORCE(v.Property.Type != Sink)

	for ei := range v.OutEdges {
		e := &v.OutEdges[ei]
		n := v.Property.Neighbours[e.Destination]
		if e.Property.Capacity <= n.ResidualCapacity {
			// no flow was sent across this edge (after canceling with the flow on the reverse edge)
			continue
		}

		flow := mathutils.Min(e.Property.Capacity-n.ResidualCapacity, retiringFlow)
		retiringFlow -= flow
		n.ResidualCapacity += flow
		v.Property.Neighbours[e.Destination] = n
		g.OnQueueVisit(g, vidx, e.Destination, []Message{{Source: vidx, Type: Decreasing, Height: v.Property.Height, Value: flow}})
		nMessages += 1

		if retiringFlow == 0 {
			break
		}
	}
	for ei := range deletedEdges {
		e := &deletedEdges[ei]
		n := v.Property.Neighbours[e.Destination]
		if e.Property.Capacity <= n.ResidualCapacity {
			// no flow was sent across this edge (after canceling with the flow on the reverse edge)
			continue
		}

		flow := mathutils.Min(e.Property.Capacity-n.ResidualCapacity, retiringFlow)
		retiringFlow -= flow
		n.ResidualCapacity += flow
		v.Property.Neighbours[e.Destination] = n
		g.OnQueueVisit(g, vidx, e.Destination, []Message{{Source: vidx, Type: Decreasing, Height: v.Property.Height, Value: flow}})
		nMessages += 1

		if retiringFlow == 0 {
			break
		}
	}
	enforce.ENFORCE(retiringFlow == 0)
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
	//for i := didxStart; i < len(source.OutEdges); i++ {
	//	dstIdx := source.OutEdges[i].Destination
	//	info(fmt.Sprintf("OnEdgeAdd id=%v sidx=%v -> dstId=%v dstIdx=%v", source.Id, sidx, g.Vertices[dstIdx].Id, dstIdx))
	//}

	for eidx := didxStart; eidx < len(source.OutEdges); eidx++ {
		dstIndex := source.OutEdges[eidx].Destination
		//info(fmt.Sprintf("Edge %v to %v is added", sidx, dstIndex))
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
	doOnVisitVertex(g, sidx, VisitMsg, deletedEdges)
	source := &g.Vertices[sidx]
	//for _, e := range deletedEdges {
	//	info(fmt.Sprintf("OnEdgeDel id=%v sidx=%v -> dstId=%v dstIdx=%v", source.Id, sidx, g.Vertices[e.Destination].Id, e.Destination))
	//}

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

		if e.Property.Capacity <= destination.ResidualCapacity {
			destination.ResidualCapacity -= e.Property.Capacity
		} else {
			flow := e.Property.Capacity - destination.ResidualCapacity
			retiringFlows = append(retiringFlows, retiringFlow{Destination: e.Destination, Flow: flow})
			source.Property.Excess += flow
			destination.ResidualCapacity = 0
		}
		// TODO: remove neighbours with 0 residual capacity?
		source.Property.Neighbours[e.Destination] = destination
	}

	if len(retiringFlows) != 0 {
		if source.Property.Type == Source {
			for _, e := range deletedEdges {
				source.Property.Excess -= e.Property.Capacity
			}
		} else {
			source.Property.Height = source.Property.InitHeight // discharge will broadcast the new height
			discharge(g, sidx)
		}
		for i := range retiringFlows {
			g.OnQueueVisit(g, sidx, retiringFlows[i].Destination,
				[]Message{{Source: sidx, Type: Decreasing, Height: source.Property.Height, Value: retiringFlows[i].Flow}},
			)
		}
	}
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, VisitMsg MessageValue) int {
	return doOnVisitVertex(g, vidx, VisitMsg, make([]graph.Edge[EdgeProperty], 0))
}

func doOnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, VisitMsg MessageValue, deletedEdges []graph.Edge[EdgeProperty]) int {
	v := &g.Vertices[vidx]
	nMessages := 0
	for messageIndex := range VisitMsg {
		m := &VisitMsg[messageIndex]
		MessageCounter[m.Type] += 1
		//if graph.DEBUG {
		//	if m.Source != math.MaxUint32 {
		//		info(fmt.Sprintf("OnVisitVertex id=%v sid=%v vidx=%v sidx=%v: m.Type=%v m.Height=%v m.Value=%v", v.Id, g.Vertices[m.Source].Id, vidx, m.Source, m.Type, m.Height, m.Value))
		//	} else {
		//		info(fmt.Sprintf("OnVisitVertex id=%v vidx=%v: m.Type=%v m.Height=%v m.Value=%v", v.Id, vidx, m.Type, m.Height, m.Value))
		//	}
		//}
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
			if v.Property.Type == Source {
				nMessages += push(g, vidx)
			}
		case PushRequest:
			nMessages += onPushRequest(g, vidx, m.Source, m.Height, m.Value)
		case RejectPush:
			nMessages += onPushRejected(g, vidx, m.Source, m.Height, m.Value)
		case Decreasing:
			nMessages += onDecreasing(g, vidx, m.Source, m.Height, m.Value, deletedEdges)
		}
	}
	aggregatedMessage := MaxFlowMessageAggregator(g, vidx, VisitMsg)
	nMessages += ProcessAggregatedMessage(&aggregatedMessage, g, vidx)
	return nMessages
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
