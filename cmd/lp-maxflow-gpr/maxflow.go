package main

import (
	"math"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
)

// TODO: handle self loop and multi-graphs

// TODO: rename to sourceInit
func sourceInit(g *Graph, v *Vertex, vidx uint32) int {
	enforce.ENFORCE(v.Property.Type == Source)
	for i := range v.OutEdges {
		edge := &v.OutEdges[i]
		v.Property.Excess += edge.Property.Capacity
	}
	return push(g, vidx)
}

func discharge(g *Graph, vidx uint32) int {
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

func push(g *Graph, vidx uint32) int {
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

func lift(g *Graph, vidx uint32) int {
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

func onPushRequest(g *Graph, vidx, source, height, flow uint32) int {
	v := &g.Vertices[vidx]
	if height <= v.Property.Height {
		g.OnQueueVisit(g, vidx, source, []Message{{Source: vidx, Type: RejectPush, Height: v.Property.Height, Value: flow}})
		return 1
	}
	//info(fmt.Sprintf("Push accepted: vidx=%v, v.Id=%v, source=%v, source.Id=%v, height=%v, flow=%v", vidx, v.Id, source, g.Vertices[source].Id, height, flow))
	v.Property.Excess += flow
	v.Property.Neighbours[source] = Neighbour{height, v.Property.Neighbours[source].ResidualCapacity + flow}
	return discharge(g, vidx)
}

func onPushRejected(g *Graph, vidx, source, height, flow uint32) int {
	v := &g.Vertices[vidx]
	v.Property.Excess += flow
	v.Property.Neighbours[source] = Neighbour{height, v.Property.Neighbours[source].ResidualCapacity + flow}
	return discharge(g, vidx)
}

func onDecreasing(g *Graph, vidx, source, height, retiringFlow uint32, deletedEdges []Edge) int {
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

	//if v.Property.Height != v.Property.InitHeight {
	//	v.Property.Height = v.Property.InitHeight
	//	for neighbourIndex := range v.Property.Neighbours {
	//		g.OnQueueVisit(g, vidx, neighbourIndex, []Message{{Source: vidx, Type: NewHeight, Height: v.Property.Height}})
	//	}
	//	nMessages += len(v.Property.Neighbours)
	//}

	min := mathutils.Min(v.Property.Excess, retiringFlow)
	v.Property.Excess -= min
	retiringFlow -= min

	if v.Property.Type != Sink {
		nMessages += fillNeighbours(g, vidx, map[uint32]bool{source: true})
	}

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

func fillNeighbours(g *Graph, vidx uint32, skip map[uint32]bool) int {
	v := &g.Vertices[vidx]
	enforce.ENFORCE(v.Property.Type != Sink)

	// TODO: try skipping broadcasting INCREASING when excess > 0
	nMessages := discharge(g, vidx)

	if v.Property.Type != Source {
		// If v.Property.Height == v.Property.InitHeight, then we haven't pushed back any flow, so there is no point to
		// "pull" flows from other vertices. This significantly improves the performance.
		if v.Property.Height > v.Property.InitHeight {
			for ni, n := range v.Property.Neighbours {
				if n.Height > v.Property.Height+1 || skip[ni] {
					// +1 is because a height difference >= 2 implies the other vertex has no flow to push to this vertex
					continue
				}
				g.OnQueueVisit(g, vidx, ni, []Message{{Source: vidx, Type: Increasing, Height: v.Property.InitHeight}})
			}
			nMessages += len(v.Property.Neighbours)
			v.Property.Height = v.Property.InitHeight
		}
	}

	return nMessages
}

func OnInitVertex(g *Graph, vidx uint32, vertexType VertexType, initHeight uint32) {
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

func MessageAggregator(dst *Vertex, _, sidx uint32, VisitMsg MessageValue) (newInfo bool) {
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

func AggregateRetrieve(target *Vertex) MessageValue {
	atomic.StoreInt32(&target.IsActive, 0) // TODO: why necessary
	target.Mutex.Lock()
	ret := target.Property.MessageBuffer
	target.Property.MessageBuffer = make([]Message, 0)
	target.Mutex.Unlock()
	return ret
}

func OnEdgeAdd(g *Graph, sidx uint32, didxStart int, VisitMsg MessageValue) {
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

	fillNeighbours(g, sidx, map[uint32]bool{})
}

func OnEdgeDel(g *Graph, sidx uint32, deletedEdges []Edge, VisitMsg MessageValue) {
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

func OnVisitVertex(g *Graph, vidx uint32, VisitMsg MessageValue) int {
	return doOnVisitVertex(g, vidx, VisitMsg, make([]Edge, 0))
}

func doOnVisitVertex(g *Graph, vidx uint32, VisitMsg MessageValue, deletedEdges []Edge) int {
	v := &g.Vertices[vidx]
	nMessages := 0
	for messageIndex := range VisitMsg {
		m := &VisitMsg[messageIndex]
		m.PrintIfNeeded(g, v, vidx)
		CountMessage(m)

		switch m.Type {
		case Unspecified:
			enforce.ENFORCE(false)
		case InitSource:
			enforce.ENFORCE(v.Property.Type == Source)
			nMessages += sourceInit(g, v, vidx)
		case InitSink:
			enforce.ENFORCE(v.Property.Type == Sink)
		case NewHeight:
			v.Property.Neighbours[m.Source] = Neighbour{m.Height, v.Property.Neighbours[m.Source].ResidualCapacity}
			//if v.Property.Type == Source {
			//	// TODO: push to m.Source only
			//	nMessages += push(g, vidx)
			//}
		case PushRequest:
			nMessages += onPushRequest(g, vidx, m.Source, m.Height, m.Value)
		case RejectPush:
			nMessages += onPushRejected(g, vidx, m.Source, m.Height, m.Value)
		case Decreasing:
			nMessages += onDecreasing(g, vidx, m.Source, m.Height, m.Value, deletedEdges)
		}
	}
	aggregatedMessage := MaxFlowMessageAggregator(g, vidx, VisitMsg)
	nMessages += ProcessAggregatedMessage(aggregatedMessage, g, vidx)
	return nMessages
}

func OnFinish(g *Graph) error {
	return nil
}
