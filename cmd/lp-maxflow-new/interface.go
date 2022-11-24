package main

import (
	"github.com/ScottSallinen/lollipop/enforce"
	"sync/atomic"
)

func MessageAggregator(dst *Vertex, _, sidx uint32, VisitMsg MessageValue) (newInfo bool) {
	enforce.ENFORCE(len(VisitMsg) == 1)
	enforce.ENFORCE(VisitMsg[0].Type != Unspecified)
	enforce.ENFORCE(VisitMsg[0].Source == sidx || VisitMsg[0].Type == Init)

	dst.Mutex.Lock()
	dst.Property.MessageBuffer = append(dst.Property.MessageBuffer, VisitMsg[0])
	dst.Mutex.Unlock()

	// Ensure we only notify the target once.
	active := atomic.SwapInt32(&dst.IsActive, 1) == 0
	return active
}

func AggregateRetrieve(target *Vertex) MessageValue {
	atomic.StoreInt32(&target.IsActive, 0)
	target.Mutex.Lock()
	ret := target.Property.MessageBuffer
	target.Property.MessageBuffer = make([]Message, 0, len(ret))
	target.Mutex.Unlock()
	return ret
}

func OnEdgeAdd(g *Graph, sidx uint32, didxStart int, VisitMsg MessageValue) {
	OnVisitVertex(g, sidx, VisitMsg)
	s := &g.Vertices[sidx]
	//for i := didxStart; i < len(s.OutEdges); i++ {
	//	didx := s.OutEdges[i].Destination
	//	info(fmt.Sprintf("OnEdgeAdd id=%v sidx=%v -> dstId=%v didx=%v", s.Id, sidx, g.Vertices[didx].Id, didx))
	//}
	for eidx := didxStart; eidx < len(s.OutEdges); eidx++ {
		e := &s.OutEdges[eidx]
		onCapacityChanged(g, sidx, e.Destination, int64(e.Property.Capacity))
	}
}

func OnEdgeDel(g *Graph, sidx uint32, deletedEdges []Edge, VisitMsg MessageValue) {
	OnVisitVertex(g, sidx, VisitMsg)
	//for _, e := range deletedEdges {
	//	info(fmt.Sprintf("OnEdgeDel id=%v sidx=%v -> dstId=%v didx=%v", s.Id, sidx, g.Vertices[e.Destination].Id, e.Destination))
	//}
	for _, e := range deletedEdges {
		onCapacityChanged(g, sidx, e.Destination, -int64(e.Property.Capacity))
	}
}

func OnVisitVertex(g *Graph, vidx uint32, VisitMsg MessageValue) (msgSent int) {
	v := &g.Vertices[vidx]
	for messageIndex := range VisitMsg {
		m := &VisitMsg[messageIndex]
		m.PrintIfNeeded(g, v, vidx)
		CountMessage(m)
		if m.Type == NewMaxVertexCount {
			msgSent += onNewMaxVertexCount(g, vidx, m.Value)
		} else {
			if m.Type == Init {
				if v.Property.Type == Source {
					msgSent += VertexCountHelper.UpdateSubscriber(g, vidx, true)
				}
				msgSent += VertexCountHelper.NewVertex(g, vidx)
			}
			msgSent += onReceivingMessage(g, vidx, m)
		}
	}
	//if v.Property.Type != Source && v.Property.Excess >= 0 {
	//	msgSent += VertexCountHelper.UpdateSubscriber(g, vidx, false)
	//}
	return
}

func OnFinish(_ *Graph) error {
	return nil
}

func send(g *Graph, m MessageType, sidx, didx uint32, value int64) (msgSent int) {
	g.OnQueueVisit(g, sidx, didx, []Message{{
		Type:   m,
		Source: sidx,
		Height: g.Vertices[sidx].Property.Height,
		Value:  value,
	}})
	return 1
}

func getVertexCount() int64 {
	return VertexCountHelper.GetVertexCount()
}
