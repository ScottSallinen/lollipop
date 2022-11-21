package main

import (
	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
	"math"
)

var tempVertexCount uint32

func send(g *Graph, m MessageType, sidx, didx uint32, value int64) (msgSent int) {
	g.OnQueueVisit(g, sidx, didx, []Message{{
		Type:   m,
		Source: sidx,
		Height: g.Vertices[sidx].Property.Height,
		Value:  value,
	}})
	return 1
}

func discharge(g *Graph, vidx uint32) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Excess <= 0 {
		return
	}
	if v.Type != Normal {
		for n := range v.Nbrs {
			msgSent += push(g, vidx, n)
			if v.Excess == 0 {
				return
			}
		}
	} else {
		for v.Excess > 0 {
			for n := range v.Nbrs {
				msgSent += push(g, vidx, n)
				if v.Excess == 0 {
					return
				}
			}
			msgSent += lift(g, vidx)
		}
	}
	return
}

func push(g *Graph, sidx, didx uint32) (msgSent int) {
	s := &g.Vertices[sidx].Property
	enforce.ENFORCE(s.Excess > 0)
	if s.Height > s.Nbrs[didx].Height {
		amount := mathutils.Min(s.Excess, s.Nbrs[didx].ResCap)
		if amount > 0 {
			s.Excess -= amount
			s.Nbrs[didx] = Nbr{
				Height: s.Nbrs[didx].Height,
				ResCap: s.Nbrs[didx].ResCap - amount,
			}
			msgSent += send(g, PushRequest, sidx, didx, amount)
		}
	}
	return
}

func lift(g *Graph, vidx uint32) (msgSent int) {
	v := &g.Vertices[vidx].Property
	enforce.ENFORCE(v.Type == Normal)
	minHeight := int64(math.MaxInt64)
	for _, n := range v.Nbrs {
		if n.ResCap > 0 && n.Height < minHeight {
			minHeight = n.Height
		}
	}
	v.Height = minHeight + 1
	for n := range v.Nbrs {
		msgSent += send(g, NewHeight, vidx, n, EmptyValue)
	}
	return
}

func descendAndPull(g *Graph, vidx uint32, height int64) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Type == Normal && v.Height > height {
		v.Height = height
		for n := range v.Nbrs {
			msgSent += send(g, Pull, vidx, n, EmptyValue)
		}
	}
	return
}

func onReceivingMessage(g *Graph, vidx uint32, m *Message) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if m.Type != Init {
		v.Nbrs[m.Source] = Nbr{
			Height: m.Height,
			ResCap: v.Nbrs[m.Source].ResCap,
		}
	}
	return onMsg[m.Type](g, vidx, m.Source, m.Value)
}

func onNewMaxVertexCount(g *Graph, vidx uint32, newCount uint32) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Type == Source {
		v.Height = int64(newCount)
		msgSent += discharge(g, vidx)
	}
	if v.Excess < 0 {
		msgSent += descendAndPull(g, vidx, -int64(newCount))
	}
	return
}

func onCapacityChanged(g *Graph, sidx, didx uint32, delta int64) (msgSent int) {
	s := &g.Vertices[sidx].Property
	s.Nbrs[didx] = Nbr{
		Height: s.Nbrs[didx].Height,
		ResCap: s.Nbrs[didx].ResCap + delta,
	}

	if s.Type == Source {
		// s.Excess < 0 ==> s.Nbrs[didx].ResCap < 0
		s.Excess += delta
	}

	if delta > 0 { // Increased
		msgSent += send(g, CapacityIncreased, sidx, didx, EmptyValue)
	} else { // Decreased
		retractAmount := mathutils.Max(0, -s.Nbrs[didx].ResCap)
		if retractAmount > 0 {
			msgSent += send(g, RetractRequest, sidx, didx, retractAmount)
		}
	}
	return
}

func getVertexCount() uint32 {
	return tempVertexCount // TODO
}

// Message Handlers

var onMsg = []func(g *Graph, vidx, sidx uint32, value int64) (msgSent int){
	nil, onMsgInit, onMsgNewHeight, onMsgPushRequest, onMsgPushReject, onMsgPull, onMsgCapacityIncreased,
	onMsgRetractRequest, onMsgRetractConfirm, onMsgRetractReject,
}

func onMsgInit(g *Graph, vidx, _ uint32, _ int64) (msgSent int) {
	v := &g.Vertices[vidx]
	switch v.Property.Type {
	case Source:
		v.Property.Height = int64(getVertexCount())
	case Sink:
		v.Property.Height = 0
	}
	for eidx := range v.OutEdges {
		e := &v.OutEdges[eidx]
		if e.Property.Capacity > 0 {
			v.Property.Nbrs[e.Destination] = Nbr{Height: InitialHeight, ResCap: int64(e.Property.Capacity)}
			msgSent += send(g, CapacityIncreased, vidx, e.Destination, EmptyValue) // OPTIMIZATION
		}
	}
	if v.Property.Type == Source {
		for eidx := range v.OutEdges {
			v.Property.Excess += int64(v.OutEdges[eidx].Property.Capacity)
		}
		msgSent += discharge(g, vidx)
	}
	return
}

func onMsgNewHeight(_ *Graph, _, _ uint32, _ int64) (msgSent int) {
	return // Do nothing
}

func onMsgPushRequest(g *Graph, vidx, sidx uint32, amount int64) (msgSent int) {
	enforce.ENFORCE(amount > 0)
	v := &g.Vertices[vidx].Property
	if v.Nbrs[sidx].Height > v.Height {
		// Note: src.Height > v.Height+ 1 ==> there is an in-flight Pull from v to src
		//       This is unusual but nothing needs to be done
		v.Nbrs[sidx] = Nbr{
			Height: v.Nbrs[sidx].Height,
			ResCap: v.Nbrs[sidx].ResCap + amount,
		}
		v.Excess += amount
		msgSent += discharge(g, vidx)
	} else {
		msgSent += send(g, PushReject, vidx, sidx, amount)
	}
	return
}

func onMsgPushReject(g *Graph, vidx, sidx uint32, amount int64) (msgSent int) {
	v := &g.Vertices[vidx].Property
	v.Nbrs[sidx] = Nbr{
		Height: v.Nbrs[sidx].Height,
		ResCap: v.Nbrs[sidx].ResCap + amount,
	}
	v.Excess += amount
	return discharge(g, vidx)
}

func onMsgPull(g *Graph, vidx, sidx uint32, _ int64) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Excess > 0 {
		msgSent += push(g, vidx, sidx)
	}
	if v.Nbrs[sidx].ResCap > 0 { // OPTIMIZATION?
		msgSent += descendAndPull(g, vidx, v.Nbrs[sidx].Height+1)
	}
	return
}

func onMsgCapacityIncreased(g *Graph, vidx, sidx uint32, _ int64) (msgSent int) {
	return send(g, Pull, vidx, sidx, EmptyValue)
}

func onMsgRetractRequest(g *Graph, vidx, sidx uint32, amount int64) (msgSent int) {
	enforce.ENFORCE(amount > 0)
	v := &g.Vertices[vidx].Property
	// Compute the amount successfully retracted
	succAmt := mathutils.Min(amount, v.Nbrs[sidx].ResCap)
	if succAmt > 0 {
		msgSent += send(g, RetractConfirm, vidx, sidx, succAmt)
		v.Excess -= succAmt
		v.Nbrs[sidx] = Nbr{
			Height: v.Nbrs[sidx].Height,
			ResCap: v.Nbrs[sidx].ResCap - succAmt,
		}
		if v.Excess < 0 {
			msgSent += descendAndPull(g, vidx, -int64(getVertexCount()))
		}
	}

	// RetractReject does nothing
	amount -= succAmt
	if amount > 0 {
		msgSent += send(g, RetractReject, vidx, sidx, amount)
	}
	return
}

func onMsgRetractConfirm(g *Graph, vidx, sidx uint32, amount int64) (msgSent int) {
	v := &g.Vertices[vidx].Property
	v.Nbrs[sidx] = Nbr{
		Height: v.Nbrs[sidx].Height,
		ResCap: v.Nbrs[sidx].ResCap + amount,
	}
	v.Excess += amount
	return discharge(g, vidx)
}

func onMsgRetractReject(_ *Graph, _, _ uint32, _ int64) (msgSent int) {
	// Do nothing
	return
}
