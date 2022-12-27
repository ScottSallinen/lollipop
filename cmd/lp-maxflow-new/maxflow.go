package main

import (
	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
	"math"
)

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
			msgSent += send(g, Push, sidx, didx, amount)
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

func descend(g *Graph, vidx uint32, height int64) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Type == Normal && v.Height > height {
		v.Height = height
		for n := range v.Nbrs {
			msgSent += send(g, NewHeight, vidx, n, EmptyValue)
		}
	}
	return
}

func onReceivingMessage(g *Graph, vidx uint32, m *Message) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if m.Type == Init {
		msgSent += initVertex(g, vidx)
	} else {
		n, exist := v.Nbrs[m.Source]
		if !exist {
			msgSent += send(g, NewHeight, vidx, m.Source, EmptyValue)
		}
		v.Nbrs[m.Source] = Nbr{m.Height, n.ResCap}

		if v.Excess > 0 {
			enforce.ENFORCE(v.Type != Normal)
			msgSent += push(g, vidx, m.Source)
		}

		msgSent += onMsg[m.Type](g, vidx, m.Source, m.Value)

		if v.Excess < 0 {
			msgSent += descend(g, vidx, -getVertexCount())
			msgSent += VertexCountHelper.UpdateSubscriber(g, vidx, true)
		}

		if v.Nbrs[m.Source].ResCap > 0 {
			msgSent += descend(g, vidx, v.Nbrs[m.Source].Height+1)
		}
	}
	return
}

func onNewMaxVertexCount(g *Graph, vidx uint32, newCount int64) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Type == Source {
		v.Height = newCount
		for n := range v.Nbrs {
			msgSent += send(g, NewHeight, vidx, n, EmptyValue)
		}
		msgSent += discharge(g, vidx)
	}
	if v.Excess < 0 {
		msgSent += descend(g, vidx, -newCount)
	}
	return
}

func onCapacityChanged(g *Graph, sidx, didx uint32, delta int64) (msgSent int) {
	s := &g.Vertices[sidx].Property

	// ignore loops and edges to the source
	if sidx == didx || g.Vertices[didx].Property.Type == Source {
		return
	}

	// Update residual capacity
	n, exist := s.Nbrs[didx]
	if !exist {
		n.Height = InitialHeight
		msgSent += send(g, NewHeight, sidx, didx, EmptyValue)
	}
	s.Nbrs[didx] = Nbr{n.Height, n.ResCap + delta}

	// Update excess for source
	if s.Type == Source {
		// s.Excess < 0 ==> s.Nbrs[didx].ResCap < 0
		s.Excess += delta
		if s.Excess > 0 {
			msgSent += push(g, sidx, didx)
		}
	}

	// Make sure it will be in a legal state
	if s.Nbrs[didx].ResCap < 0 {
		msgSent += send(g, RetractRequest, sidx, didx, -s.Nbrs[didx].ResCap)
	} else if s.Nbrs[didx].ResCap > 0 {
		msgSent += descend(g, sidx, s.Nbrs[didx].Height+1)
	}
	return
}

// Message Handlers

var onMsg = []func(g *Graph, vidx, sidx uint32, value int64) (msgSent int){
	nil, nil, onMsgNewHeight, onMsgPush, onMsgRetractRequest,
}

func initVertex(g *Graph, vidx uint32) (msgSent int) {
	v := &g.Vertices[vidx]
	for eidx := range v.OutEdges {
		e := &v.OutEdges[eidx]
		if e.Property.Capacity > 0 {
			msgSent += onCapacityChanged(g, vidx, e.Destination, int64(e.Property.Capacity))
		}
	}
	return
}

func onMsgNewHeight(_ *Graph, _, _ uint32, _ int64) (msgSent int) {
	return // Do nothing
}

func onMsgPush(g *Graph, vidx, sidx uint32, amount int64) (msgSent int) {
	enforce.ENFORCE(amount > 0)
	v := &g.Vertices[vidx].Property
	v.Nbrs[sidx] = Nbr{
		Height: v.Nbrs[sidx].Height,
		ResCap: v.Nbrs[sidx].ResCap + amount,
	}
	v.Excess += amount
	if v.Excess > 0 {
		msgSent += push(g, vidx, sidx)
		msgSent += discharge(g, vidx)
	}
	return
}

func onMsgRetractRequest(g *Graph, vidx, sidx uint32, amount int64) (msgSent int) {
	enforce.ENFORCE(amount > 0)
	v := &g.Vertices[vidx].Property
	// Compute the amount successfully retracted
	succAmt := mathutils.Min(amount, v.Nbrs[sidx].ResCap)
	if succAmt > 0 {
		msgSent += send(g, Push, vidx, sidx, succAmt)
		v.Excess -= succAmt
		v.Nbrs[sidx] = Nbr{
			Height: v.Nbrs[sidx].Height,
			ResCap: v.Nbrs[sidx].ResCap - succAmt,
		}
	}

	// RetractReject does nothing
	//amount -= succAmt
	//if amount > 0 {
	//	msgSent += send(g, RetractReject, vidx, sidx, amount)
	//}
	return
}
