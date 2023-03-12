package main

import (
	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
	"math"
)

func onInit(g *Graph, vidx uint32) (msgSent int) {
	v := &g.Vertices[vidx]
	for eidx := range v.OutEdges {
		e := &v.OutEdges[eidx]
		if e.Property.Capacity > 0 {
			msgSent += onCapacityChanged(g, vidx, e.Destination, int64(e.Property.Capacity))
		}
	}
	return
}

func push(g *Graph, sidx, didx uint32) (msgSent int) {
	s := &g.Vertices[sidx].Property
	amount := mathutils.Min(s.Excess, s.Nbrs[didx].ResCap)
	if amount > 0 && s.Height > s.Nbrs[didx].Height {
		s.Excess -= amount
		s.Nbrs[didx] = Nbr{
			Height: s.Nbrs[didx].Height,
			ResCap: s.Nbrs[didx].ResCap - amount,
		}
		msgSent += send(g, sidx, didx, amount)
	}
	return
}

func updateHeight(g *Graph, vidx uint32, newHeight int64) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Height != newHeight {
		//if newHeight == -getVertexCount() {
		//	info("Updating height to -|V|!")
		//}
		//enforce.ENFORCE(v.Type != Source || int(newHeight) > 0)
		v.Height = newHeight
		for n := range v.Nbrs {
			msgSent += send(g, vidx, n, 0)
		}
	}
	return
}

func lift(g *Graph, vidx uint32) (msgSent int) {
	v := &g.Vertices[vidx].Property
	enforce.ENFORCE(v.Type == Normal && v.Excess > 0)

	minHeight := int64(math.MaxInt64)
	for _, n := range v.Nbrs {
		if n.ResCap > 0 && n.Height < minHeight {
			minHeight = n.Height
		}
	}

	enforce.ENFORCE(minHeight != int64(math.MaxInt64) && v.Height < minHeight+1)
	msgSent += updateHeight(g, vidx, minHeight+1)
	return
}

func discharge(g *Graph, vidx uint32) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Type != Normal {
		for n := range v.Nbrs {
			msgSent += push(g, vidx, n)
		}
	} else {
		for v.Excess > 0 {
			for n := range v.Nbrs {
				msgSent += push(g, vidx, n)
			}
			if v.Excess > 0 {
				msgSent += lift(g, vidx)
			}
		}
	}
	return
}

func restoreHeightInvariant(g *Graph, vidx, widx uint32) (msgSent int) {
	if vidx == widx {
		return
	}
	v := &g.Vertices[vidx].Property
	if !bfsPhase {
		msgSent += push(g, vidx, widx)
	}
	if v.Type == Normal && v.Nbrs[widx].ResCap > 0 {
		maxHeight := v.Nbrs[widx].Height + 1
		if v.Height > maxHeight {
			msgSent += updateHeight(g, vidx, maxHeight)
		}
	}
	return
}

func onReceivingMessage(g *Graph, vidx uint32, m *Message) (msgSent int) {
	v := &g.Vertices[vidx].Property
	enforce.ENFORCE(m.Type == Flow)

	if m.Source != vidx {
		n, exist := v.Nbrs[m.Source]
		if !exist {
			msgSent += send(g, vidx, m.Source, 0)
		}
		v.Nbrs[m.Source] = Nbr{m.Height, n.ResCap}
		msgSent += handleFlow(g, vidx, m.Source, m.Value)
	}

	if resetPhase {
		return
	}

	msgSent += restoreHeightInvariant(g, vidx, m.Source)

	if v.Excess > 0 {
		if !bfsPhase {
			msgSent += discharge(g, vidx)
		}
	} else if v.Excess < 0 && v.Type == Normal && v.Height > 0 {
		msgSent += updateHeight(g, vidx, -getVertexCount())
	}
	return
}

func onNewMaxVertexCount(g *Graph, vidx uint32, newCount int64) (msgSent int) {
	v := &g.Vertices[vidx].Property
	if v.Type == Source {
		msgSent += updateHeight(g, vidx, newCount)
		msgSent += discharge(g, vidx)
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
		n.Height = math.MaxUint32
		msgSent += send(g, sidx, didx, 0)
	}
	s.Nbrs[didx] = Nbr{n.Height, n.ResCap + delta}

	// Update excess for source
	if s.Type == Source {
		// s.Excess < 0 ==> s.Nbrs[didx].ResCap < 0
		s.Excess += delta
	}

	if resetPhase {
		return
	}

	// Make sure it will be in a legal state
	if s.Nbrs[didx].ResCap < 0 {
		msgSent += send(g, sidx, didx, s.Nbrs[didx].ResCap)
	} else {
		msgSent += restoreHeightInvariant(g, sidx, didx)
	}
	return
}

func handleFlow(g *Graph, vidx, sidx uint32, amount int64) (msgSent int) {
	v := &g.Vertices[vidx].Property

	if amount < 0 { // Retract Request
		if v.Nbrs[sidx].ResCap < 0 {
			// Cannot fulfill this request since the ResCap is already < 0
			return
		}
		amount = mathutils.Max(amount, -v.Nbrs[sidx].ResCap)
	}

	v.Nbrs[sidx] = Nbr{
		Height: v.Nbrs[sidx].Height,
		ResCap: v.Nbrs[sidx].ResCap + amount,
	}
	v.Excess += amount

	if amount < 0 { // Retract Request
		msgSent += send(g, vidx, sidx, -amount) // (partly) fulfill this request
	}
	return
}
