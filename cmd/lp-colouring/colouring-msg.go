package main

import (
	"math"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type ColouringMsg struct{}

// A strategy (for static graphs) is to use wait count to have each vertex pick a colour "in order".
// Note that the Base MessageMsg for a dynamic graph would have no beginning edges, so wait count would be zero.
const USE_WAIT_COUNT_MSG = false

const MAGIC_VAL_MSG = math.MaxUint32 - 1
const EMPTY_VAL_MSG = math.MaxUint32

type VPropMsg struct {
	Colour         uint32
	NbrScratch     []uint32
	ColoursIndexed utils.Bitmap
	WaitCount      int32
}

type EPropMsg struct {
	graph.EmptyEdge
	Priority bool
}

type MessageMsg struct {
	Init bool
}

type NoteMsg struct {
	Pos      uint32
	Colour   uint32
	Priority bool
}

func (VPropMsg) New() (vp VPropMsg) {
	vp.Colour = 0
	vp.ColoursIndexed.Grow(127) // Two of 8 bytes is a good start.
	return vp
}

func (MessageMsg) New() (m MessageMsg) {
	m.Init = false
	return m
}

func (*ColouringMsg) BaseVertexMessage(src *graph.Vertex[VPropMsg, EPropMsg], internalId uint32, rawId graph.RawType) (m MessageMsg) {
	edgeAmount := len(src.OutEdges)                      // NoteMsg: will be zero for dynamic graphs.
	src.Property.NbrScratch = make([]uint32, edgeAmount) // Make either way.
	src.Property.Colour = EMPTY_VAL_MSG

	if edgeAmount > 0 {
		myPriority := hash(internalId)
		for i := 0; i < edgeAmount; i++ {
			didx := src.OutEdges[i].Didx
			if comparePriority(hash(didx), myPriority, didx, internalId) {
				src.OutEdges[i].Property.Priority = true
				src.Property.NbrScratch[i] = MAGIC_VAL_MSG
				if USE_WAIT_COUNT_MSG {
					src.Property.WaitCount++ // If the edge has priority, we need to wait for it.
				}
			} else {
				src.Property.NbrScratch[i] = EMPTY_VAL_MSG
			}
		}
	}
	//if USE_WAIT_COUNT_MSG {
	//	src.Property.WaitCount++ // Wait for self.
	//}
	return m
}

// Self MessageMsg (init).
func (*ColouringMsg) InitAllMessage(src *graph.Vertex[VPropMsg, EPropMsg], internalId uint32, rawId graph.RawType) (m MessageMsg) {
	m.Init = true
	return m
}

func (*ColouringMsg) MessageMerge(incoming MessageMsg, sidx uint32, existing *MessageMsg) (newInfo bool) {
	if incoming.Init {
		*existing = incoming
	}
	return true
}

func (*ColouringMsg) MessageRetrieve(existing *MessageMsg, src *graph.Vertex[VPropMsg, EPropMsg]) (outgoing MessageMsg) {
	if existing.Init {
		existing.Init = false
		return MessageMsg{true}
	}
	return outgoing
}

func (alg *ColouringMsg) OnUpdateVertex(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], src *graph.Vertex[VPropMsg, EPropMsg], notif graph.Notification[NoteMsg], m MessageMsg) (sent uint64) {
	prop := &src.Property

	if m.Init { // Init Message, do not apply to neighbour.
		//if USE_WAIT_COUNT_MSG {
		//	prop.WaitCount-- // Done waiting for self.
		//}
	} else {
		ln := uint32(len(prop.NbrScratch))
		if notif.Note.Pos > ln {
			prop.NbrScratch = append(prop.NbrScratch, make([]uint32, ((notif.Note.Pos)-(ln)))...)
			for i := ln; i < notif.Note.Pos; i++ {
				prop.NbrScratch[i] = 0
			}
			panic(1)
		}
		prev := prop.NbrScratch[notif.Note.Pos]
		prop.NbrScratch[notif.Note.Pos] = notif.Note.Colour

		if notif.Note.Priority {
			if notif.Note.Colour == prop.Colour {
				prop.Colour = EMPTY_VAL_MSG // Have to re-colour.
			}
			if USE_WAIT_COUNT_MSG && prev == MAGIC_VAL_MSG { // Message was from a priority edge.
				prop.WaitCount--
			}
		}
	}

	if notif.Activity > 0 || (USE_WAIT_COUNT_MSG && prop.WaitCount > 0) {
		return 0
	}

	if prop.Colour == EMPTY_VAL_MSG {
		prop.ColoursIndexed.Zeroes()
		for i := range prop.NbrScratch {
			if prop.NbrScratch[i] <= uint32(len(prop.NbrScratch)) {
				if !prop.ColoursIndexed.QuickSet(prop.NbrScratch[i]) {
					prop.ColoursIndexed.Set(prop.NbrScratch[i])
				}
			}
		}
		prop.Colour = prop.ColoursIndexed.FirstUnused()
		for eidx := range src.OutEdges {
			didx := src.OutEdges[eidx].Didx
			priority := !(src.OutEdges[eidx].Property.Priority)

			n := graph.Notification[NoteMsg]{Target: didx, Note: NoteMsg{Pos: src.OutEdges[eidx].Pos, Colour: prop.Colour, Priority: priority}}
			vtm, tidx := g.NodeVertexMessages(didx)
			sent += g.EnsureSend(g.ActiveNotification(notif.Target, n, vtm, tidx))
		}
	}
	return sent
}

func (alg *ColouringMsg) OnEdgeAdd(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], src *graph.Vertex[VPropMsg, EPropMsg], sidx uint32, eidxStart int, m MessageMsg) (sent uint64) {
	panic("TODO")
}

func (alg *ColouringMsg) OnEdgeDel(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], src *graph.Vertex[VPropMsg, EPropMsg], sidx uint32, deletedEdges []graph.Edge[EPropMsg], m MessageMsg) (sent uint64) {
	panic("TODO")
}

func (*ColouringMsg) OnCheckCorrectness(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]) {
	// TODO

	ComputeGraphColouringMsgStat(g)
}

func ComputeGraphColouringMsgStat(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]) {
	maxColour := uint32(0)
	allColours := make([]uint32, 1, 64)
	g.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VPropMsg, EPropMsg]) {
		vColour := vertex.Property.Colour
		if int(vColour) >= len(allColours) {
			allColours = append(allColours, make([]uint32, int(vColour)+1-len(allColours))...)
		}
		allColours[vColour]++
		if vColour > maxColour {
			maxColour = vColour
		}
	})
	nColours := len(allColours)
	log.Info().Msg("Colour distribution (0 to " + utils.V(nColours) + "): ")
	log.Info().Msg(utils.V(allColours))
	log.Info().Msg("Max colour: " + utils.V(maxColour) + " Number of colours: " + utils.V(nColours) + " Ratio: " + utils.V(float64(maxColour+1)/float64(nColours)))
}
