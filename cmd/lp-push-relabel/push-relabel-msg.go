package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
	"math"
)

type PushRelabelMsg struct{}

type VPropMsg struct {
	Type       VertexType
	Height     int64
	Excess     int32
	ResCap     map[uint32]int32
	NbrHeights map[uint32]int64

	NewHeight int64
}

type EPropMsg struct {
	graph.WeightedEdge
}

type MessageMsg struct {
	Init bool
}

type NoteMsg struct {
	Src    uint32
	Flow   int32 // flow, -
	Height int64 // height of sender, -

	NewMaxVertexCount bool // source needs to increase height
}

func (VPropMsg) New() (new VPropMsg) {
	new.ResCap = make(map[uint32]int32)
	new.NbrHeights = make(map[uint32]int64)
	return new
}

func (MessageMsg) New() (new MessageMsg) {
	return new
}

func (pr *PushRelabelMsg) GetMaxFlowValue(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]) int32 {
	_, sink := g.NodeVertexFromRaw(sinkRawId)
	return sink.Property.Excess
}

func (pr *PushRelabelMsg) InitAllMessage(vertex *graph.Vertex[VPropMsg, EPropMsg], internalId uint32, rawId graph.RawType) MessageMsg {
	return MessageMsg{Init: true}
}

func (pr *PushRelabelMsg) BaseVertexMessage(v *graph.Vertex[VPropMsg, EPropMsg], internalId uint32, rawId graph.RawType) (m MessageMsg) {
	v.Property.Height = initialHeight
	v.Property.ResCap = make(map[uint32]int32)

	if rawId == sourceRawId {
		v.Property.Type = Source
		v.Property.Height = VertexCountHelper.RegisterSource(internalId)
	} else if rawId == sinkRawId {
		v.Property.Type = Sink
		v.Property.Height = 0
	}
	v.Property.NewHeight = v.Property.Height

	return m
}

func (*PushRelabelMsg) MessageMerge(incoming MessageMsg, sidx uint32, existing *MessageMsg) (newInfo bool) {
	if incoming.Init {
		*existing = incoming
	}
	return true
}

func (*PushRelabelMsg) MessageRetrieve(existing *MessageMsg, vertex *graph.Vertex[VPropMsg, EPropMsg]) (outgoing MessageMsg) {
	if existing.Init {
		existing.Init = false
		return MessageMsg{true}
	}
	return outgoing
}

func (pr *PushRelabelMsg) Init(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], v *graph.Vertex[VPropMsg, EPropMsg], internalId uint32) (sent uint64) {
	// Iterate over existing edges
	for _, e := range v.OutEdges {
		if e.Didx == internalId || e.Property.Weight <= 0 { // TODO: also skip if the target is source
			continue
		}

		v.Property.ResCap[e.Didx] += int32(e.Property.Weight)

		// new neighbour?
		nbrHeight, isOldNbr := v.Property.NbrHeights[e.Didx]
		if !isOldNbr {
			outNotif := graph.Notification[NoteMsg]{
				Target: e.Didx,
				Note:   NoteMsg{Src: internalId, Flow: 0, Height: v.Property.NewHeight},
			}
			vtm, tidx := g.NodeVertexMessages(e.Didx)
			sent += g.EnsureSend(g.ActiveNotification(internalId, outNotif, vtm, tidx))
			v.Property.NbrHeights[e.Didx] = initialHeight
			nbrHeight = initialHeight
		}

		// restoreHeightInvariant
		maxHeight, resCap := nbrHeight+1, v.Property.ResCap[e.Didx]
		if maxHeight < v.Property.NewHeight && resCap > 0 {
			if v.Property.Excess > 0 {
				amount := utils.Min(v.Property.Excess, resCap)
				v.Property.Excess -= amount
				resCap -= amount
				v.Property.ResCap[e.Didx] = resCap

				outNotif := graph.Notification[NoteMsg]{
					Target: e.Didx,
					Note:   NoteMsg{Src: internalId, Flow: amount, Height: v.Property.NewHeight},
				}
				vtm, tidx := g.NodeVertexMessages(e.Didx)
				sent += g.EnsureSend(g.ActiveNotification(internalId, outNotif, vtm, tidx))
			}
			if resCap > 0 {
				assert(v.Property.Type != Source, "")
				v.Property.NewHeight = maxHeight
			}
		}

		if v.Property.Type == Source {
			v.Property.Excess += int32(e.Property.Weight)
		}
	}

	source := VertexCountHelper.NewVertex()
	if source != math.MaxUint32 {
		outNotif := graph.Notification[NoteMsg]{
			Target: source,
			Note:   NoteMsg{Src: internalId, Height: v.Property.NewHeight, NewMaxVertexCount: true},
		}
		vtm, tidx := g.NodeVertexMessages(source)
		sent += g.EnsureSend(g.ActiveNotification(internalId, outNotif, vtm, tidx))
	}
	return
}

func (pr *PushRelabelMsg) OnUpdateVertex(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], v *graph.Vertex[VPropMsg, EPropMsg], n graph.Notification[NoteMsg], m MessageMsg) (sent uint64) {
	if m.Init {
		sent += pr.Init(g, v, n.Target)
	}

	// newMaxVertexCount
	if n.Note.NewMaxVertexCount {
		if v.Property.Type != Source {
			log.Panic().Msg("Non-source received NewMaxVertexCount")
		}
		v.Property.NewHeight = VertexCountHelper.GetMaxVertexCount()
	}

	// handleFlow
	if n.Note.Flow < 0 {
		// retract request
		amount := utils.Max(n.Note.Flow, -v.Property.ResCap[n.Note.Src])
		if amount < 0 {
			v.Property.ResCap[n.Note.Src] -= -amount
			v.Property.Excess -= -amount

			outNotif := graph.Notification[NoteMsg]{
				Target: n.Note.Src,
				Note:   NoteMsg{Src: n.Target, Flow: -amount, Height: v.Property.NewHeight},
			}
			vtm, tidx := g.NodeVertexMessages(n.Note.Src)
			sent += g.EnsureSend(g.ActiveNotification(n.Target, outNotif, vtm, tidx))
		}
	} else if n.Note.Flow > 0 {
		// additional flow
		v.Property.ResCap[n.Note.Src] += n.Note.Flow
		v.Property.Excess += n.Note.Flow
	}

	// new neighbour?
	oldHeight, isOldNbr := v.Property.NbrHeights[n.Note.Src]
	v.Property.NbrHeights[n.Note.Src] = n.Note.Height
	if !isOldNbr {
		v.Property.ResCap[n.Note.Src] += 0 // ensure it has an entry in ResCap
		outNotif := graph.Notification[NoteMsg]{
			Target: n.Note.Src,
			Note:   NoteMsg{Src: n.Target, Flow: 0, Height: v.Property.NewHeight},
		}
		vtm, tidx := g.NodeVertexMessages(n.Note.Src)
		sent += g.EnsureSend(g.ActiveNotification(n.Target, outNotif, vtm, tidx))
	}

	// restoreHeightInvariant
	if !isOldNbr || oldHeight > n.Note.Height || n.Note.Flow > 0 {
		maxHeight, resCap := v.Property.NbrHeights[n.Note.Src]+1, v.Property.ResCap[n.Note.Src]
		if maxHeight < v.Property.NewHeight && resCap > 0 {
			if v.Property.Excess > 0 {
				amount := utils.Min(v.Property.Excess, resCap)
				v.Property.Excess -= amount
				resCap -= amount
				v.Property.ResCap[n.Note.Src] = resCap

				outNotif := graph.Notification[NoteMsg]{
					Target: n.Note.Src,
					Note:   NoteMsg{Src: n.Target, Flow: amount, Height: v.Property.NewHeight},
				}
				vtm, tidx := g.NodeVertexMessages(n.Note.Src)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, outNotif, vtm, tidx))
			}
			if resCap > 0 {
				assert(v.Property.Type != Source, "")
				v.Property.NewHeight = maxHeight
			}
		}
	}

	// Skip the rest if there are more incoming messages
	if n.Activity > 0 {
		return
	}

	// discharge
	if v.Property.Excess > 0 {
		if v.Property.Type == Normal {
		excessFor:
			for v.Property.Excess > 0 {
				nextHeight := int64(MaxHeight)
				for id, rc := range v.Property.ResCap {
					if rc > 0 {
						h := v.Property.NbrHeights[id]
						if !(v.Property.NewHeight > h) {
							nextHeight = utils.Min(nextHeight, h+1)
							continue
						}
						amount := utils.Min(v.Property.Excess, rc)
						v.Property.Excess -= amount
						v.Property.ResCap[id] -= amount
						assert(amount > 0, "")

						outNotif := graph.Notification[NoteMsg]{
							Target: id,
							Note:   NoteMsg{Src: n.Target, Flow: amount, Height: v.Property.NewHeight},
						}
						vtm, tidx := g.NodeVertexMessages(id)
						sent += g.EnsureSend(g.ActiveNotification(n.Target, outNotif, vtm, tidx))

						if v.Property.Excess == 0 {
							break excessFor
						}
					}
				}
				assert(nextHeight != MaxHeight, "")
				v.Property.NewHeight = nextHeight
			}
		} else {
			// Cannot lift
			for id, rc := range v.Property.ResCap {
				if rc > 0 && v.Property.NewHeight > v.Property.NbrHeights[id] {
					amount := utils.Min(v.Property.Excess, rc)
					v.Property.Excess -= amount
					v.Property.ResCap[id] -= amount

					outNotif := graph.Notification[NoteMsg]{
						Target: id,
						Note:   NoteMsg{Src: n.Target, Flow: amount, Height: v.Property.NewHeight},
					}
					vtm, tidx := g.NodeVertexMessages(id)
					sent += g.EnsureSend(g.ActiveNotification(n.Target, outNotif, vtm, tidx))

					if v.Property.Excess == 0 {
						break
					}
				}
			}
		}
	} else if v.Property.Excess < 0 && v.Property.Type == Normal && v.Property.Height > 0 {
		v.Property.NewHeight = -VertexCountHelper.GetMaxVertexCount()
	}

	// broadcast heights if needed
	if v.Property.NewHeight != v.Property.Height {
		noteMsg := NoteMsg{Src: n.Target, Height: v.Property.NewHeight}
		for nbrId := range v.Property.ResCap {
			vtm, tidx := g.NodeVertexMessages(nbrId)
			sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsg]{Target: nbrId, Note: noteMsg}, vtm, tidx))
		}
		v.Property.Height = v.Property.NewHeight
	}

	return
}

func (pr *PushRelabelMsg) OnEdgeAdd(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], src *graph.Vertex[VPropMsg, EPropMsg], sidx uint32, eidxStart int, m MessageMsg) (sent uint64) {
	// TODO
	return 0
}

func (*PushRelabelMsg) OnEdgeDel(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], src *graph.Vertex[VPropMsg, EPropMsg], sidx uint32, deletedEdges []graph.Edge[EPropMsg], m MessageMsg) (sent uint64) {
	// TODO
	return 0
}

func (*PushRelabelMsg) OnCheckCorrectness(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]) {
	log.Info().Msg("Ensuring the vertex type is correct")
	sourceInternalId, source := g.NodeVertexFromRaw(sourceRawId)
	sinkInternalId, sink := g.NodeVertexFromRaw(sinkRawId)
	assert(source.Property.Type == Source, "")
	assert(sink.Property.Type == Sink, "")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		if v.Property.Type != Normal {
			assert(internalId == sourceInternalId || internalId == sinkInternalId, "")
		}
	})

	log.Info().Msg("Ensuring all messages are processed")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		vtm, _ := g.NodeVertexMessages(internalId)
		assert(vtm.Inbox.Init == false, "")
		assert(v.Property.Height == v.Property.NewHeight, "")
		assert(len(v.Property.ResCap) == len(v.Property.NbrHeights), "")
	})

	log.Info().Msg("Checking the heights of the source and the sink")
	vertexCount := g.NodeVertexCount()
	assert(source.Property.Height >= int64(vertexCount),
		fmt.Sprintf("Source height %d < # of vertices %d", source.Property.Height, vertexCount))
	assert(sink.Property.Height == 0,
		fmt.Sprintf("Sink height %d != 0", sink.Property.Height))

	// Check Excess & residual capacity
	log.Info().Msg("Checking excess & residual capacity")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		if v.Property.Type == Normal {
			assert(v.Property.Excess == 0, "")
		}
		for n, rc := range v.Property.ResCap {
			assert(rc >= 0, fmt.Sprintf("%d -> %d has negative residual capacity (%d)", internalId, n, rc))
		}
	})

	log.Info().Msg("Checking sum of edge capacities in the original graph == in the residual graph")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		capacityOriginal := int64(0)
		capacityResidual := int64(0)
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight == 0 { // TODO: also skip if the target is source
				continue
			}
			capacityOriginal += int64(e.Property.Weight)
		}
		for _, rc := range v.Property.ResCap {
			capacityResidual += int64(rc)
		}
		if v.Property.Type == Source {
			assert(int64(v.Property.Excess) == capacityResidual, "")
		} else if v.Property.Type == Sink {
			assert(capacityOriginal+int64(v.Property.Excess) == capacityResidual, "")
		} else {
			assert(capacityOriginal == capacityResidual, "")
		}
	})

	log.Info().Msg("Checking sourceOut and sinkIn")
	sourceOut := int64(0)
	for _, e := range source.OutEdges {
		sourceOut += int64(e.Property.Weight)
	}
	sourceOut -= int64(source.Property.Excess)
	sinkIn := int64(sink.Property.Excess)
	assert(sourceOut == sinkIn, "")
	log.Info().Msg(fmt.Sprintf("Maximum flow from %d to %d is %d", sourceRawId, sinkRawId, sourceOut))

	log.Info().Msg("Ensuring NbrHeight is accurate")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		for n, h := range v.Property.NbrHeights {
			realHeight := g.NodeVertex(n).Property.Height
			assert(h == realHeight, "")
		}
	})

	log.Info().Msg("Checking height invariants")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		h := v.Property.Height
		for n, rc := range v.Property.ResCap {
			if rc > 0 {
				assert(h <= v.Property.NbrHeights[n]+1, "")
			}
		}
	})

	// TODO: Print # of vertices in flow
}
