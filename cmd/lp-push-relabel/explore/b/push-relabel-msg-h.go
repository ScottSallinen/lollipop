package b

import (
	"fmt"
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

type PushRelabelMsg struct{}

type VPropMsg struct {
	Type          VertexType
	Height        int64
	Excess        int32
	ResCap        map[uint32]int32
	NbrHeights    map[uint32]int64
	HeightChanged bool
}

type EPropMsg struct {
	graph.TimestampWeightedEdge
}

type MessageMsg struct {
	Init bool
}

type NoteMsg struct {
	Src    uint32
	Flow   int32
	Height int64

	NewMaxVertexCount bool // source needs to increase height
}

type Graph = graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]
type Vertex = graph.Vertex[VPropMsg, EPropMsg]
type Edge = graph.Edge[EPropMsg]

func (VPropMsg) New() (new VPropMsg) {
	new.ResCap = make(map[uint32]int32)
	new.NbrHeights = make(map[uint32]int64)
	return new
}

func (MessageMsg) New() (new MessageMsg) {
	return new
}

func RunMsgH(options graph.GraphOptions) (maxFlow int32, g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]) {
	alg := new(PushRelabelMsg)
	g = graph.LaunchGraphExecution[*EPropMsg, VPropMsg, EPropMsg, MessageMsg, NoteMsg](alg, options, nil, nil)
	return alg.GetMaxFlowValue(g), g
}

func (pr *PushRelabelMsg) GetMaxFlowValue(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]) int32 {
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	return sink.Property.Excess
}

func (pr *PushRelabelMsg) InitAllMail(vertex *graph.Vertex[VPropMsg, EPropMsg], internalId uint32, rawId graph.RawType) MessageMsg {
	return MessageMsg{Init: true}
}

func (pr *PushRelabelMsg) BaseVertexMailbox(v *graph.Vertex[VPropMsg, EPropMsg], internalId uint32, s *graph.VertexStructure) (m MessageMsg) {
	v.Property.Height = InitialHeight
	v.Property.ResCap = make(map[uint32]int32)

	if s.RawId == SourceRawId {
		v.Property.Type = Source
		v.Property.Height = VertexCountHelper.RegisterSource(internalId)
	} else if s.RawId == SinkRawId {
		v.Property.Type = Sink
		v.Property.Height = 0
	}

	return m
}

func (*PushRelabelMsg) MailMerge(incoming MessageMsg, sidx uint32, existing *MessageMsg) (newInfo bool) {
	if incoming.Init {
		*existing = incoming
	}
	return true
}

func (*PushRelabelMsg) MailRetrieve(existing *MessageMsg, vertex *graph.Vertex[VPropMsg, EPropMsg]) (outgoing MessageMsg) {
	if existing.Init {
		existing.Init = false
		return MessageMsg{true}
	}
	return outgoing
}

func (pr *PushRelabelMsg) restoreHeightInvariant(g *Graph, v *Vertex, myId, nbrId uint32, nbrHeight int64) (sent uint64) {
	resCap := v.Property.ResCap[nbrId]
	if resCap > 0 && v.Property.Height > nbrHeight+1 {
		if v.Property.Excess > 0 {
			amount := utils.Min(v.Property.Excess, resCap)
			v.Property.Excess -= amount
			resCap -= amount
			v.Property.ResCap[nbrId] = resCap

			mailbox, tidx := g.NodeVertexMailbox(nbrId)
			sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[NoteMsg]{
				Target: nbrId,
				Note:   NoteMsg{Src: myId, Flow: amount, Height: v.Property.Height},
			}, mailbox, tidx))
		}
		if resCap > 0 {
			Assert(v.Property.Type != Source, "")
			v.Property.Height = nbrHeight + 1
			v.Property.HeightChanged = true
		}
	}
	return
}

func (pr *PushRelabelMsg) finalizeVertexState(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	// discharge
	if v.Property.Excess > 0 {
		if v.Property.Type == Normal {
		excessFor:
			for v.Property.Excess > 0 {
				nextHeight := int64(MaxHeight)
				for id, rc := range v.Property.ResCap {
					if rc > 0 {
						h := v.Property.NbrHeights[id]
						if !(v.Property.Height > h) {
							nextHeight = utils.Min(nextHeight, h+1)
							continue
						}
						amount := utils.Min(v.Property.Excess, rc)
						v.Property.Excess -= amount
						v.Property.ResCap[id] -= amount
						Assert(amount > 0, "")

						outNotif := graph.Notification[NoteMsg]{
							Target: id,
							Note:   NoteMsg{Src: myId, Flow: amount, Height: v.Property.Height},
						}
						mailbox, tidx := g.NodeVertexMailbox(id)
						sent += g.EnsureSend(g.ActiveNotification(myId, outNotif, mailbox, tidx))

						if v.Property.Excess == 0 {
							break excessFor
						}
					}
				}
				Assert(nextHeight != MaxHeight, "")
				v.Property.Height = nextHeight
				v.Property.HeightChanged = true
			}
		} else {
			// Cannot lift
			for id, rc := range v.Property.ResCap {
				if rc > 0 && v.Property.Height > v.Property.NbrHeights[id] {
					amount := utils.Min(v.Property.Excess, rc)
					v.Property.Excess -= amount
					v.Property.ResCap[id] -= amount

					outNotif := graph.Notification[NoteMsg]{
						Target: id,
						Note:   NoteMsg{Src: myId, Flow: amount, Height: v.Property.Height},
					}
					mailbox, tidx := g.NodeVertexMailbox(id)
					sent += g.EnsureSend(g.ActiveNotification(myId, outNotif, mailbox, tidx))

					if v.Property.Excess == 0 {
						break
					}
				}
			}
		}
	} else if v.Property.Excess < 0 && v.Property.Type == Normal && v.Property.Height > 0 {
		v.Property.Height = -VertexCountHelper.GetMaxVertexCount()
		v.Property.HeightChanged = true
	}

	// broadcast heights if needed
	if v.Property.HeightChanged {
		v.Property.HeightChanged = false
		noteMsg := NoteMsg{Src: myId, Height: v.Property.Height}
		for nbrId := range v.Property.ResCap {
			mailbox, tidx := g.NodeVertexMailbox(nbrId)
			sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[NoteMsg]{Target: nbrId, Note: noteMsg}, mailbox, tidx))
		}
	}
	return
}

func (pr *PushRelabelMsg) Init(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], v *graph.Vertex[VPropMsg, EPropMsg], internalId uint32) (sent uint64) {
	// Iterate over existing edges
	for _, e := range v.OutEdges {
		if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == VertexCountHelper.GetSourceId() {
			continue
		}

		if v.Property.Type == Source {
			v.Property.Excess += int32(e.Property.Weight)
		}

		v.Property.ResCap[e.Didx] += int32(e.Property.Weight)

		// new neighbour?
		nbrHeight, isOldNbr := v.Property.NbrHeights[e.Didx]
		if !isOldNbr {
			outNotif := graph.Notification[NoteMsg]{
				Target: e.Didx,
				Note:   NoteMsg{Src: internalId, Flow: 0, Height: v.Property.Height},
			}
			mailbox, tidx := g.NodeVertexMailbox(e.Didx)
			sent += g.EnsureSend(g.ActiveNotification(internalId, outNotif, mailbox, tidx))
			v.Property.NbrHeights[e.Didx] = InitialHeight
			nbrHeight = InitialHeight
		}

		// restoreHeightInvariant
		sent += pr.restoreHeightInvariant(g, v, internalId, e.Didx, nbrHeight)
	}

	source := VertexCountHelper.NewVertex()
	if source != math.MaxUint32 {
		outNotif := graph.Notification[NoteMsg]{
			Target: source,
			Note:   NoteMsg{Src: internalId, Height: v.Property.Height, NewMaxVertexCount: true},
		}
		mailbox, tidx := g.NodeVertexMailbox(source)
		sent += g.EnsureSend(g.ActiveNotification(internalId, outNotif, mailbox, tidx))
	}
	return
}

func (pr *PushRelabelMsg) OnUpdateVertex(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], v *graph.Vertex[VPropMsg, EPropMsg], n graph.Notification[NoteMsg], m MessageMsg) (sent uint64) {
	if m.Init {
		sent += pr.Init(g, v, n.Target)
		var empty NoteMsg
		if n.Note == empty { // FIXME: could be a real useful message
			return
		}
	}
	if n.Note.Src == n.Target {
		return
	}

	// newMaxVertexCount
	if n.Note.NewMaxVertexCount {
		if v.Property.Type != Source {
			log.Panic().Msg("Non-source received NewMaxVertexCount")
		}
		v.Property.Height = VertexCountHelper.GetMaxVertexCount()
		v.Property.HeightChanged = true
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
				Note:   NoteMsg{Src: n.Target, Flow: -amount, Height: v.Property.Height},
			}
			mailbox, tidx := g.NodeVertexMailbox(n.Note.Src)
			sent += g.EnsureSend(g.ActiveNotification(n.Target, outNotif, mailbox, tidx))
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
			Note:   NoteMsg{Src: n.Target, Flow: 0, Height: v.Property.Height},
		}
		mailbox, tidx := g.NodeVertexMailbox(n.Note.Src)
		sent += g.EnsureSend(g.ActiveNotification(n.Target, outNotif, mailbox, tidx))
	}

	// restoreHeightInvariant
	if !isOldNbr || oldHeight > n.Note.Height || n.Note.Flow > 0 {
		sent += pr.restoreHeightInvariant(g, v, n.Target, n.Note.Src, v.Property.NbrHeights[n.Note.Src])
	}

	// Skip the rest if there are more incoming messages
	if n.Activity > 0 {
		return
	}

	sent += pr.finalizeVertexState(g, v, n.Target)

	return
}

func (pr *PushRelabelMsg) OnEdgeAdd(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], src *graph.Vertex[VPropMsg, EPropMsg], sidx uint32, eidxStart int, m MessageMsg) (sent uint64) {
	if m.Init {
		sent += pr.Init(g, src, sidx)
	} else {
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			e := &src.OutEdges[eidx]
			if e.Didx == sidx || e.Property.Weight <= 0 || e.Didx == VertexCountHelper.GetSourceId() {
				continue
			}

			src.Property.ResCap[e.Didx] += int32(e.Property.Weight)
			if src.Property.Type == Source {
				src.Property.Excess += int32(e.Property.Weight)
			}

			// new neighbour?
			nbrHeight, isOldNbr := src.Property.NbrHeights[e.Didx]
			if !isOldNbr {
				outNotif := graph.Notification[NoteMsg]{
					Target: e.Didx,
					Note:   NoteMsg{Src: sidx, Flow: 0, Height: src.Property.Height},
				}
				mailbox, tidx := g.NodeVertexMailbox(e.Didx)
				sent += g.EnsureSend(g.ActiveNotification(sidx, outNotif, mailbox, tidx))
				src.Property.NbrHeights[e.Didx] = InitialHeight
				nbrHeight = InitialHeight
			}

			// restoreHeightInvariant
			sent += pr.restoreHeightInvariant(g, src, sidx, e.Didx, nbrHeight)
		}
	}
	sent += pr.finalizeVertexState(g, src, sidx)
	return
}

func (*PushRelabelMsg) OnEdgeDel(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg], src *graph.Vertex[VPropMsg, EPropMsg], sidx uint32, deletedEdges []graph.Edge[EPropMsg], m MessageMsg) (sent uint64) {
	// TODO
	return 0
}

func (*PushRelabelMsg) OnCheckCorrectness(g *graph.Graph[VPropMsg, EPropMsg, MessageMsg, NoteMsg]) {
	log.Info().Msg("Ensuring the vertex type is correct")
	sourceInternalId, source := g.NodeVertexFromRaw(SourceRawId)
	sinkInternalId, sink := g.NodeVertexFromRaw(SinkRawId)
	Assert(source.Property.Type == Source, "")
	Assert(sink.Property.Type == Sink, "")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		if v.Property.Type != Normal {
			Assert(internalId == sourceInternalId || internalId == sinkInternalId, "")
		}
	})

	log.Info().Msg("Ensuring all messages are processed")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		mailbox, _ := g.NodeVertexMailbox(internalId)
		Assert(mailbox.Inbox.Init == false, "")
		Assert(v.Property.HeightChanged == false, "")
		Assert(len(v.Property.ResCap) == len(v.Property.NbrHeights), "")
	})

	log.Info().Msg("Checking the heights of the source and the sink")
	vertexCount := g.NodeVertexCount()
	Assert(source.Property.Height >= int64(vertexCount),
		fmt.Sprintf("Source height %d < # of vertices %d", source.Property.Height, vertexCount))
	Assert(sink.Property.Height == 0,
		fmt.Sprintf("Sink height %d != 0", sink.Property.Height))

	// Check Excess & residual capacity
	log.Info().Msg("Checking excess & residual capacity")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		if v.Property.Type == Normal {
			Assert(v.Property.Excess == 0, "")
		}
		for n, rc := range v.Property.ResCap {
			Assert(rc >= 0, fmt.Sprintf("%d -> %d has negative residual capacity (%d)", internalId, n, rc))
		}
	})

	log.Info().Msg("Checking sum of edge capacities in the original graph == in the residual graph")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		capacityOriginal := int64(0)
		capacityResidual := int64(0)
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == VertexCountHelper.GetSourceId() {
				continue
			}
			capacityOriginal += int64(e.Property.Weight)
		}
		for _, rc := range v.Property.ResCap {
			capacityResidual += int64(rc)
		}
		if v.Property.Type == Source {
			Assert(int64(v.Property.Excess) == capacityResidual, "")
		} else if v.Property.Type == Sink {
			Assert(capacityOriginal+int64(v.Property.Excess) == capacityResidual, "")
		} else {
			Assert(capacityOriginal == capacityResidual, "")
		}
	})

	log.Info().Msg("Checking sourceOut and sinkIn")
	sourceOut := int64(0)
	for _, e := range source.OutEdges {
		sourceOut += int64(e.Property.Weight)
	}
	sourceOut -= int64(source.Property.Excess)
	sinkIn := int64(sink.Property.Excess)
	Assert(sourceOut == sinkIn, "")
	log.Info().Msg(fmt.Sprintf("Maximum flow from %d to %d is %d", SourceRawId, SinkRawId, sourceOut))

	log.Info().Msg("Ensuring NbrHeight is accurate")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		for n, h := range v.Property.NbrHeights {
			realHeight := g.NodeVertex(n).Property.Height
			Assert(h == realHeight, "")
		}
	})

	log.Info().Msg("Checking height invariants")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropMsg, EPropMsg]) {
		h := v.Property.Height
		for n, rc := range v.Property.ResCap {
			if rc > 0 {
				Assert(h <= v.Property.NbrHeights[n]+1, "")
			}
		}
	})

	// TODO: Print # of vertices in flow
}
