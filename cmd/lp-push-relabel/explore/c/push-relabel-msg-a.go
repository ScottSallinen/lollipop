package c

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

type PushRelabelMsgA struct{}

type Nbr struct {
	Height int64
	ResCap int32
}

type InNbr struct {
	Nbr
	Pos  uint32 // Position of me in the neighbour
	Didx uint32
}

type VertexPMsgA struct {
	Type      VertexType
	Excess    int32
	Height    int64
	NewHeight int64
	InNbrs    []InNbr
}

type EdgePMsgA struct {
	graph.WeightedEdge
	Nbr
}

type MessageMsgA struct {
	Init bool
}

type NoteMsgA struct {
	SrcInternalId uint32 // FIXME: better if this is given by the framework
	Flow          int32  // Could be used for my position when hand-shaking
	Height        int64

	SrcPos            uint32 // position of the sender in the receiver's InNbrs or OutEdges
	SentForward       bool   // this message is sent across a forward edge (sender is in InNbrs)
	HandShake         bool
	NewMaxVertexCount bool // source needs to increase height, should not interpret other fields if this is set to true
}

func (VertexPMsgA) New() (new VertexPMsgA) {
	return new
}

func (MessageMsgA) New() (new MessageMsgA) {
	return new
}

func RunMsgA(options graph.GraphOptions) (maxFlow int32, g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA]) {
	alg := new(PushRelabelMsgA)
	g = graph.LaunchGraphExecution[*EdgePMsgA, VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA](alg, options)
	return alg.GetMaxFlowValue(g), g
}

func (pr *PushRelabelMsgA) GetMaxFlowValue(g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA]) int32 {
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	return sink.Property.Excess
}

func (pr *PushRelabelMsgA) InitAllMessage(vertex *graph.Vertex[VertexPMsgA, EdgePMsgA], internalId uint32, rawId graph.RawType) MessageMsgA {
	return MessageMsgA{Init: true}
}

func (pr *PushRelabelMsgA) BaseVertexMessage(v *graph.Vertex[VertexPMsgA, EdgePMsgA], internalId uint32, rawId graph.RawType) (m MessageMsgA) {
	v.Property.Height = InitialHeight
	if rawId == SourceRawId {
		v.Property.Type = Source
		v.Property.Height = VertexCountHelper.RegisterSource(internalId)
	} else if rawId == SinkRawId {
		v.Property.Type = Sink
		v.Property.Height = 0
	}
	v.Property.NewHeight = v.Property.Height

	for i := range v.OutEdges {
		v.OutEdges[i].Property.Height = InitialHeight
	}

	return m
}

func (*PushRelabelMsgA) MessageMerge(incoming MessageMsgA, sidx uint32, existing *MessageMsgA) (newInfo bool) {
	if incoming.Init {
		*existing = incoming
	}
	return true
}

func (*PushRelabelMsgA) MessageRetrieve(existing *MessageMsgA, vertex *graph.Vertex[VertexPMsgA, EdgePMsgA]) (outgoing MessageMsgA) {
	if existing.Init {
		existing.Init = false
		return MessageMsgA{true}
	}
	return outgoing
}

func (pr *PushRelabelMsgA) Init(g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA], v *graph.Vertex[VertexPMsgA, EdgePMsgA], internalId uint32) (sent uint64) {
	// Iterate over existing edges
	for eidx := range v.OutEdges {
		e := &v.OutEdges[eidx]
		if e.Didx == internalId || e.Property.Weight <= 0 { // TODO: also skip if the target is source
			continue
		}
		if v.Property.Type == Source {
			v.Property.Excess += int32(e.Property.Weight)
		}

		Assert(e.Property.ResCap == 0, "")
		e.Property.ResCap = int32(e.Property.Weight)
		e.Property.Height = InitialHeight

		// Initiate handshake
		vtm, tidx := g.NodeVertexMessages(e.Didx)
		sent += g.EnsureSend(g.ActiveNotification(internalId, graph.Notification[NoteMsgA]{
			Target: e.Didx,
			Note:   NoteMsgA{SrcInternalId: internalId, SrcPos: e.Pos, Height: v.Property.NewHeight, Flow: int32(eidx), SentForward: true, HandShake: true},
		}, vtm, tidx))
	}

	source := VertexCountHelper.NewVertex()
	if source != EmptyValue {
		vtm, tidx := g.NodeVertexMessages(source)
		sent += g.EnsureSend(g.ActiveNotification(internalId, graph.Notification[NoteMsgA]{
			Target: source,
			Note:   NoteMsgA{NewMaxVertexCount: true},
		}, vtm, tidx))
	}
	return
}

func (pr *PushRelabelMsgA) OnUpdateVertex(g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA], v *graph.Vertex[VertexPMsgA, EdgePMsgA], n graph.Notification[NoteMsgA], m MessageMsgA) (sent uint64) {
	if m.Init {
		sent += pr.Init(g, v, n.Target)
		var empty NoteMsgA
		if n.Note == empty { // FIXME: could be a real useful message
			return
		}
	}
	// newMaxVertexCount?
	if n.Note.NewMaxVertexCount {
		Assert(v.Property.Type != Source, "Non-source received NewMaxVertexCount")
		v.Property.NewHeight = VertexCountHelper.GetMaxVertexCount()
	} else {
		// Handle handshakes
		var nbr *Nbr
		var myPos uint32 // position of me in this neighbour
		if n.Note.HandShake {
			// Grow InNbrs and set new elements to InitialHeight if necessary
			if n.Note.SentForward && n.Note.SrcPos >= uint32(len(v.Property.InNbrs)) {
				ol := len(v.Property.InNbrs)
				nl := int(g.NodeVertexInEventPos(n.Target))
				v.Property.InNbrs = append(v.Property.InNbrs, make([]InNbr, nl-ol)...)
				for i := ol; i < nl; i++ {
					v.Property.InNbrs[i].Height = InitialHeight
				}
			}
			// send back my height
			if n.Note.SentForward {
				inNbr := &v.Property.InNbrs[n.Note.SrcPos]
				myPos = uint32(n.Note.Flow)
				n.Note.Flow = 0
				inNbr.Pos = myPos
				inNbr.Didx = n.Note.SrcInternalId
				vtm, tidx := g.NodeVertexMessages(n.Note.SrcInternalId)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsgA]{
					Target: n.Note.SrcInternalId,
					Note:   NoteMsgA{SrcInternalId: n.Target, SrcPos: myPos, Height: v.Property.NewHeight, SentForward: false, HandShake: true},
				}, vtm, tidx))
				nbr = &inNbr.Nbr
				Assert(n.Note.SrcInternalId == v.Property.InNbrs[n.Note.SrcPos].Didx, "")
			} else {
				nbr = &v.OutEdges[n.Note.SrcPos].Property.Nbr
				myPos = v.OutEdges[n.Note.SrcPos].Pos
				Assert(n.Note.SrcInternalId == v.OutEdges[n.Note.SrcPos].Didx, "")
			}
		} else {
			if n.Note.SentForward {
				nbr = &v.Property.InNbrs[n.Note.SrcPos].Nbr
				myPos = v.Property.InNbrs[n.Note.SrcPos].Pos
				Assert(n.Note.SrcInternalId == v.Property.InNbrs[n.Note.SrcPos].Didx, "")
			} else {
				nbr = &v.OutEdges[n.Note.SrcPos].Property.Nbr
				myPos = v.OutEdges[n.Note.SrcPos].Pos
				Assert(n.Note.SrcInternalId == v.OutEdges[n.Note.SrcPos].Didx, "")
			}
		}

		// Update height
		oldHeight := nbr.Height
		nbr.Height = n.Note.Height

		// handleFlow
		if n.Note.Flow < 0 {
			// retract request
			amount := utils.Max(n.Note.Flow, -nbr.ResCap)
			if amount < 0 {
				nbr.ResCap -= -amount
				v.Property.Excess -= -amount

				vtm, tidx := g.NodeVertexMessages(n.Note.SrcInternalId)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsgA]{
					Target: n.Note.SrcInternalId,
					Note:   NoteMsgA{SrcInternalId: n.Target, SrcPos: myPos, Height: v.Property.NewHeight, Flow: -amount, SentForward: !n.Note.SentForward},
				}, vtm, tidx))
			}
		} else if n.Note.Flow > 0 {
			// additional flow
			nbr.ResCap += n.Note.Flow
			v.Property.Excess += n.Note.Flow
		}

		// restoreHeightInvariant
		if n.Note.HandShake || oldHeight > nbr.Height || n.Note.Flow > 0 {
			if nbr.Height+1 < v.Property.NewHeight && nbr.ResCap > 0 {
				if v.Property.Excess > 0 {
					amount := utils.Min(v.Property.Excess, nbr.ResCap)
					v.Property.Excess -= amount
					nbr.ResCap -= amount

					vtm, tidx := g.NodeVertexMessages(n.Note.SrcInternalId)
					sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsgA]{
						Target: n.Note.SrcInternalId,
						Note:   NoteMsgA{SrcInternalId: n.Target, SrcPos: myPos, Height: v.Property.NewHeight, Flow: amount, SentForward: !n.Note.SentForward},
					}, vtm, tidx))
				}
				if nbr.ResCap > 0 {
					Assert(v.Property.Type != Source, "")
					v.Property.NewHeight = nbr.Height + 1
				}
			}
		}
	}

	// done with the message itself
	// Skip the rest if there are more incoming messages
	if n.Activity > 0 {
		return
	}

	// discharge
	if v.Property.Excess > 0 {
		if v.Property.Type == Normal {
			for v.Property.Excess > 0 {
				dischargeSent, nextHeight := pr.dischargeOnce(g, v, n)
				sent += dischargeSent
				if v.Property.Excess == 0 {
					break
				}
				// lift
				Assert(nextHeight != MaxHeight, "")
				v.Property.NewHeight = nextHeight
			}
		} else {
			// Cannot lift
			dischargeSent, _ := pr.dischargeOnce(g, v, n)
			sent += dischargeSent
		}
	} else if v.Property.Excess < 0 && v.Property.Type == Normal && v.Property.Height > 0 {
		v.Property.NewHeight = -VertexCountHelper.GetMaxVertexCount()
	}

	// broadcast heights if needed
	if v.Property.NewHeight != v.Property.Height {
		v.Property.Height = v.Property.NewHeight
		for _, inNbr := range v.Property.InNbrs {
			vtm, tidx := g.NodeVertexMessages(inNbr.Didx)
			sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsgA]{
				Target: inNbr.Didx,
				Note:   NoteMsgA{SrcInternalId: n.Target, SrcPos: inNbr.Pos, Height: v.Property.NewHeight, SentForward: false},
			}, vtm, tidx))
		}
		for _, e := range v.OutEdges {
			vtm, tidx := g.NodeVertexMessages(e.Didx)
			sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsgA]{
				Target: e.Didx,
				Note:   NoteMsgA{SrcInternalId: n.Target, SrcPos: e.Pos, Height: v.Property.NewHeight, SentForward: true},
			}, vtm, tidx))
		}
	}

	return
}

func (pr *PushRelabelMsgA) dischargeOnce(g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA], v *graph.Vertex[VertexPMsgA, EdgePMsgA], n graph.Notification[NoteMsgA]) (sent uint64, nextHeight int64) {
	nextHeight = int64(MaxHeight)
	for i := range v.Property.InNbrs {
		nbr := &v.Property.InNbrs[i].Nbr
		if nbr.ResCap > 0 {
			if !(v.Property.NewHeight > nbr.Height) {
				nextHeight = utils.Min(nextHeight, nbr.Height+1)
			} else {
				amount := utils.Min(v.Property.Excess, nbr.ResCap)
				v.Property.Excess -= amount
				nbr.ResCap -= amount
				Assert(amount > 0, "")

				inNbr := &v.Property.InNbrs[i]
				vtm, tidx := g.NodeVertexMessages(inNbr.Didx)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsgA]{
					Target: inNbr.Didx,
					Note:   NoteMsgA{SrcInternalId: n.Target, SrcPos: inNbr.Pos, Height: v.Property.NewHeight, Flow: amount, SentForward: false},
				}, vtm, tidx))

				if v.Property.Excess == 0 {
					return
				}
			}
		}
	}

	for i := range v.OutEdges {
		nbr := &v.OutEdges[i].Property
		if nbr.ResCap > 0 {
			if !(v.Property.NewHeight > nbr.Height) {
				nextHeight = utils.Min(nextHeight, nbr.Height+1)
			} else {
				amount := utils.Min(v.Property.Excess, nbr.ResCap)
				v.Property.Excess -= amount
				nbr.ResCap -= amount
				Assert(amount > 0, "")

				e := &v.OutEdges[i]
				vtm, tidx := g.NodeVertexMessages(e.Didx)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[NoteMsgA]{
					Target: e.Didx,
					Note:   NoteMsgA{SrcInternalId: n.Target, SrcPos: e.Pos, Height: v.Property.NewHeight, Flow: amount, SentForward: true},
				}, vtm, tidx))

				if v.Property.Excess == 0 {
					return
				}
			}
		}
	}
	return sent, nextHeight
}

func (pr *PushRelabelMsgA) OnEdgeAdd(g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA], src *graph.Vertex[VertexPMsgA, EdgePMsgA], sidx uint32, eidxStart int, m MessageMsgA) (sent uint64) {
	// TODO
	return
}

func (*PushRelabelMsgA) OnEdgeDel(g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA], src *graph.Vertex[VertexPMsgA, EdgePMsgA], sidx uint32, deletedEdges []graph.Edge[EdgePMsgA], m MessageMsgA) (sent uint64) {
	// TODO
	return 0
}

func (*PushRelabelMsgA) OnCheckCorrectness(g *graph.Graph[VertexPMsgA, EdgePMsgA, MessageMsgA, NoteMsgA]) {
	log.Info().Msg("Ensuring the vertex type is correct")
	sourceInternalId, source := g.NodeVertexFromRaw(SourceRawId)
	sinkInternalId, sink := g.NodeVertexFromRaw(SinkRawId)
	Assert(source.Property.Type == Source, "")
	Assert(sink.Property.Type == Sink, "")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VertexPMsgA, EdgePMsgA]) {
		if v.Property.Type != Normal {
			Assert(internalId == sourceInternalId || internalId == sinkInternalId, "")
		}
	})

	log.Info().Msg("Checking Pos are correct")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VertexPMsgA, EdgePMsgA]) {
		for i, e := range v.OutEdges {
			target := g.NodeVertex(e.Didx)
			inNbr := &target.Property.InNbrs[e.Pos]
			Assert(internalId == inNbr.Didx, "")
			Assert(uint32(i) == inNbr.Pos, "")
		}
		for i, inNbr := range v.Property.InNbrs {
			target := g.NodeVertex(inNbr.Didx)
			outEdge := &target.OutEdges[inNbr.Pos]
			Assert(internalId == outEdge.Didx, "")
			Assert(uint32(i) == outEdge.Pos, "")
		}
	})

	log.Info().Msg("Ensuring all messages are processed")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VertexPMsgA, EdgePMsgA]) {
		vtm, _ := g.NodeVertexMessages(internalId)
		Assert(vtm.Inbox.Init == false, "")
		Assert(v.Property.Height == v.Property.NewHeight, "")
	})

	log.Info().Msg("Checking the heights of the source and the sink")
	vertexCount := g.NodeVertexCount()
	Assert(source.Property.Height >= int64(vertexCount),
		fmt.Sprintf("Source height %d < # of vertices %d", source.Property.Height, vertexCount))
	Assert(sink.Property.Height == 0,
		fmt.Sprintf("Sink height %d != 0", sink.Property.Height))

	// Check Excess & residual capacity
	log.Info().Msg("Checking excess & residual capacity")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VertexPMsgA, EdgePMsgA]) {
		if v.Property.Type == Normal {
			Assert(v.Property.Excess == 0, "")
		}
		for _, inNbr := range v.Property.InNbrs {
			Assert(inNbr.ResCap >= 0, "")
		}
		for _, e := range v.OutEdges {
			Assert(e.Property.ResCap >= 0, "")
		}
	})

	log.Info().Msg("Checking sum of edge capacities in the original graph == in the residual graph")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VertexPMsgA, EdgePMsgA]) {
		capacityOriginal := int64(0)
		capacityResidual := int64(0)
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight == 0 { // TODO: also skip if the target is source
				continue
			}
			capacityOriginal += int64(e.Property.Weight)
		}

		for _, inNbr := range v.Property.InNbrs {
			capacityResidual += int64(inNbr.ResCap)
		}
		for _, e := range v.OutEdges {
			capacityResidual += int64(e.Property.ResCap)
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
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VertexPMsgA, EdgePMsgA]) {
		for _, inNbr := range v.Property.InNbrs {
			Assert(inNbr.Height == g.NodeVertex(inNbr.Didx).Property.Height, "")
		}
		for _, e := range v.OutEdges {
			Assert(e.Property.Height == g.NodeVertex(e.Didx).Property.Height, "")
		}
	})

	log.Info().Msg("Checking height invariants")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VertexPMsgA, EdgePMsgA]) {
		h := v.Property.Height
		for _, inNbr := range v.Property.InNbrs {
			if inNbr.ResCap > 0 {
				Assert(h <= inNbr.Height+1, "")
			}
		}
		for _, e := range v.OutEdges {
			if e.Property.ResCap > 0 {
				Assert(h <= e.Property.Height+1, "")
			}
		}
	})

	// TODO: Print # of vertices in flow
}
