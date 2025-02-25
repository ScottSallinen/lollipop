package main

import (
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
)

type PushRelabel struct {
	// Config
	SourceRawId   graph.RawType
	SinkRawId     graph.RawType
	HandleDeletes bool

	Gr          GlobalRelabel
	VertexCount VertexCount
	MsgCounter  ThreadMsgCounter[int64]

	SourceId     atomic.Uint32
	SinkId       atomic.Uint32
	SourceSupply int64
}

type Neighbour struct {
	HeightPos uint32
	HeightNeg uint32
	ResCapOut int64
	ResCapIn  int64
	Pos       int32 // Position of me in the neighbour, -1 means unknown
	Didx      uint32
}

type VertexProp struct {
	Type             VertexType
	Excess           int64
	HeightPos        uint32
	HeightNeg        uint32
	HeightPosChanged bool
	HeightNegChanged bool

	Nbrs            []Neighbour
	NbrMap          map[uint32]int32 // Id -> Pos
	UnknownPosCount uint32           // shouldn't do anything if it's not 0, EmptyValue means init
}

type EdgeProp struct {
	graph.TimestampWeightedEdge
}

type Mail struct{}

type Note struct {
	Flow      int64 // Flow, ResCapOffset
	HeightPos uint32
	HeightNeg uint32
	SrcPos    int32 // If PosType >= 0, position of the sender in the receiver's array. Otherwise, the internal ID of the sender
	PosType   uint32
}

type Graph = graph.Graph[VertexProp, EdgeProp, Mail, Note]
type GraphThread = graph.GraphThread[VertexProp, EdgeProp, Mail, Note]
type Vertex = graph.Vertex[VertexProp, EdgeProp]
type Edge = graph.Edge[EdgeProp]

const (
	Name = "PushRelabel (N)"

	TypePosMask       = 0b11
	NewNbr            = 0b0001 // + Pos<<2
	NbrPos            = 0b0010 // + Pos<<2
	UpdateInCapPos    = 0b0011
	UpdateInCapId     = 0b1011
	NewMaxVertexCount = 0b0111
)

func getNegativeExcessVertexHeight(vc *VertexCount) uint32 {
	// A vertex should be given a valid height (<MaxHeight) so that it can pull positive
	// flow in a isolated component with no s/t.
	// Ideally, for a vertex with a negative excess, we would like to set its height to a
	// value equals to the distance to t after the negative excess has been returned to t.
	return 0
}

func (v *VertexProp) resetHeights(vc *VertexCount) (HeightPosChanged, HeightNegChanged bool) {
	v.HeightPos, v.HeightNeg = MaxHeight, MaxHeight
	if v.Type == Source {
		v.HeightPos, v.HeightNeg = uint32(vc.GetMaxVertexCount()), 0
		return true, true
	} else if v.Type == Sink {
		v.HeightPos, v.HeightNeg = 0, uint32(vc.GetMaxVertexCount())
		return true, true
	} else if v.Excess < 0 {
		v.HeightPos = getNegativeExcessVertexHeight(vc)
		return true, false
	}
	return false, false
}

func (v *VertexProp) updateHeightPos(heightPos uint32) {
	v.HeightPos = heightPos
	v.HeightPosChanged = true
}

func (v *VertexProp) updateHeightNeg(heightNeg uint32) {
	v.HeightNeg = heightNeg
	v.HeightNegChanged = true
}

func (pr *PushRelabel) updateResCapOut(nbr *Neighbour, delta int64, sendHeightNeg *bool) {
	oldResCapOut := nbr.ResCapOut
	nbr.ResCapOut += delta
	if pr.HandleDeletes && oldResCapOut <= 0 && nbr.ResCapOut > 0 {
		*sendHeightNeg = true
	}
}

func (pr *PushRelabel) updateResCapIn(nbr *Neighbour, delta int64, sendHeightPos *bool) {
	oldResCapIn := nbr.ResCapIn
	nbr.ResCapIn += delta
	if oldResCapIn <= 0 && nbr.ResCapIn > 0 {
		*sendHeightPos = true
	}
}

func (pr *PushRelabel) updateFlowSH(vp *VertexProp, nbr *Neighbour, amount int64, sendHeightPos *bool, sendHeightNeg *bool) {
	pr.updateResCapOut(nbr, -amount, sendHeightNeg)
	pr.updateResCapIn(nbr, amount, sendHeightPos)
	vp.Excess -= amount
}

func (pr *PushRelabel) updateFlow(vp *VertexProp, nbr *Neighbour, amount int64) {
	nbr.ResCapOut -= amount
	nbr.ResCapIn += amount
	vp.Excess -= amount
}

func (old *PushRelabel) New() (new *PushRelabel) {
	new = old // FIXME: this breaks the stability test. This is suppose to allocate a new struct on the heap, which unfortunately conflicts with how GoLogMsgCount works.

	new.SourceRawId = old.SourceRawId
	new.SinkRawId = old.SinkRawId
	new.HandleDeletes = old.HandleDeletes

	new.MsgCounter.Reset()
	new.VertexCount.Reset(1000)
	new.Gr.Reset()
	new.SourceId.Store(EmptyValue)
	new.SinkId.Store(EmptyValue)
	return new
}

func (VertexProp) New() (new VertexProp) {
	new.NbrMap = make(map[uint32]int32)
	return new
}

func (Mail) New() (new Mail) {
	return new
}

func Run(options graph.GraphOptions) (maxFlow int64, g *Graph) {
	AssertC(unsafe.Sizeof(graph.Notification[Note]{}) == 32)

	TimeSeriesReset()

	// Create Alg
	alg := new(PushRelabel)
	alg.SourceRawId = SourceRawId
	alg.SinkRawId = SinkRawId
	alg.HandleDeletes = options.InsertDeleteOnExpire > 0

	alg = alg.New()

	// Create Graph
	g = new(Graph)
	g.Options = options

	// Launch
	done := false
	if options.DebugLevel > 0 {
		alg.MsgCounter.GoLogMsgCount(alg, g, &done)
	}
	graph.Launch(alg, g)
	done = true

	return GetMaxFlowValue(g), g
}

func GetMaxFlowValue(g *Graph) int64 {
	sinkId, _ := g.NodeVertexFromRaw(SinkRawId)
	return g.NodeVertexProperty(sinkId).Excess
}

func (pr *PushRelabel) InitAllNote(_ *Vertex, _ *VertexProp, _ uint32, _ graph.RawType) (initialNote Note) {
	return Note{PosType: EmptyValue}
}

func (pr *PushRelabel) BaseVertexMailbox(v *Vertex, vp *VertexProp, internalId uint32, s *graph.VertexStructure) (m Mail) {
	if s.RawId == pr.SourceRawId {
		vp.Type = Source
		pr.SourceId.Store(internalId)
	} else if s.RawId == pr.SinkRawId {
		vp.Type = Sink
		pr.SinkId.Store(internalId)
	}
	vp.resetHeights(&pr.VertexCount)
	vp.UnknownPosCount = EmptyValue // Make as uninitialized
	return m
}

func (*PushRelabel) MailMerge(incoming Mail, sidx uint32, existing *Mail) (newInfo bool) {
	return true
}

func (*PushRelabel) MailRetrieve(existing *Mail, vertex *Vertex, vp *VertexProp) (outgoing Mail) {
	return outgoing
}

func (pr *PushRelabel) Init(g *Graph, v *Vertex, vp *VertexProp, myId uint32) (sent uint64) {
	// Iterate over existing edges
	sourceOutCap := 0
	for eidx := range v.OutEdges {
		e := &v.OutEdges[eidx]
		if e.Didx == myId || e.Property.Weight <= 0 || e.Didx == pr.SourceId.Load() || vp.Type == Sink {
			continue
		}
		AssertC(e.Property.Weight <= math.MaxInt64) // Cannot handle this weight

		if vp.Type == Source {
			sourceOutCap += int(e.Property.Weight)
		}

		if vp.Type == Source {
			vp.Excess += int64(e.Property.Weight)
			pr.SourceSupply += int64(e.Property.Weight)
		}

		pos, exist := vp.NbrMap[e.Didx]
		if !exist {
			pos = int32(len(vp.Nbrs))
			vp.Nbrs = append(vp.Nbrs, Neighbour{HeightPos: MaxHeight, HeightNeg: MaxHeight, Pos: -1, Didx: e.Didx})
			vp.NbrMap[e.Didx] = pos
			vp.UnknownPosCount++
		}

		var sendHeight bool
		pr.updateResCapOut(&vp.Nbrs[pos], int64(e.Property.Weight), &sendHeight)
	}
	if vp.Type == Source {
		log.Info().Msg("sourceOutCap=" + strconv.Itoa(sourceOutCap))
		log.Info().Msg("SourceSupply=" + strconv.Itoa(int(pr.SourceSupply)))
	}

	for i, nbr := range vp.Nbrs {
		mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
		sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{HeightPos: vp.HeightPos, HeightNeg: vp.HeightNeg, Flow: nbr.ResCapOut, SrcPos: int32(myId), PosType: NewNbr | (uint32(i) << 2)},
		}, mailbox, tidx))
	}

	sendNewMaxVertexCount := pr.VertexCount.NewVertexN()
	if sendNewMaxVertexCount {
		if source := pr.SourceId.Load(); source != EmptyValue {
			mailbox, tidx := g.NodeVertexMailbox(source)
			sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
				Target: source,
				Note:   Note{PosType: NewMaxVertexCount},
			}, mailbox, tidx))
		}
		if pr.HandleDeletes {
			if sink := pr.SinkId.Load(); sink != EmptyValue {
				mailbox, tidx := g.NodeVertexMailbox(sink)
				sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
					Target: sink,
					Note:   Note{PosType: NewMaxVertexCount},
				}, mailbox, tidx))
			}
		}
	}
	return
}

func (pr *PushRelabel) OnUpdateVertex(g *Graph, gt *GraphThread, v *Vertex, vp *VertexProp, n graph.Notification[Note], m Mail) (sent uint64) {
	if vp.UnknownPosCount == EmptyValue {
		vp.UnknownPosCount = 0
		sent += pr.Init(g, v, vp, n.Target)
	}

	sent += pr.processMessage(g, vp, n)

	if n.Activity == 0 && vp.UnknownPosCount == 0 { // Skip if there are more incoming messages
		sent += pr.finalizeVertexState(g, vp, n.Target)
	}
	return
}

func (pr *PushRelabel) processMessage(g *Graph, vp *VertexProp, n graph.Notification[Note]) (sent uint64) {
	if n.Note.PosType == EmptyValue {
		return
	}

	// tidx := n.Target >> graph.THREAD_SHIFT
	var nbr *Neighbour
	sendHeightPos, sendHeightNeg := false, false // Is their view of our height stale?

	if n.Note.PosType&TypePosMask != 0 {
		// Handle special messages
		// pr.MsgCounter.IncrementMsgCount(tidx, 0, true)
		switch n.Note.PosType & TypePosMask {

		case NewNbr:
			senderId, receiverPos, resCapOffset := uint32(n.Note.SrcPos), int32(n.Note.PosType>>2), n.Note.Flow
			senderPos, exist := vp.NbrMap[senderId] // Check if they are a new neighbour
			if !exist {
				senderPos = int32(len(vp.Nbrs))
				vp.Nbrs = append(vp.Nbrs, Neighbour{HeightPos: n.Note.HeightPos, HeightNeg: n.Note.HeightNeg, ResCapIn: resCapOffset, Pos: receiverPos, Didx: senderId})
				vp.NbrMap[senderId] = senderPos
				nbr = &vp.Nbrs[senderPos]

				mailbox, tidx := g.NodeVertexMailbox(senderId)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
					Target: senderId,
					Note:   Note{HeightPos: vp.HeightPos, HeightNeg: vp.HeightNeg, SrcPos: receiverPos, PosType: NbrPos | (uint32(senderPos) << 2)},
				}, mailbox, tidx))
			} else {
				nbr = &vp.Nbrs[senderPos]
				if nbr.Pos == -1 {
					nbr.Pos = receiverPos
					vp.UnknownPosCount--
				}
				pr.updateResCapIn(nbr, resCapOffset, &sendHeightPos)
			}

		case NbrPos:
			nbr = &vp.Nbrs[n.Note.SrcPos]
			if nbr.Pos == -1 {
				nbr.Pos = int32(n.Note.PosType >> 2)
				vp.UnknownPosCount--
			}

		case TypePosMask: // other special message type
			switch n.Note.PosType {

			case UpdateInCapPos:
				nbr = &vp.Nbrs[n.Note.SrcPos]
				pr.updateResCapIn(nbr, n.Note.Flow, &sendHeightPos)

			case UpdateInCapId:
				senderPos, exist := vp.NbrMap[uint32(n.Note.SrcPos)]
				AssertC(exist)
				nbr = &vp.Nbrs[senderPos]
				pr.updateResCapIn(nbr, n.Note.Flow, &sendHeightPos)

			case NewMaxVertexCount:
				if vp.Type == Source {
					vp.updateHeightPos(uint32(pr.VertexCount.GetMaxVertexCount()))
				} else if vp.Type == Sink {
					vp.updateHeightNeg(uint32(pr.VertexCount.GetMaxVertexCount()))
				} else {
					log.Panic().Msg("This normal vertex should not receive NewMaxVertexCount")
				}
				return

			default:
				panic(1) // Shouldn't reach here
			}
		default:
			panic(1) // Shouldn't reach here
		}

	} else {
		// Handle normal messages
		// pr.MsgCounter.IncrementMsgCount(tidx, n.Note.Flow, false)
		nbr = &vp.Nbrs[n.Note.SrcPos]

		// handle positive/negative flow
		if n.Note.Flow != 0 {
			AssertC(n.Note.Flow > 0 || pr.HandleDeletes)
			pr.updateFlowSH(vp, nbr, -n.Note.Flow, &sendHeightPos, &sendHeightNeg)
		}
	}

	nbr.HeightPos, nbr.HeightNeg = n.Note.HeightPos, n.Note.HeightNeg

	if nbr.ResCapIn < 0 {
		// The neighbour needs help with their c_f
		AssertC(pr.HandleDeletes)
		amount := -nbr.ResCapIn
		pr.updateFlowSH(vp, nbr, amount, &sendHeightPos, &sendHeightNeg)
		noti := graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{HeightPos: vp.HeightPos, HeightNeg: vp.HeightNeg, Flow: amount, SrcPos: nbr.Pos},
		}
		mailbox, tidx := g.NodeVertexMailbox(noti.Target)
		sent += g.EnsureSend(g.ActiveNotification(n.Target, noti, mailbox, tidx))
		sendHeightPos, sendHeightNeg = false, false // already told the neighbour our height
	}

	sent += pr.restoreHeightInvariantWithPush(g, vp, nbr, n.Target, &sendHeightPos, &sendHeightNeg)

	if (sendHeightPos && !vp.HeightPosChanged) || (sendHeightNeg && !vp.HeightNegChanged) { // Tell the neighbour our height
		mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
		sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{HeightPos: vp.HeightPos, HeightNeg: vp.HeightNeg, SrcPos: nbr.Pos},
		}, mailbox, tidx))
	}
	return
}

func (pr *PushRelabel) restoreHeightInvariantWithPush(g *Graph, vp *VertexProp, nbr *Neighbour, myId uint32, sendHeightPos, sendHeightNeg *bool) (sent uint64) {
	excess := vp.Excess
	if !pr.Gr.BlockPush.Load() && excess != 0 {
		amount := int64(0)
		if excess > 0 && nbr.ResCapOut > 0 && vp.HeightPos > nbr.HeightPos+1 {
			// Push positive flow
			amount = utils.Min(excess, nbr.ResCapOut)
			AssertC(amount > 0)
		} else if pr.HandleDeletes && excess < 0 && nbr.ResCapIn > 0 && vp.HeightNeg > nbr.HeightNeg+1 {
			// Push negative flow
			amount = -utils.Min(-excess, nbr.ResCapIn)
			AssertC(amount < 0)
		}
		if amount != 0 {
			pr.updateFlow(vp, nbr, amount)
			noti := graph.Notification[Note]{
				Target: nbr.Didx,
				Note:   Note{HeightPos: vp.HeightPos, HeightNeg: vp.HeightNeg, Flow: amount, SrcPos: nbr.Pos},
			}
			mailbox, tidx := g.NodeVertexMailbox(noti.Target)
			sent += g.EnsureSend(g.ActiveNotification(myId, noti, mailbox, tidx))
			*sendHeightPos, *sendHeightNeg = false, false
		}
	}
	pr.restoreHeightPosInvariant(g, vp, nbr, myId)
	pr.restoreHeightNegInvariant(g, vp, nbr, myId)
	return
}

func (pr *PushRelabel) restoreHeightPosInvariant(g *Graph, vp *VertexProp, nbr *Neighbour, myId uint32) {
	if nbr.ResCapOut > 0 && vp.HeightPos > nbr.HeightPos+1 {
		if vp.Type != Source { // Source has sufficient flow to saturate all outgoing edges (push might be disabled)
			AssertC(vp.Type != Sink)
			vp.updateHeightPos(nbr.HeightPos + 1)
		}
	}
}

func (pr *PushRelabel) restoreHeightNegInvariant(g *Graph, vp *VertexProp, nbr *Neighbour, myId uint32) {
	if pr.HandleDeletes && nbr.ResCapIn > 0 && vp.HeightNeg > nbr.HeightNeg+1 {
		if vp.Type == Sink {
			// Sink has sufficient flow to saturate all outgoing edges
			// AssertC(!canPush) // TODO: Maybe we should let sink generate negative excess at start
		} else {
			AssertC(vp.Type != Source)
			vp.updateHeightNeg(nbr.HeightNeg + 1)
		}
	}
}

func (pr *PushRelabel) discharge(g *Graph, vp *VertexProp, myId uint32) (sent uint64) {
	if vp.Excess == 0 || pr.Gr.BlockPush.Load() {
		return
	}

	positiveFlow := vp.Excess > 0

	if vp.Type != Normal && !(positiveFlow && vp.Type == Source) && !(!positiveFlow && vp.Type == Sink) {
		return
	}

	myHeight, updateHeight := &vp.HeightPos, vp.updateHeightPos
	negate := int64(1)
	restoreInvar := pr.restoreHeightNegInvariant
	if !positiveFlow {
		Assert(pr.HandleDeletes, "Excess shouldn't be <0 if there's no delete. Integer overflow?")
		myHeight, updateHeight = &vp.HeightNeg, vp.updateHeightNeg
		negate = -1
		restoreInvar = pr.restoreHeightPosInvariant
	}

	if *myHeight >= MaxHeight {
		return
	}

	lifted := false
	cannotLift := vp.Type != Normal || pr.Gr.BlockLift.Load()
	for vp.Excess != 0 {
		// push
		nextHeight := uint32(MaxHeight)
		var nextPushTarget *Neighbour
		for i := range vp.Nbrs {
			nbr := &vp.Nbrs[i]
			resCap, nbrHeight := nbr.ResCapOut, nbr.HeightPos
			if !positiveFlow {
				resCap, nbrHeight = nbr.ResCapIn, nbr.HeightNeg
			}
			if resCap > 0 {
				if !(*myHeight > nbrHeight) {
					if nbrHeight+1 < nextHeight {
						nextHeight, nextPushTarget = nbrHeight+1, nbr
					}
				} else {
					// push
					amount := utils.Min(negate*vp.Excess, resCap)
					AssertC(amount > 0)
					amount *= negate
					pr.updateFlow(vp, nbr, amount)
					restoreInvar(g, vp, nbr, myId)
					noti := graph.Notification[Note]{Target: nbr.Didx, Note: Note{HeightPos: vp.HeightPos, HeightNeg: vp.HeightNeg, Flow: amount, SrcPos: nbr.Pos}}
					mailbox, tidx := g.NodeVertexMailbox(noti.Target)
					sent += g.EnsureSend(g.ActiveNotification(myId, noti, mailbox, tidx))

					if vp.Excess == 0 {
						break
					}
				}
			}
		}

		if vp.Excess == 0 || cannotLift || nextHeight >= MaxHeight {
			break
		}

		// lift
		updateHeight(nextHeight)
		lifted = true

		nbr := nextPushTarget
		resCap := nbr.ResCapOut
		if !positiveFlow {
			resCap = nbr.ResCapIn
		}

		// push
		amount := utils.Min(negate*vp.Excess, resCap)
		AssertC(amount > 0)
		amount *= negate
		pr.updateFlow(vp, nbr, amount)
		restoreInvar(g, vp, nbr, myId)
		noti := graph.Notification[Note]{Target: nbr.Didx, Note: Note{HeightPos: vp.HeightPos, HeightNeg: vp.HeightNeg, Flow: amount, SrcPos: nbr.Pos}}
		mailbox, tidx := g.NodeVertexMailbox(noti.Target)
		sent += g.EnsureSend(g.ActiveNotification(myId, noti, mailbox, tidx))
	}

	if GlobalRelabelingEnabled && lifted {
		pr.Gr.OnLift(g, pr)
	}
	return
}

func (pr *PushRelabel) finalizeVertexState(g *Graph, vp *VertexProp, myId uint32) (sent uint64) {
	if vp.Excess < 0 && vp.Type == Normal {
		AssertC(pr.HandleDeletes)
		targetHeight := getNegativeExcessVertexHeight(&pr.VertexCount)
		if vp.HeightPos > targetHeight {
			vp.updateHeightPos(targetHeight)
		}
	}

	// discharge
	sent += pr.discharge(g, vp, myId)

	// broadcast heights if needed
	posChanged, negChanged := vp.HeightPosChanged, vp.HeightNegChanged
	if posChanged || negChanged {
		for _, nbr := range vp.Nbrs {
			if (posChanged && nbr.ResCapIn > 0) || (negChanged && nbr.ResCapOut > 0) {
				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{HeightPos: vp.HeightPos, HeightNeg: vp.HeightNeg, SrcPos: nbr.Pos},
				}, mailbox, tidx))
			}
		}
		vp.HeightPosChanged, vp.HeightNegChanged = false, false
	}
	return sent
}

func (pr *PushRelabel) OnSuperStepConverged(g *Graph) (sent uint64) {
	return pr.Gr.OnSuperStepConverged(g, pr)
}

func (pr *PushRelabel) OnEdgeAdd(g *Graph, gt *GraphThread, src *Vertex, srcProp *VertexProp, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	if srcProp.UnknownPosCount == EmptyValue {
		srcProp.UnknownPosCount = 0
		sent += pr.Init(g, src, srcProp, sidx)
	} else {
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			e := &src.OutEdges[eidx]
			if e.Didx == sidx || e.Property.Weight <= 0 || e.Didx == pr.SourceId.Load() || srcProp.Type == Sink {
				continue
			}
			AssertC(e.Property.Weight <= math.MaxInt64) // Cannot handle this weight

			if srcProp.Type == Source {
				srcProp.Excess += int64(e.Property.Weight)
				pr.SourceSupply += int64(e.Property.Weight)
			}

			pos, exist := srcProp.NbrMap[e.Didx]
			if exist {
				nbr := &srcProp.Nbrs[pos]
				var sendHeight bool
				pr.updateResCapOut(nbr, int64(e.Property.Weight), &sendHeight)

				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				notification := graph.Notification[Note]{Target: nbr.Didx, Note: Note{HeightPos: srcProp.HeightPos, HeightNeg: srcProp.HeightNeg, Flow: int64(e.Property.Weight)}}
				if nbr.Pos == -1 {
					notification.Note.SrcPos, notification.Note.PosType = int32(sidx), UpdateInCapId
				} else {
					notification.Note.SrcPos, notification.Note.PosType = nbr.Pos, UpdateInCapPos
				}
				sent += g.EnsureSend(g.ActiveNotification(sidx, notification, mailbox, tidx))

				// No need to restore height invariant here because
				//  (i) if old ResCapOut <= 0, dst will send its height to src;
				// (ii) if old ResCapOut >  0, the invariant is already maintained.
			} else {
				pos = int32(len(srcProp.Nbrs))
				srcProp.Nbrs = append(srcProp.Nbrs, Neighbour{HeightPos: MaxHeight, HeightNeg: MaxHeight, ResCapOut: int64(e.Property.Weight), Pos: -1, Didx: e.Didx})
				srcProp.NbrMap[e.Didx] = pos
				srcProp.UnknownPosCount++

				mailbox, tidx := g.NodeVertexMailbox(e.Didx)
				sent += g.EnsureSend(g.ActiveNotification(sidx, graph.Notification[Note]{
					Target: e.Didx,
					Note:   Note{HeightPos: srcProp.HeightPos, HeightNeg: srcProp.HeightNeg, Flow: int64(e.Property.Weight), SrcPos: int32(sidx), PosType: NewNbr | (uint32(pos) << 2)},
				}, mailbox, tidx))
			}
		}
	}

	mailbox, _ := g.NodeVertexMailbox(sidx)
	if srcProp.UnknownPosCount == 0 && atomic.LoadInt32(&mailbox.Activity) == 0 {
		sent += pr.finalizeVertexState(g, srcProp, sidx)
	}
	return
}

func (pr *PushRelabel) OnEdgeDel(g *Graph, gt *GraphThread, src *Vertex, srcProp *VertexProp, sidx uint32, deletedEdges []Edge, m Mail) (sent uint64) {
	AssertC(pr.HandleDeletes)
	if srcProp.UnknownPosCount == EmptyValue {
		srcProp.UnknownPosCount = 0
		sent += pr.Init(g, src, srcProp, sidx)
	} else {
		for _, e := range deletedEdges {
			if e.Didx == sidx || e.Property.Weight <= 0 || e.Didx == pr.SourceId.Load() || srcProp.Type == Sink {
				continue
			}

			if srcProp.Type == Source {
				srcProp.Excess -= int64(e.Property.Weight)
				pr.SourceSupply -= int64(e.Property.Weight)
			}

			pos, exist := srcProp.NbrMap[e.Didx]
			AssertC(exist && pos >= 0)
			nbr := &srcProp.Nbrs[pos]
			var sendHeight bool
			pr.updateResCapOut(nbr, -int64(e.Property.Weight), &sendHeight)

			noti := graph.Notification[Note]{
				Target: nbr.Didx,
				Note:   Note{HeightPos: srcProp.HeightPos, HeightNeg: srcProp.HeightNeg, Flow: -int64(e.Property.Weight)},
			}
			if nbr.Pos == -1 {
				noti.Note.SrcPos, noti.Note.PosType = int32(sidx), UpdateInCapId
			} else {
				noti.Note.SrcPos, noti.Note.PosType = nbr.Pos, UpdateInCapPos
			}
			mailbox, tidx := g.NodeVertexMailbox(noti.Target)
			sent += g.EnsureSend(g.ActiveNotification(sidx, noti, mailbox, tidx))
		}
	}

	mailbox, _ := g.NodeVertexMailbox(sidx)
	if srcProp.UnknownPosCount == 0 && atomic.LoadInt32(&mailbox.Activity) == 0 {
		sent += pr.finalizeVertexState(g, srcProp, sidx)
	}
	return
}

func (pr *PushRelabel) OnCheckCorrectness(g *Graph) {
	sourceId, sinkId := pr.SourceId.Load(), pr.SinkId.Load()
	if sourceId == EmptyValue || sinkId == EmptyValue {
		log.Warn().Msg("Skipping OnCheckCorrectness due to missing source or sink")
		return
	}
	AssertC(g.NodeVertexRawID(sourceId) == pr.SourceRawId)
	AssertC(g.NodeVertexRawID(sinkId) == pr.SinkRawId)
	sourceProp, sinkProp := g.NodeVertexProperty(sourceId), g.NodeVertexProperty(sinkId)

	log.Info().Msg("Sink excess is " + utils.V(sinkProp.Excess))

	log.Info().Msg("Ensuring the vertex type is correct")
	AssertC(sourceProp.Type == Source)
	AssertC(sinkProp.Type == Sink)
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		if vp.Type != Normal {
			AssertC(internalId == sourceId || internalId == sinkId)
		}
	})

	log.Info().Msg("Ensuring all messages are processed")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		AssertC(vp.UnknownPosCount == 0)
		AssertC(!vp.HeightPosChanged)
		AssertC(!vp.HeightNegChanged)
	})

	log.Info().Msg("Checking Pos are correct")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == sourceId || vp.Type == Sink {
				continue
			}
			pos, ok := vp.NbrMap[e.Didx]
			AssertC(ok)
			AssertC(vp.Nbrs[pos].Didx == e.Didx)
		}

		AssertC(len(vp.Nbrs) == len(vp.NbrMap))
		for _, nbr := range vp.Nbrs {
			AssertC(nbr.Pos >= 0)
		}
	})
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		for i, nbr := range vp.Nbrs {
			targetVertexProp := g.NodeVertexProperty(nbr.Didx)
			realPos, ok := targetVertexProp.NbrMap[internalId]
			AssertC(ok)
			AssertC(nbr.Pos == realPos)
			AssertC(targetVertexProp.Nbrs[realPos].Didx == internalId)
			AssertC(targetVertexProp.Nbrs[realPos].Pos == int32(i))
		}
	})

	log.Info().Msg("Checking ResCapOut and ResCapIn are correct")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		for _, nbr := range vp.Nbrs {
			targetVertexProp := g.NodeVertexProperty(nbr.Didx)
			realResCap := targetVertexProp.Nbrs[nbr.Pos].ResCapOut
			AssertC(nbr.ResCapIn == realResCap)
			AssertC(nbr.ResCapIn >= 0)
			AssertC(nbr.ResCapOut >= 0)
		}
	})

	log.Info().Msg("Checking the heights of the source and the sink")
	vertexCount := g.NodeVertexCount()
	AssertC(vertexCount < MaxHeight/4)
	Assert(sourceProp.HeightPos >= uint32(vertexCount),
		fmt.Sprintf("Source heightPos %v < # of vertices %d", utils.V(sourceProp.HeightPos), vertexCount))
	Assert(sinkProp.HeightPos == 0,
		fmt.Sprintf("Sink heightPos %v != 0", utils.V(sinkProp.HeightPos)))
	if pr.HandleDeletes {
		Assert(sourceProp.HeightNeg == 0,
			fmt.Sprintf("Source heightNeg %v != 0", utils.V(sourceProp.HeightNeg)))
		Assert(sinkProp.HeightNeg >= uint32(vertexCount),
			fmt.Sprintf("Sink heightNeg %v < # of vertices %d", utils.V(sinkProp.HeightNeg), vertexCount))
	}

	// Check Excess & residual capacity
	log.Info().Msg("Checking excess & residual capacity")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		if vp.Type == Normal {
			AssertC(vp.Excess == 0)
		}
		for _, nbr := range vp.Nbrs {
			AssertC(nbr.ResCapOut >= 0)
		}
	})

	log.Info().Msg("Checking sum of edge capacities in the original graph == in the residual graph")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		capacityOriginal := int64(0)
		capacityResidual := int64(0)
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == sourceId || vp.Type == Sink {
				continue
			}
			capacityOriginal += int64(e.Property.Weight)
		}

		for _, nbr := range vp.Nbrs {
			capacityResidual += int64(nbr.ResCapOut)
		}

		if vp.Type == Source {
			AssertC(int64(vp.Excess) == capacityResidual)
		} else if vp.Type == Sink {
			AssertC(capacityOriginal+int64(vp.Excess) == capacityResidual)
		} else {
			AssertC(capacityOriginal == capacityResidual)
		}
	})

	log.Info().Msg("Checking sourceOut and sinkIn")
	sourceOut := int64(0)
	for _, e := range g.NodeVertex(sourceId).OutEdges {
		if e.Didx == sourceId || e.Property.Weight <= 0 {
			continue
		}
		sourceOut += int64(e.Property.Weight)
	}
	sourceOut -= int64(sourceProp.Excess)
	sinkIn := int64(sinkProp.Excess)
	AssertC(sourceOut == sinkIn)
	log.Info().Msg(fmt.Sprintf("Maximum flow from %d to %d is %d", SourceRawId, SinkRawId, sourceOut))

	log.Info().Msg("Ensuring NbrHeight is accurate")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		for _, nbr := range vp.Nbrs {
			wp := g.NodeVertexProperty(nbr.Didx)
			if nbr.ResCapOut > 0 {
				AssertC(nbr.HeightPos == wp.HeightPos)
			}
			if pr.HandleDeletes && nbr.ResCapIn > 0 {
				AssertC(nbr.HeightNeg == wp.HeightNeg)
			}
		}
	})

	log.Info().Msg("Checking height invariants")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex, vp *VertexProp) {
		hp, hn := vp.HeightPos, vp.HeightNeg
		for _, nbr := range vp.Nbrs {
			if nbr.ResCapOut > 0 {
				AssertC(hp <= nbr.HeightPos+1)
			}
			if pr.HandleDeletes && vp.Type != Sink && nbr.ResCapIn > 0 {
				AssertC(hn <= nbr.HeightNeg+1)
			}
		}
	})
}
