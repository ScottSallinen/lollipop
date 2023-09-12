package o

import (
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"unsafe"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
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
type Vertex = graph.Vertex[VertexProp, EdgeProp]
type Edge = graph.Edge[EdgeProp]

const (
	Name = "PushRelabel (O)"

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
	return uint32(2 * vc.GetMaxVertexCount())
}

func (v *VertexProp) resetHeights(vc *VertexCount) (HeightPosChanged, HeightNegChanged bool) {
	v.HeightPos, v.HeightNeg = 0, MaxHeight
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

func (pr *PushRelabel) updateResCapOut(v *Vertex, nbr *Neighbour, delta int64, sendHeightNeg *bool) {
	oldResCapOut := nbr.ResCapOut
	nbr.ResCapOut += delta
	if pr.HandleDeletes && oldResCapOut <= 0 && nbr.ResCapOut > 0 {
		*sendHeightNeg = true
	}
}

func (pr *PushRelabel) updateResCapIn(v *Vertex, nbr *Neighbour, delta int64, sendHeightPos *bool) {
	oldResCapIn := nbr.ResCapIn
	nbr.ResCapIn += delta
	if oldResCapIn <= 0 && nbr.ResCapIn > 0 {
		*sendHeightPos = true
	}
}

func (pr *PushRelabel) updateFlowSH(v *Vertex, nbr *Neighbour, amount int64, sendHeightPos *bool, sendHeightNeg *bool) {
	pr.updateResCapOut(v, nbr, -amount, sendHeightNeg)
	pr.updateResCapIn(v, nbr, amount, sendHeightPos)
	v.Property.Excess -= amount
}

func (pr *PushRelabel) updateFlow(v *Vertex, nbr *Neighbour, amount int64) {
	nbr.ResCapOut -= amount
	nbr.ResCapIn += amount
	v.Property.Excess -= amount
}

func (old *PushRelabel) New() (new *PushRelabel) {
	new = old

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
	alg.HandleDeletes = true

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
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	return sink.Property.Excess
}

func (pr *PushRelabel) InitAllNote(_ *Vertex, _ uint32, _ graph.RawType) (initialNote Note) {
	return Note{PosType: EmptyValue}
}

func (pr *PushRelabel) BaseVertexMailbox(v *Vertex, internalId uint32, s *graph.VertexStructure) (m Mail) {
	if s.RawId == pr.SourceRawId {
		v.Property.Type = Source
		pr.SourceId.Store(internalId)
	} else if s.RawId == pr.SinkRawId {
		v.Property.Type = Sink
		pr.SinkId.Store(internalId)
	}
	v.Property.resetHeights(&pr.VertexCount)
	v.Property.UnknownPosCount = EmptyValue // Make as uninitialized
	return m
}

func (*PushRelabel) MailMerge(incoming Mail, sidx uint32, existing *Mail) (newInfo bool) {
	return true
}

func (*PushRelabel) MailRetrieve(existing *Mail, vertex *Vertex) (outgoing Mail) {
	return outgoing
}

func (pr *PushRelabel) Init(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	// Iterate over existing edges
	sourceOutCap := 0
	for eidx := range v.OutEdges {
		e := &v.OutEdges[eidx]
		if e.Didx == myId || e.Property.Weight <= 0 || e.Didx == pr.SourceId.Load() || v.Property.Type == Sink {
			continue
		}
		AssertC(e.Property.Weight <= math.MaxInt64) // Cannot handle this weight

		if v.Property.Type == Source {
			sourceOutCap += int(e.Property.Weight)
		}

		if v.Property.Type == Source {
			v.Property.Excess += int64(e.Property.Weight)
			pr.SourceSupply += int64(e.Property.Weight)
		}

		pos, exist := v.Property.NbrMap[e.Didx]
		if !exist {
			pos = int32(len(v.Property.Nbrs))
			v.Property.Nbrs = append(v.Property.Nbrs, Neighbour{HeightPos: MaxHeight, HeightNeg: MaxHeight, Pos: -1, Didx: e.Didx})
			v.Property.NbrMap[e.Didx] = pos
			v.Property.UnknownPosCount++
		}

		var sendHeight bool
		pr.updateResCapOut(v, &v.Property.Nbrs[pos], int64(e.Property.Weight), &sendHeight)
	}
	if v.Property.Type == Source {
		log.Info().Msg("sourceOutCap=" + strconv.Itoa(sourceOutCap))
		log.Info().Msg("SourceSupply=" + strconv.Itoa(int(pr.SourceSupply)))
	}

	for i, nbr := range v.Property.Nbrs {
		mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
		sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, Flow: nbr.ResCapOut, SrcPos: int32(myId), PosType: NewNbr | (uint32(i) << 2)},
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

func (pr *PushRelabel) OnUpdateVertex(g *Graph, v *Vertex, n graph.Notification[Note], m Mail) (sent uint64) {
	if v.Property.UnknownPosCount == EmptyValue {
		v.Property.UnknownPosCount = 0
		sent += pr.Init(g, v, n.Target)
	}

	sent += pr.processMessage(g, v, n)

	if n.Activity == 0 && v.Property.UnknownPosCount == 0 { // Skip if there are more incoming messages
		sent += pr.finalizeVertexState(g, v, n.Target)
	}
	return
}

func (pr *PushRelabel) processMessage(g *Graph, v *Vertex, n graph.Notification[Note]) (sent uint64) {
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
			senderPos, exist := v.Property.NbrMap[senderId] // Check if they are a new neighbour
			if !exist {
				senderPos = int32(len(v.Property.Nbrs))
				v.Property.Nbrs = append(v.Property.Nbrs, Neighbour{HeightPos: n.Note.HeightPos, HeightNeg: n.Note.HeightNeg, ResCapIn: resCapOffset, Pos: receiverPos, Didx: senderId})
				v.Property.NbrMap[senderId] = senderPos
				nbr = &v.Property.Nbrs[senderPos]

				mailbox, tidx := g.NodeVertexMailbox(senderId)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
					Target: senderId,
					Note:   Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, SrcPos: receiverPos, PosType: NbrPos | (uint32(senderPos) << 2)},
				}, mailbox, tidx))
			} else {
				nbr = &v.Property.Nbrs[senderPos]
				if nbr.Pos == -1 {
					nbr.Pos = receiverPos
					v.Property.UnknownPosCount--
				}
				pr.updateResCapIn(v, nbr, resCapOffset, &sendHeightPos)
			}

		case NbrPos:
			nbr = &v.Property.Nbrs[n.Note.SrcPos]
			if nbr.Pos == -1 {
				nbr.Pos = int32(n.Note.PosType >> 2)
				v.Property.UnknownPosCount--
			}

		case TypePosMask: // other special message type
			switch n.Note.PosType {

			case UpdateInCapPos:
				nbr = &v.Property.Nbrs[n.Note.SrcPos]
				pr.updateResCapIn(v, nbr, n.Note.Flow, &sendHeightPos)

			case UpdateInCapId:
				senderPos, exist := v.Property.NbrMap[uint32(n.Note.SrcPos)]
				AssertC(exist)
				nbr = &v.Property.Nbrs[senderPos]
				pr.updateResCapIn(v, nbr, n.Note.Flow, &sendHeightPos)

			case NewMaxVertexCount:
				if v.Property.Type == Source {
					v.Property.updateHeightPos(uint32(pr.VertexCount.GetMaxVertexCount()))
				} else if v.Property.Type == Sink {
					v.Property.updateHeightNeg(uint32(pr.VertexCount.GetMaxVertexCount()))
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
		nbr = &v.Property.Nbrs[n.Note.SrcPos]

		// handle positive/negative flow
		if n.Note.Flow != 0 {
			AssertC(n.Note.Flow > 0 || pr.HandleDeletes)
			pr.updateFlowSH(v, nbr, -n.Note.Flow, &sendHeightPos, &sendHeightNeg)
		}
	}

	nbr.HeightPos, nbr.HeightNeg = n.Note.HeightPos, n.Note.HeightNeg

	if nbr.ResCapIn < 0 {
		// The neighbour needs help with their c_f
		AssertC(pr.HandleDeletes)
		amount := -nbr.ResCapIn
		pr.updateFlowSH(v, nbr, amount, &sendHeightPos, &sendHeightNeg)
		noti := graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, Flow: amount, SrcPos: nbr.Pos},
		}
		mailbox, tidx := g.NodeVertexMailbox(noti.Target)
		sent += g.EnsureSend(g.ActiveNotification(n.Target, noti, mailbox, tidx))
		sendHeightPos, sendHeightNeg = false, false // already told the neighbour our height
	}

	sent += pr.restoreHeightInvariantWithPush(g, v, nbr, n.Target, &sendHeightPos, &sendHeightNeg)

	if (sendHeightPos && !v.Property.HeightPosChanged) || (sendHeightNeg && !v.Property.HeightNegChanged) { // Tell the neighbour our height
		mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
		sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, SrcPos: nbr.Pos},
		}, mailbox, tidx))
	}
	return
}

func (pr *PushRelabel) restoreHeightInvariantWithPush(g *Graph, v *Vertex, nbr *Neighbour, myId uint32, sendHeightPos, sendHeightNeg *bool) (sent uint64) {
	excess := v.Property.Excess
	if !pr.Gr.BlockPush.Load() && excess != 0 {
		amount := int64(0)
		if excess > 0 && nbr.ResCapOut > 0 && v.Property.HeightPos > nbr.HeightPos+1 {
			// Push positive flow
			amount = utils.Min(excess, nbr.ResCapOut)
			AssertC(amount > 0)
		} else if pr.HandleDeletes && excess < 0 && nbr.ResCapIn > 0 && v.Property.HeightNeg > nbr.HeightNeg+1 {
			// Push negative flow
			amount = -utils.Min(-excess, nbr.ResCapIn)
			AssertC(amount < 0)
		}
		if amount != 0 {
			pr.updateFlow(v, nbr, amount)
			noti := graph.Notification[Note]{
				Target: nbr.Didx,
				Note:   Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, Flow: amount, SrcPos: nbr.Pos},
			}
			mailbox, tidx := g.NodeVertexMailbox(noti.Target)
			sent += g.EnsureSend(g.ActiveNotification(myId, noti, mailbox, tidx))
			*sendHeightPos, *sendHeightNeg = false, false
		}
	}
	sent += pr.restoreHeightPosInvariant(g, v, nbr, myId)
	sent += pr.restoreHeightNegInvariant(g, v, nbr, myId)
	return
}

func (pr *PushRelabel) restoreHeightPosInvariant(g *Graph, v *Vertex, nbr *Neighbour, myId uint32) (sent uint64) {
	if nbr.ResCapOut > 0 && v.Property.HeightPos > nbr.HeightPos+1 {
		if v.Property.Type != Source { // Source has sufficient flow to saturate all outgoing edges (push might be disabled)
			AssertC(v.Property.Type != Sink)
			if pr.Gr.BlockPush.Load() {
				return
			}
			// v.Property.updateHeightPos(nbr.HeightPos + 1)
			amount := nbr.ResCapOut
			pr.updateFlow(v, nbr, amount)
			noti := graph.Notification[Note]{
				Target: nbr.Didx,
				Note:   Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, Flow: amount, SrcPos: nbr.Pos},
			}
			mailbox, tidx := g.NodeVertexMailbox(noti.Target)
			sent += g.EnsureSend(g.ActiveNotification(myId, noti, mailbox, tidx))
		}
	}
	return
}

func (pr *PushRelabel) restoreHeightNegInvariant(g *Graph, v *Vertex, nbr *Neighbour, myId uint32) (sent uint64) {
	if pr.HandleDeletes && nbr.ResCapIn > 0 && v.Property.HeightNeg > nbr.HeightNeg+1 {
		if v.Property.Type == Sink {
			// Sink has sufficient flow to saturate all outgoing edges
			// AssertC(!canPush) // TODO: Maybe we should let sink generate negative excess at start
		} else {
			AssertC(v.Property.Type != Source)
			v.Property.updateHeightNeg(nbr.HeightNeg + 1)
		}
	}
	return
}

func (pr *PushRelabel) discharge(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	if v.Property.Excess == 0 || pr.Gr.BlockPush.Load() {
		return
	}

	positiveFlow := v.Property.Excess > 0

	if v.Property.Type != Normal && !(positiveFlow && v.Property.Type == Source) && !(!positiveFlow && v.Property.Type == Sink) {
		return
	}

	myHeight, updateHeight := &v.Property.HeightPos, v.Property.updateHeightPos
	negate := int64(1)
	restoreInvar := pr.restoreHeightNegInvariant
	if !positiveFlow {
		Assert(pr.HandleDeletes, "Excess shouldn't be <0 if there's no delete. Integer overflow?")
		myHeight, updateHeight = &v.Property.HeightNeg, v.Property.updateHeightNeg
		negate = -1
		restoreInvar = pr.restoreHeightPosInvariant
	}

	if *myHeight >= MaxHeight {
		return
	}

	lifted := false
	cannotLift := v.Property.Type != Normal || pr.Gr.BlockLift.Load()
	for v.Property.Excess != 0 {
		// push
		nextHeight := uint32(MaxHeight)
		var nextPushTarget *Neighbour
		for i := range v.Property.Nbrs {
			nbr := &v.Property.Nbrs[i]
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
					amount := utils.Min(negate*v.Property.Excess, resCap)
					AssertC(amount > 0)
					amount *= negate
					pr.updateFlow(v, nbr, amount)
					sent += restoreInvar(g, v, nbr, myId)
					noti := graph.Notification[Note]{Target: nbr.Didx, Note: Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, Flow: amount, SrcPos: nbr.Pos}}
					mailbox, tidx := g.NodeVertexMailbox(noti.Target)
					sent += g.EnsureSend(g.ActiveNotification(myId, noti, mailbox, tidx))

					if v.Property.Excess == 0 {
						break
					}
				}
			}
		}

		if v.Property.Excess == 0 || cannotLift || nextHeight >= MaxHeight {
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
		amount := utils.Min(negate*v.Property.Excess, resCap)
		AssertC(amount > 0)
		amount *= negate
		pr.updateFlow(v, nbr, amount)
		restoreInvar(g, v, nbr, myId)
		noti := graph.Notification[Note]{Target: nbr.Didx, Note: Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, Flow: amount, SrcPos: nbr.Pos}}
		mailbox, tidx := g.NodeVertexMailbox(noti.Target)
		sent += g.EnsureSend(g.ActiveNotification(myId, noti, mailbox, tidx))
	}

	if GlobalRelabelingEnabled && lifted {
		pr.Gr.OnLift(g, pr)
	}
	return
}

func (pr *PushRelabel) finalizeVertexState(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	if v.Property.Excess < 0 && v.Property.Type == Normal {
		AssertC(pr.HandleDeletes)
		targetHeight := getNegativeExcessVertexHeight(&pr.VertexCount)
		if v.Property.HeightPos > targetHeight {
			v.Property.updateHeightPos(targetHeight)
		}
	}

	// discharge
	sent += pr.discharge(g, v, myId)

	// broadcast heights if needed
	posChanged, negChanged := v.Property.HeightPosChanged, v.Property.HeightNegChanged
	if posChanged || negChanged {
		for _, nbr := range v.Property.Nbrs {
			if (posChanged && nbr.ResCapIn > 0) || (negChanged && nbr.ResCapOut > 0) {
				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{HeightPos: v.Property.HeightPos, HeightNeg: v.Property.HeightNeg, SrcPos: nbr.Pos},
				}, mailbox, tidx))
			}
		}
		v.Property.HeightPosChanged, v.Property.HeightNegChanged = false, false
	}
	return sent
}

func (pr *PushRelabel) OnSuperStepConverged(g *Graph) (sent uint64) {
	return pr.Gr.OnSuperStepConverged(g, pr)
}

func (pr *PushRelabel) OnEdgeAdd(g *Graph, src *Vertex, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	if src.Property.UnknownPosCount == EmptyValue {
		src.Property.UnknownPosCount = 0
		sent += pr.Init(g, src, sidx)
	} else {
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			e := &src.OutEdges[eidx]
			if e.Didx == sidx || e.Property.Weight <= 0 || e.Didx == pr.SourceId.Load() || src.Property.Type == Sink {
				continue
			}
			AssertC(e.Property.Weight <= math.MaxInt64) // Cannot handle this weight

			if src.Property.Type == Source {
				src.Property.Excess += int64(e.Property.Weight)
				pr.SourceSupply += int64(e.Property.Weight)
			}

			pos, exist := src.Property.NbrMap[e.Didx]
			if exist {
				nbr := &src.Property.Nbrs[pos]
				var sendHeight bool
				pr.updateResCapOut(src, nbr, int64(e.Property.Weight), &sendHeight)

				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				notification := graph.Notification[Note]{Target: nbr.Didx, Note: Note{HeightPos: src.Property.HeightPos, HeightNeg: src.Property.HeightNeg, Flow: int64(e.Property.Weight)}}
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
				pos = int32(len(src.Property.Nbrs))
				src.Property.Nbrs = append(src.Property.Nbrs, Neighbour{HeightPos: MaxHeight, HeightNeg: MaxHeight, ResCapOut: int64(e.Property.Weight), Pos: -1, Didx: e.Didx})
				src.Property.NbrMap[e.Didx] = pos
				src.Property.UnknownPosCount++

				mailbox, tidx := g.NodeVertexMailbox(e.Didx)
				sent += g.EnsureSend(g.ActiveNotification(sidx, graph.Notification[Note]{
					Target: e.Didx,
					Note:   Note{HeightPos: src.Property.HeightPos, HeightNeg: src.Property.HeightNeg, Flow: int64(e.Property.Weight), SrcPos: int32(sidx), PosType: NewNbr | (uint32(pos) << 2)},
				}, mailbox, tidx))
			}
		}
	}

	mailbox, _ := g.NodeVertexMailbox(sidx)
	if src.Property.UnknownPosCount == 0 && atomic.LoadInt32(&mailbox.Activity) == 0 {
		sent += pr.finalizeVertexState(g, src, sidx)
	}
	return
}

func (pr *PushRelabel) OnEdgeDel(g *Graph, src *Vertex, sidx uint32, deletedEdges []Edge, m Mail) (sent uint64) {
	AssertC(pr.HandleDeletes)
	if src.Property.UnknownPosCount == EmptyValue {
		src.Property.UnknownPosCount = 0
		sent += pr.Init(g, src, sidx)
	} else {
		for _, e := range deletedEdges {
			if e.Didx == sidx || e.Property.Weight <= 0 || e.Didx == pr.SourceId.Load() || src.Property.Type == Sink {
				continue
			}

			if src.Property.Type == Source {
				src.Property.Excess -= int64(e.Property.Weight)
				pr.SourceSupply -= int64(e.Property.Weight)
			}

			pos, exist := src.Property.NbrMap[e.Didx]
			AssertC(exist && pos >= 0)
			nbr := &src.Property.Nbrs[pos]
			var sendHeight bool
			pr.updateResCapOut(src, nbr, -int64(e.Property.Weight), &sendHeight)

			noti := graph.Notification[Note]{
				Target: nbr.Didx,
				Note:   Note{HeightPos: src.Property.HeightPos, HeightNeg: src.Property.HeightNeg, Flow: -int64(e.Property.Weight)},
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
	if src.Property.UnknownPosCount == 0 && atomic.LoadInt32(&mailbox.Activity) == 0 {
		sent += pr.finalizeVertexState(g, src, sidx)
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
	source, sink := g.NodeVertex(sourceId), g.NodeVertex(sinkId)

	log.Info().Msg("Sink excess is " + utils.V(sink.Property.Excess))

	log.Info().Msg("Ensuring the vertex type is correct")
	AssertC(source.Property.Type == Source)
	AssertC(sink.Property.Type == Sink)
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		if v.Property.Type != Normal {
			AssertC(internalId == sourceId || internalId == sinkId)
		}
	})

	log.Info().Msg("Ensuring all messages are processed")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		AssertC(v.Property.UnknownPosCount == 0)
		AssertC(!v.Property.HeightPosChanged)
		AssertC(!v.Property.HeightNegChanged)
	})

	log.Info().Msg("Checking Pos are correct")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == sourceId || v.Property.Type == Sink {
				continue
			}
			pos, ok := v.Property.NbrMap[e.Didx]
			AssertC(ok)
			AssertC(v.Property.Nbrs[pos].Didx == e.Didx)
		}

		AssertC(len(v.Property.Nbrs) == len(v.Property.NbrMap))
		for _, nbr := range v.Property.Nbrs {
			AssertC(nbr.Pos >= 0)
		}
	})
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		for i, nbr := range v.Property.Nbrs {
			targetVertex := g.NodeVertex(nbr.Didx)
			realPos, ok := targetVertex.Property.NbrMap[internalId]
			AssertC(ok)
			AssertC(nbr.Pos == realPos)
			AssertC(targetVertex.Property.Nbrs[realPos].Didx == internalId)
			AssertC(targetVertex.Property.Nbrs[realPos].Pos == int32(i))
		}
	})

	log.Info().Msg("Checking ResCapOut and ResCapIn are correct")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		for _, nbr := range v.Property.Nbrs {
			targetVertex := g.NodeVertex(nbr.Didx)
			realResCap := targetVertex.Property.Nbrs[nbr.Pos].ResCapOut
			AssertC(nbr.ResCapIn == realResCap)
			AssertC(nbr.ResCapIn >= 0)
			AssertC(nbr.ResCapOut >= 0)
		}
	})

	log.Info().Msg("Checking the heights of the source and the sink")
	vertexCount := g.NodeVertexCount()
	AssertC(vertexCount < MaxHeight/4)
	Assert(source.Property.HeightPos >= uint32(vertexCount),
		fmt.Sprintf("Source heightPos %v < # of vertices %d", utils.V(source.Property.HeightPos), vertexCount))
	Assert(sink.Property.HeightPos == 0,
		fmt.Sprintf("Sink heightPos %v != 0", utils.V(sink.Property.HeightPos)))
	if pr.HandleDeletes {
		Assert(source.Property.HeightNeg == 0,
			fmt.Sprintf("Source heightNeg %v != 0", utils.V(source.Property.HeightNeg)))
		Assert(sink.Property.HeightNeg >= uint32(vertexCount),
			fmt.Sprintf("Sink heightNeg %v < # of vertices %d", utils.V(sink.Property.HeightNeg), vertexCount))
	}

	// Check Excess & residual capacity
	log.Info().Msg("Checking excess & residual capacity")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		if v.Property.Type == Normal {
			AssertC(v.Property.Excess == 0)
		}
		for _, nbr := range v.Property.Nbrs {
			AssertC(nbr.ResCapOut >= 0)
		}
	})

	log.Info().Msg("Checking sum of edge capacities in the original graph == in the residual graph")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		capacityOriginal := int64(0)
		capacityResidual := int64(0)
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == sourceId || v.Property.Type == Sink {
				continue
			}
			capacityOriginal += int64(e.Property.Weight)
		}

		for _, nbr := range v.Property.Nbrs {
			capacityResidual += int64(nbr.ResCapOut)
		}

		if v.Property.Type == Source {
			AssertC(int64(v.Property.Excess) == capacityResidual)
		} else if v.Property.Type == Sink {
			AssertC(capacityOriginal+int64(v.Property.Excess) == capacityResidual)
		} else {
			AssertC(capacityOriginal == capacityResidual)
		}
	})

	log.Info().Msg("Checking sourceOut and sinkIn")
	sourceOut := int64(0)
	for _, e := range source.OutEdges {
		if e.Didx == sourceId || e.Property.Weight <= 0 {
			continue
		}
		sourceOut += int64(e.Property.Weight)
	}
	sourceOut -= int64(source.Property.Excess)
	sinkIn := int64(sink.Property.Excess)
	AssertC(sourceOut == sinkIn)
	log.Info().Msg(fmt.Sprintf("Maximum flow from %d to %d is %d", SourceRawId, SinkRawId, sourceOut))

	log.Info().Msg("Ensuring NbrHeight is accurate")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		for _, nbr := range v.Property.Nbrs {
			w := g.NodeVertex(nbr.Didx)
			if nbr.ResCapOut > 0 {
				AssertC(nbr.HeightPos == w.Property.HeightPos)
			}
			if pr.HandleDeletes && nbr.ResCapIn > 0 {
				AssertC(nbr.HeightNeg == w.Property.HeightNeg)
			}
		}
	})

	log.Info().Msg("Checking height invariants")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		hp, hn := v.Property.HeightPos, v.Property.HeightNeg
		for _, nbr := range v.Property.Nbrs {
			if nbr.ResCapOut > 0 {
				AssertC(hp <= nbr.HeightPos+1)
			}
			if pr.HandleDeletes && v.Property.Type != Sink && nbr.ResCapIn > 0 {
				AssertC(hn <= nbr.HeightNeg+1)
			}
		}
	})
}
