package graph

import (
	"bytes"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

type EventType uint32

const (
	ADD EventType = iota // Implicit, 0, means add.
	DEL
	// ADD_VERTEX
	// UPDATE // not used yet. Update *which* edge? To revisit with more consideration with multi-graphs, which is the assumed mode right now
)

const EVENT_TYPE_BITS = 2
const EVENT_TYPE_MASK = (1 << EVENT_TYPE_BITS) - 1
const REMITTER_MIN_SLEEP_TIME_NANO = 10_000_000 // 10ms

// TypeAndEventIdx & EVENT_TYPE_MASK
func (t TopologyEvent[E]) EventType() EventType {
	return EventType(t.TypeAndEventIdx & EVENT_TYPE_MASK)
}

// TypeAndEventIdx >> EVENT_TYPE_BITS
func (t TopologyEvent[E]) EventIdx() uint64 {
	return uint64(t.TypeAndEventIdx >> EVENT_TYPE_BITS)
}

// TypeAndEventIdx & EVENT_TYPE_MASK
func (e RawEdgeEvent[E]) EventType() EventType {
	return EventType(e.TypeAndEventIdx & EVENT_TYPE_MASK)
}

// TypeAndEventIdx >> EVENT_TYPE_BITS
func (e RawEdgeEvent[E]) EventIdx() uint64 {
	return uint64(e.TypeAndEventIdx >> EVENT_TYPE_BITS)
}

// TypeAndEventIdx & EVENT_TYPE_MASK
func (e InternalTopologyEvent[E]) EventType() EventType {
	return EventType(e.TypeAndEventIdx & EVENT_TYPE_MASK)
}

// TypeAndEventIdx >> EVENT_TYPE_BITS
func (e InternalTopologyEvent[E]) EventIdx() uint64 {
	return uint64(e.TypeAndEventIdx >> EVENT_TYPE_BITS)
}

func (t EventType) String() string {
	switch t {
	case ADD:
		return "ADD"
	case DEL:
		return "DEL"
	// case ADD_VERTEX:
	// 	return "ADD_VERTEX"
	//case UPDATE:
	//	return "UPDATE"
	default:
		return "UNKNOWN"
	}
}

// A basic topology event that refers to the external, raw identifiers.
type TopologyEvent[E EPI[E]] struct {
	TypeAndEventIdx uint64
	SrcRaw          RawType
	DstRaw          RawType
	EdgeProperty    E
}

// A topology event that has had the edge remapped to the internal index.
type RawEdgeEvent[E EPI[E]] struct {
	TypeAndEventIdx uint64
	SrcRaw          RawType
	DstRaw          RawType // Unused at the moment (Didx is known in the Edge). Could be changed out for something else.
	Edge            Edge[E] // Didx, Pos, Property
}

type InternalTopologyEvent[E EPI[E]] struct {
	TypeAndEventIdx uint64
	SrcRaw          RawType
	DstRaw          RawType
	SrcIdx          uint32  // For an edge event, this is the internal ID of the source vertex.
	Edge            Edge[E] // Didx, Pos, Property
}

func (t TopologyEvent[E]) String() string {
	return "{Type: " + utils.V(t.EventType()) + ", EventIdx: " + utils.V(t.EventIdx()) + ", SrcRaw: " + utils.V(t.SrcRaw) + ", DstRaw: " + utils.V(t.DstRaw) + ", EdgeProperty: " + utils.V(t.EdgeProperty) + "}"
}

func (e RawEdgeEvent[E]) String() string {
	return "{Type: " + utils.V(e.EventType()) + ", EventIdx: " + utils.V(e.EventIdx()) + ", SrcRaw: " + utils.V(e.SrcRaw) + ", Edge: " + e.Edge.String() + "}"
}

func (e InternalTopologyEvent[E]) String() string {
	return "{Type: " + utils.V(e.EventType()) + ", EventIdx: " + utils.V(e.EventIdx()) + ", SrcRaw: " + utils.V(e.SrcRaw) + ", DstRaw: " + utils.V(e.DstRaw) + ", Edge: " + e.Edge.String() + "}"
}

// Retrieves order from the emitter, then looks for the remitted events; then puts to the topology event queues.
// Has some added logic to check if we should interrupt to ask a query.
func (g *Graph[V, E, M, N]) Remitter(order *utils.GrowableRingBuff[uint32]) (remitted uint64) {
	runtime.LockOSThread()
	nextTarget := g.Options.TimeSeriesInterval
	targetRate := g.Options.TargetRate // events per second
	queryByEventCount := g.Options.LogTimeseries && g.Options.TimeseriesEdgeCount
	queryByTimeStamp := g.Options.LogTimeseries && !g.Options.TimeseriesEdgeCount
	retried := 0
	totalRetriedOrder := 0
	totalRetriedThreads := 0
	totalPutFails := 0
	targetEventCount := uint64(0)
	var event InternalTopologyEvent[E]
	var target uint32
	var ok bool
	var closed bool
	var pos uint64

	for {
		if target, ok, pos = order.GetFast(); !ok {
			if target, closed, retried = order.GetSlow(pos); closed {
				break
			}
			totalRetriedOrder += retried
		}
		if event, ok, pos = g.GraphThreads[target].ToRemitQueue.GetFast(); !ok {
			event, _, retried = g.GraphThreads[target].ToRemitQueue.GetSlow(pos)
			totalRetriedThreads += retried
		}
		sTidx := IdxToTidx(event.SrcIdx)
		if pos, ok = g.GraphThreads[sTidx].TopologyQueue.PutFast(event); !ok {
			totalPutFails += g.GraphThreads[sTidx].TopologyQueue.PutSlow(event, pos)
		}

		//log.Debug().Msg("Remitter " + utils.V(event))

		remitted++

		// Check to interrupt to ask a query if we've reached the next target
		if queryByEventCount && remitted >= nextTarget {
			nextTarget += g.Options.TimeSeriesInterval
			g.ExecuteQuery(remitted)
		} else if queryByTimeStamp {
			thisTimestamp := event.Edge.Property.GetTimestamp()
			if thisTimestamp >= nextTarget {
				nextTarget = thisTimestamp + g.Options.TimeSeriesInterval
				g.ExecuteQuery(thisTimestamp)
			}
		}

		if targetRate != 0 && remitted > targetEventCount {
			targetEventCount = uint64(g.Watch.Elapsed().Seconds() * targetRate)
			if remitted > targetEventCount { // Going too fast
				extraEventCount := remitted - targetEventCount
				sleepTime := float64(extraEventCount) / targetRate * float64(time.Nanosecond)
				sleepTime = utils.Max(REMITTER_MIN_SLEEP_TIME_NANO, sleepTime)
				time.Sleep(time.Duration(sleepTime))
				targetEventCount = uint64(g.Watch.Elapsed().Seconds() * targetRate)
			}
		}
	}

	if g.Options.DebugLevel >= 3 {
		log.Debug().Msg("Remitter retried " + utils.F("%10d", totalRetriedOrder) + " (get order)")
		log.Debug().Msg("Remitter retried " + utils.F("%10d", totalRetriedThreads) + " (get TRE)")
		gLeft := make([]int, g.NumThreads)
		for i := 0; i < int(g.NumThreads); i++ {
			gLeft[i] = int(g.GraphThreads[i].TopologyQueue.MaxGrow)
		}
		log.Debug().Msg("Remitter retried " + utils.F("%10d", totalPutFails) + " (put S)     Grows left: " + utils.V(gLeft))
	}

	// When all events have been remitted, we can close the TopologyQueues.
	for t := 0; t < int(g.NumThreads); t++ {
		g.GraphThreads[t].TopologyQueue.Close()
	}
	order.End()
	runtime.UnlockOSThread()
	return remitted
}

// Emitter is one thread that presents events in linear order to the system.
// It sends corresponding TopologyEvent to the FromEmitQueue queue of the source vertex's thread.
func Emitter[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N], edgeQueues []utils.RingBuffSPSC[TopologyEvent[E]], order *utils.GrowableRingBuff[uint32]) {
	runtime.LockOSThread()
	undirected := g.Options.Undirected
	logicalTime := g.Options.LogicalTime
	eventIdx := uint64(0)
	retried := 0
	totalRetried := 0
	putFails := 0
	gtPutFails := 0
	eventBatch := make([]TopologyEvent[E], g.Options.MlaBatch)
	eventBatchPlacement := make([]utils.Pair[uint32, uint32], g.Options.MlaBatch)
	eventBatchIdx := 0
	closed := false
	if len(edgeQueues) != 1 {
		log.Panic().Msg("Only 1 load thread supported")
	}

	for !closed {
	batch:
		// Round robin through threads, same how the threads are assigned to positions in the event log, so we receive in order.
		for {
			for i := 0; i < len(edgeQueues); i++ {
				event, ok, pos := edgeQueues[i].GetFast()
				if !ok {
					if event, closed, retried = edgeQueues[i].GetSlow(pos); closed {
						break batch
					}
					totalRetried += retried
				}
				event.TypeAndEventIdx |= (eventIdx << EVENT_TYPE_BITS)
				eventIdx++

				eventBatch[eventBatchIdx] = event
				eventBatchIdx++
				// if eventBatchIdx == int(g.Options.MlaBatch) {
				// 	break batch
				// }
				if eventBatchIdx == int(g.Options.MlaBatch) && i+1 == len(edgeQueues) {
					break batch
				}
			}
		}

		g.Partitioner(g, eventBatch, eventBatchPlacement, eventBatchIdx, undirected)
		// g.FindVertexPlacementBulkIndividual(eventBatch, eventBatchPlacement, eventBatchIdx, undirected)

		for i := 0; i < eventBatchIdx; i++ {
			event := eventBatch[i]
			srcId, dstId := eventBatchPlacement[i].First, eventBatchPlacement[i].Second
			srcTidx, dstTidx := IdxToTidx(srcId), IdxToTidx(dstId)

			internalEvent := InternalTopologyEvent[E]{TypeAndEventIdx: event.TypeAndEventIdx, SrcRaw: event.SrcRaw, DstRaw: event.DstRaw, SrcIdx: srcId, Edge: Edge[E]{
				Property: event.EdgeProperty,
				Didx:     dstId,
				Pos:      ^uint32(0),
			}}
			if pos, ok := g.GraphThreads[dstTidx].FromEmitQueue.PutFast(internalEvent); !ok {
				gtPutFails += g.GraphThreads[dstTidx].FromEmitQueue.PutSlow(internalEvent, pos)
			}
			if pos, ok := order.PutFast(dstTidx); !ok {
				putFails += order.PutSlow(dstTidx, pos)
			}

			//log.Debug().Msg("Emitter " + utils.V(event) + " to " + utils.V(targetTidx))

			if undirected {
				uEventIdx := event.TypeAndEventIdx & EVENT_TYPE_MASK
				uEventIdx |= (eventIdx << EVENT_TYPE_BITS)
				uEvent := InternalTopologyEvent[E]{
					TypeAndEventIdx: uEventIdx, SrcRaw: event.DstRaw, DstRaw: event.SrcRaw,
					SrcIdx: dstId,
					Edge: Edge[E]{
						Property: event.EdgeProperty,
						Didx:     srcId,
						Pos:      ^uint32(0),
					},
				}

				EP(&uEvent.Edge.Property).ReplaceRaw(event.SrcRaw)
				if logicalTime {
					EP(&uEvent.Edge.Property).ReplaceTimestamp(eventIdx)
				}
				eventIdx++
				if pos, ok := g.GraphThreads[srcTidx].FromEmitQueue.PutFast(uEvent); !ok {
					gtPutFails += g.GraphThreads[srcTidx].FromEmitQueue.PutSlow(uEvent, pos)
				}
				if pos, ok := order.PutFast(srcTidx); !ok {
					putFails += order.PutSlow(srcTidx, pos)
				}
			}
		}

		eventBatchIdx = 0
	}

	if g.Options.DebugLevel >= 3 {
		log.Debug().Msg("Emitter  retried " + utils.F("%10d", totalRetried) + " (get edges)")
		for i := 0; i < len(edgeQueues); i++ {
			edgeQueues[i].End()
		}
		gLeft := make([]int, g.NumThreads)
		for t := 0; t < int(g.NumThreads); t++ {
			gLeft[t] = int(g.GraphThreads[t].FromEmitQueue.MaxGrow)
		}
		log.Debug().Msg("Emitter  retried " + utils.F("%10d", gtPutFails) + " (put FEE)   Grows left: " + utils.V(gLeft))
		log.Debug().Msg("Emitter  retried " + utils.F("%10d", putFails) + " (put order) Grows left: " + utils.V(order.MaxGrow))
	}

	for t := 0; t < int(g.NumThreads); t++ {
		g.GraphThreads[t].FromEmitQueue.Close()
	}
	order.Close()
	runtime.UnlockOSThread()
}

func EdgeEnqueueToEmitter[EP EPP[E], E EPI[E]](name string, myIndex uint64, enqCount uint64, edgeQueue *utils.RingBuffSPSC[TopologyEvent[E]], wPos int32, tPos int32, transpose bool, undirected bool, logicalTime bool) {
	runtime.LockOSThread()
	file := utils.OpenFile(name)
	fieldsBuff := [MAX_ELEMS_PER_EDGE]string{}
	fields := fieldsBuff[:]
	scanner := utils.FastFileLines{}
	scannerBuff := [4096 * 16]byte{}
	scanner.Buf = scannerBuff[:]
	var event TopologyEvent[E]
	var b []byte
	var remaining []string
	parseProp := wPos >= 0 || tPos >= 0
	undirectedMul := uint64(1)
	if undirected {
		undirectedMul = 2
	}

	for lines := uint64(0); ; lines++ {
		if i := bytes.IndexByte(scannerBuff[scanner.Start:scanner.End], '\n'); i >= 0 {
			b = scannerBuff[scanner.Start : scanner.Start+i]
			scanner.Start += i + 1
		} else {
			if b = scanner.Scan(file); b == nil {
				break
			}
		}
		if (b[0]) == '#' {
			lines-- // To match the count of actual events.
			continue
		}
		if (lines % enqCount) == myIndex {
			utils.FastFields(fields, b)
			event, remaining = EdgeParser[E](fields)
			EP(&event.EdgeProperty).ReplaceWeight(DEFAULT_WEIGHT)
			if parseProp {
				EP(&event.EdgeProperty).ParseProperty(remaining, wPos, tPos)
			}
			if logicalTime {
				EP(&event.EdgeProperty).ReplaceTimestamp(lines * undirectedMul)
			}
			if transpose {
				event.SrcRaw, event.DstRaw = event.DstRaw, event.SrcRaw
			}
			EP(&event.EdgeProperty).ReplaceRaw(event.DstRaw)

			if pos, ok := edgeQueue.PutFast(event); !ok {
				edgeQueue.PutSlow(event, pos)
			}

			//log.Debug().Msg("Edge " + utils.V(event))
		}
	}
	edgeQueue.Close()
	file.Close()
	runtime.UnlockOSThread()
}

// Starts to read edges stored in the file. When it returns, all edges are read.
func LoadGraphStream[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N], wg *sync.WaitGroup) {
	loadThreads := uint64(g.Options.LoadThreads)

	log.Info().Msg("Input stream from " + g.Options.Name + " with " + utils.V(loadThreads) + " load threads")

	order := new(utils.GrowableRingBuff[uint32])
	order.Init(BASE_SIZE*uint64(g.NumThreads), GROW_LIMIT)

	// Launch the Edge Enqueue threads.
	edgeQueues := make([]utils.RingBuffSPSC[TopologyEvent[E]], loadThreads)
	for i := uint64(0); i < loadThreads; i++ {
		edgeQueues[i].Init(BASE_SIZE * 8) // Small seems fine
		go EdgeEnqueueToEmitter[EP](g.Options.Name, i, loadThreads, &edgeQueues[i], int32(g.Options.WeightPos-1), int32(g.Options.TimestampPos-1), g.Options.Transpose, g.Options.Undirected, g.Options.LogicalTime)
	}

	// Launch the Emitter thread.
	go Emitter[EP](g, edgeQueues, order)

	// This thread becomes the Remitter thread.
	lines := g.Remitter(order)

	time := g.Watch.Elapsed()
	log.Info().Msg("Streamed " + utils.V(lines) + " events in (ms): " + utils.V(time.Milliseconds()))
	log.Trace().Msg(", stream, " + utils.F("%0.3f", time.Seconds()*1000))
	wg.Done()
}

// ---------------------------- For testing ----------------------------

// For testing. Launches a remitter only; so you can be an emitter.
func (g *Graph[V, E, M, N]) StreamRemitOnly() (order *utils.GrowableRingBuff[uint32]) {
	order = new(utils.GrowableRingBuff[uint32])
	order.Init(BASE_SIZE*uint64(g.NumThreads), GROW_LIMIT)
	go g.Remitter(order)
	return order
}

// For testing. Direct add
func (g *Graph[V, E, M, N]) SendAdd(srcRaw RawType, dstRaw RawType, EdgeProperty E, order *utils.GrowableRingBuff[uint32]) {
	event := TopologyEvent[E]{TypeAndEventIdx: uint64(ADD), SrcRaw: srcRaw, DstRaw: dstRaw, EdgeProperty: EdgeProperty}

	srcId, dstId := g.FindVertexPlacement(event, false)
	internalEvent := InternalTopologyEvent[E]{TypeAndEventIdx: event.TypeAndEventIdx, SrcRaw: event.SrcRaw, DstRaw: event.DstRaw, SrcIdx: srcId, Edge: Edge[E]{
		Property: event.EdgeProperty,
		Didx:     dstId,
		Pos:      ^uint32(0),
	}}

	targetIdx := IdxToTidx(dstId)
	if pos, ok := g.GraphThreads[targetIdx].FromEmitQueue.PutFast(internalEvent); !ok {
		g.GraphThreads[targetIdx].FromEmitQueue.PutSlow(internalEvent, pos)
	}
	if pos, ok := order.PutFast(targetIdx); !ok {
		order.PutSlow(targetIdx, pos)
	}
}

// For testing. Direct delete
func (g *Graph[V, E, M, N]) SendDel(srcRaw RawType, dstRaw RawType, EdgeProperty E, order *utils.GrowableRingBuff[uint32]) {
	event := TopologyEvent[E]{TypeAndEventIdx: uint64(DEL), SrcRaw: srcRaw, DstRaw: dstRaw, EdgeProperty: EdgeProperty}

	srcId, dstId := g.FindVertexPlacement(event, false)
	internalEvent := InternalTopologyEvent[E]{TypeAndEventIdx: event.TypeAndEventIdx, SrcRaw: event.SrcRaw, DstRaw: event.DstRaw, SrcIdx: srcId, Edge: Edge[E]{
		Property: event.EdgeProperty,
		Didx:     dstId,
		Pos:      ^uint32(0),
	}}

	targetIdx := IdxToTidx(dstId)
	if pos, ok := g.GraphThreads[targetIdx].FromEmitQueue.PutFast(internalEvent); !ok {
		g.GraphThreads[targetIdx].FromEmitQueue.PutSlow(internalEvent, pos)
	}
	if pos, ok := order.PutFast(targetIdx); !ok {
		order.PutSlow(targetIdx, pos)
	}
}
