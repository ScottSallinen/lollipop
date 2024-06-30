package graph

import (
	"bytes"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

type EventType uint32

const (
	ADD EventType = iota // Implicit, 0, means add.
	DEL
	QUERY // Signals a query. Would be sent to all threads.
	// UPDATE // not used yet. Update *which* edge? To revisit with more consideration with multi-graphs, which is the assumed mode right now
)

const EVENT_TYPE_BITS = 2
const EVENT_TYPE_MASK = (1 << EVENT_TYPE_BITS) - 1
const REMITTER_MIN_SLEEP_TIME_NANO = 10_000_000 // 10ms

// TypeAndEventIdx & EVENT_TYPE_MASK
func (t InputEvent[E]) EventType() EventType {
	return EventType(t.TypeAndEventIdx & EVENT_TYPE_MASK)
}

// TypeAndEventIdx >> EVENT_TYPE_BITS
func (t InputEvent[E]) EventIdx() uint64 {
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
func (e RemitEvent[E]) EventType() EventType {
	return EventType(e.TypeAndEventIdx & EVENT_TYPE_MASK)
}

// TypeAndEventIdx >> EVENT_TYPE_BITS
func (e RemitEvent[E]) EventIdx() uint64 {
	return uint64(e.TypeAndEventIdx >> EVENT_TYPE_BITS)
}

func (t EventType) String() string {
	switch t {
	case ADD:
		return "ADD"
	case DEL:
		return "DEL"
	case QUERY:
		return "QUERY"
	//case UPDATE:
	//	return "UPDATE"
	default:
		return "UNKNOWN"
	}
}

// A basic topology event that refers to the external, raw identifiers.
type InputEvent[E EPI[E]] struct {
	TypeAndEventIdx uint64  // Type for add, del, etc. EventIdx is global total order.
	SrcRaw          RawType // External, raw identifiers.
	DstRaw          RawType // External, raw identifiers.
	EdgeProperty    E       // The property of the edge, as supplied by external input.
	ThreadOrderSrc  uint64  // For src thread, the emmiter tracked index.
	ThreadOrderDst  uint64  // For dst thread, the emmiter tracked index.
}

// A topology event that has had the edge remapped to the internal index.
type RawEdgeEvent[E EPI[E]] struct {
	TypeAndEventIdx uint64
	Sidx            uint32  // Source internal ID
	Edge            Edge[E] // Didx, Pos, Property
}

type RemitEvent[E EPI[E]] struct {
	TypeAndEventIdx uint64
	Order           uint64
	Edge            Edge[E] // Didx, Pos, Property
}

func (t InputEvent[E]) String() string {
	return "{Type: " + utils.V(t.EventType()) + ", EventIdx: " + utils.V(t.EventIdx()) + ", SrcRaw: " + utils.V(t.SrcRaw) + ", DstRaw: " + utils.V(t.DstRaw) + ", EdgeProperty: " + utils.V(t.EdgeProperty) + ", ThreadOrderSrc: " + utils.V(t.ThreadOrderSrc) + ", ThreadOrderDst: " + utils.V(t.ThreadOrderDst) + "}"
}

func (e RawEdgeEvent[E]) String() string {
	return "{Type: " + utils.V(e.EventType()) + ", EventIdx: " + utils.V(e.EventIdx()) + ", Edge: " + e.Edge.String()
}

// Emitter is one thread that presents events in linear order to the system.
// It sends corresponding TopologyEvent to the FromEmitQueue queue of the source vertex's thread.
func Emitter[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N], edgeQueues []utils.RingBuffSPSC[InputEvent[E]]) (lines uint64) {
	runtime.LockOSThread()
	pid := syscall.Getpid()
	syscall.Setpriority(syscall.PRIO_PROCESS, pid, 19)
	undirected := g.Options.Undirected
	logicalTime := g.Options.LogicalTime
	queryByEventCount := g.Options.LogTimeseries && g.Options.TimeseriesEdgeCount
	queryByTimeStamp := g.Options.LogTimeseries && !g.Options.TimeseriesEdgeCount
	nextTarget := g.Options.TimeSeriesInterval
	if !undirected {
		nextTarget += g.Options.TimeSeriesInterval
	}
	targetRate := g.Options.TargetRate // events per second
	targetEventCount := uint64(0)
	globalEventIdx := uint64(0)
	retried := 0
	totalRetried := 0
	gtPutFails := 0
	var event InputEvent[E]
	var ok bool
	var closed bool
	var pos uint64
	THREADS := g.NumThreads
	threadOrders := make([]uint64, THREADS)

outer:
	for {
		// Round robin through threads, same how the threads are assigned to positions in the event log, so we receive in order.
		for i := 0; i < len(edgeQueues); i++ {
			if event, ok, pos = edgeQueues[i].GetFast(); !ok {
				if event, closed, retried = edgeQueues[i].GetSlow(pos); closed {
					break outer
				}
				totalRetried += retried
			}
			event.TypeAndEventIdx |= (globalEventIdx << EVENT_TYPE_BITS)
			globalEventIdx++
			dstTargetTidx := event.DstRaw.Within(THREADS)
			srcTargetTidx := event.SrcRaw.Within(THREADS)
			event.ThreadOrderDst = threadOrders[dstTargetTidx]
			threadOrders[dstTargetTidx]++
			event.ThreadOrderSrc = threadOrders[srcTargetTidx]
			threadOrders[srcTargetTidx]++

			// Send to the destination thread.
			if pos, ok = g.GraphThreads[dstTargetTidx].FromEmitQueue.PutFast(event); !ok {
				gtPutFails += g.GraphThreads[dstTargetTidx].FromEmitQueue.PutSlow(event, pos)
			}

			// Increment event.
			event.TypeAndEventIdx &= EVENT_TYPE_MASK
			event.TypeAndEventIdx |= (globalEventIdx << EVENT_TYPE_BITS)
			if logicalTime {
				EP(&event.EdgeProperty).ReplaceTimestamp(globalEventIdx)
			}
			// Swap src and dst for inverse of the edge.
			event.SrcRaw, event.DstRaw = event.DstRaw, event.SrcRaw
			event.ThreadOrderDst, event.ThreadOrderSrc = event.ThreadOrderSrc, event.ThreadOrderDst
			EP(&event.EdgeProperty).ReplaceRaw(event.DstRaw)
			globalEventIdx++

			// Send to the source thread.
			if pos, ok = g.GraphThreads[srcTargetTidx].FromEmitQueue.PutFast(event); !ok {
				gtPutFails += g.GraphThreads[srcTargetTidx].FromEmitQueue.PutSlow(event, pos)
			}

			// Check to interrupt to ask a query if we've reached the next target
			if queryByEventCount {
				if globalEventIdx >= nextTarget {
					nextTarget += g.Options.TimeSeriesInterval
					if !undirected {
						nextTarget += g.Options.TimeSeriesInterval
					}
					// All threads receive the request for query at this point in time.
					event = InputEvent[E]{TypeAndEventIdx: uint64(QUERY)}
					for t := 0; t < int(g.NumThreads); t++ {
						if pos, ok := g.GraphThreads[t].FromEmitQueue.PutFast(event); !ok {
							g.GraphThreads[t].FromEmitQueue.PutSlow(event, pos)
						}
					}
					g.LogEntryChan <- globalEventIdx
				}
			} else if queryByTimeStamp {
				thisTimestamp := event.EdgeProperty.GetTimestamp()
				if thisTimestamp >= nextTarget {
					nextTarget = thisTimestamp + g.Options.TimeSeriesInterval
					// All threads receive the request for query at this point in time.
					event = InputEvent[E]{TypeAndEventIdx: uint64(QUERY)}
					for t := 0; t < int(g.NumThreads); t++ {
						if pos, ok := g.GraphThreads[t].FromEmitQueue.PutFast(event); !ok {
							g.GraphThreads[t].FromEmitQueue.PutSlow(event, pos)
						}
					}
					g.LogEntryChan <- globalEventIdx
				}
			}

			if targetRate != 0 && globalEventIdx > targetEventCount {
				targetEventCount = uint64(g.Watch.Elapsed().Seconds() * targetRate)
				if globalEventIdx > targetEventCount { // Going too fast
					extraEventCount := globalEventIdx - targetEventCount
					sleepTime := float64(extraEventCount) / targetRate * float64(time.Nanosecond)
					sleepTime = utils.Max(REMITTER_MIN_SLEEP_TIME_NANO, sleepTime)
					time.Sleep(time.Duration(sleepTime))
					targetEventCount = uint64(g.Watch.Elapsed().Seconds() * targetRate)
				}
			}
		}
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
	}

	for t := 0; t < int(g.NumThreads); t++ {
		g.GraphThreads[t].FromEmitQueue.Close()
	}
	runtime.UnlockOSThread()
	if !undirected {
		return globalEventIdx / 2
	}
	return globalEventIdx
}

func FileEdgeEnqueueToEmitter[EP EPP[E], E EPI[E]](name string, myIndex uint64, enqCount uint64, edgeQueue *utils.RingBuffSPSC[InputEvent[E]], wPos int32, tPos int32, transpose bool, undirected bool, logicalTime bool) {
	runtime.LockOSThread()
	pid := syscall.Getpid()
	syscall.Setpriority(syscall.PRIO_PROCESS, pid, 18)
	file := utils.OpenFile(name)
	fieldsBuff := [MAX_ELEMS_PER_EDGE]string{}
	fields := fieldsBuff[:]
	scanner := utils.FastFileLines{}
	scannerBuff := [4096 * 16]byte{}
	scanner.Buf = scannerBuff[:]
	var event InputEvent[E]
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
func LoadFileGraphStream[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N], wg *sync.WaitGroup) {
	loadThreads := uint64(g.Options.LoadThreads)

	log.Info().Msg("Input stream from " + g.Options.Name + " with " + utils.V(loadThreads) + " load threads")

	// Launch the Edge Enqueue threads.
	edgeQueues := make([]utils.RingBuffSPSC[InputEvent[E]], loadThreads)
	for i := uint64(0); i < loadThreads; i++ {
		edgeQueues[i].Init(BASE_SIZE * 8) // Small seems fine
		go FileEdgeEnqueueToEmitter[EP](g.Options.Name, i, loadThreads, &edgeQueues[i], int32(g.Options.WeightPos-1), int32(g.Options.TimestampPos-1), g.Options.Transpose, g.Options.Undirected, g.Options.LogicalTime)
	}

	// This thread becomes the Emitter.
	lines := Emitter[EP](g, edgeQueues)

	time := g.Watch.Elapsed()
	log.Info().Msg("Streamed " + utils.V(lines) + " events in (ms): " + utils.V(time.Milliseconds()))
	log.Trace().Msg(", stream, " + utils.F("%0.3f", time.Seconds()*1000))
	wg.Done()
}

// ---------------------------- For testing ----------------------------

// For testing. Not optimized. Lets you use one custom stream to build the graph.
// TODO: this won't play nice with the backoff strategy!
func TestGraphStream[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N]) (edgeQueue *utils.RingBuffSPSC[InputEvent[E]]) {
	edgeQueues := make([]utils.RingBuffSPSC[InputEvent[E]], 1)
	edgeQueues[0].Init(BASE_SIZE * 8) // Small seems fine

	go Emitter[EP](g, edgeQueues)
	return &edgeQueues[0]
}

// For testing. Direct add
func (g *Graph[V, E, M, N]) SendAdd(srcRaw RawType, dstRaw RawType, EdgeProperty E, edgeQueue *utils.RingBuffSPSC[InputEvent[E]]) {
	typeAndEventIdx := uint64(ADD)
	event := InputEvent[E]{TypeAndEventIdx: typeAndEventIdx, SrcRaw: RawType(srcRaw), DstRaw: RawType(dstRaw), EdgeProperty: EdgeProperty}
	if pos, ok := edgeQueue.PutFast(event); !ok {
		edgeQueue.PutSlow(event, pos)
	}
}

// For testing. Direct delete
func (g *Graph[V, E, M, N]) SendDel(srcRaw RawType, dstRaw RawType, EdgeProperty E, edgeQueue *utils.RingBuffSPSC[InputEvent[E]]) {
	typeAndEventIdx := uint64(DEL)
	event := InputEvent[E]{TypeAndEventIdx: typeAndEventIdx, SrcRaw: srcRaw, DstRaw: dstRaw, EdgeProperty: EdgeProperty}
	if pos, ok := edgeQueue.PutFast(event); !ok {
		edgeQueue.PutSlow(event, pos)
	}
}
