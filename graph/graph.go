package graph

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/rs/zerolog/log"

	_ "net/http/pprof"

	"github.com/ScottSallinen/lollipop/utils"
)

/*
func init() {
	// runtime.SetMutexProfileFraction(1)
	// debug.SetGCPercent(-1)
}
*/

// Defines max threads. Note: this may limit graph size with small thread counts. For testing a large graph with 1 thread, set this lower!
const THREAD_MAX = 1 << THREAD_BITS         // (32) Max threads.
const THREAD_BITS = 5                       // Bit count
const THREAD_SHIFT = 32 - THREAD_BITS       // Bit offset to make thread bits first in the uint32
const THREAD_MASK = (1 << THREAD_SHIFT) - 1 // Bit mask

// Some constants for the graph.
const BASE_SIZE = 4096 * 4
const GROW_LIMIT = 7             // Number of times certain buffers can grow (double in size)
const MSG_MAX = 1024             // Max messages a thread MAY pull from the queue at a time, before cycling back to check other tasks. A message is a notification genuinely sent (e.g. not discarded due to non-uniqueness).
const FAKE_TIMESTAMP = false     // Replaces timestamp with an ordinal index. Useful for testing.
const QUERY_EMULATE_BSP = false  // (Only if NoConvergeForQuery) If enabled, queries block in a fashion that makes the system emulate a bulk synchronous design. Slower than async, but good for debugging.
const TOPOLOGY_FIRST = false     // Should be false. Only process topology events (no algorithm-only events) until the topology is fully loaded. For testing how useful the topology hooks only are.
const QUERY_NON_BLOCKING = false // Should be false. Determines if the input stream should be non blocked while waiting for a query result -- this way until we can make a better async state view. If a small rate or bundle size is used it could work (otherwise the system moves too fast).

// For vertex allocation.
const BUCKET_SHIFT = 12
const BUCKET_SIZE = 1 << BUCKET_SHIFT
const BUCKET_MASK = BUCKET_SIZE - 1

// Graph type. This is the main data structure for the graph.
type Graph[V VPI[V], E EPI[E], M MVI[M], N any] struct {
	_                   [0]atomic.Int64                     // Alignment for the GraphThreads.
	GraphThreads        [THREAD_MAX]GraphThread[V, E, M, N] // Graph threads. NOTE: do not len(GraphThreads)! Use g.NumThreads instead. (it is a fixed size for offset purposes).
	NumThreads          uint32                              // Number of threads.
	SourceInit          bool                                // Set true if detected that source vertices are targeted for initialization (rather than by All).
	NoteInit            bool                                // Set true if detected that notifications are used for initialization (rather than mail).
	Options             GraphOptions                        // Graph options.
	InitMails           map[RawType]M                       // If used, takes priority over InitAllMail -- if set, will not use InitAllMail! A map from raw vertex ID to an initial Mail they receive.
	InitNotes           map[RawType]N                       // If used, takes priority over InitAllNote, but not either Mail type. A map from raw vertex ID to an initial Notification they receive.
	TerminateData       []int64                             // Value for each thread to share their (sent - received) messages.
	TerminateView       []int64                             // Thread view to terminate.
	TerminateVotes      []int64                             // Vote to terminate.
	Watch               utils.Watch                         // General (e.g. wall clock) timer for the graph.
	AlgTimer            utils.Watch                         // Timer for the algorithm, to sometimes determine algorithm-specific performance (e.g., derive query interruption compared to wall clock).
	LogEntryChan        chan uint64                         // Channel for logging timeseries data.
	QueryWaiter         sync.WaitGroup                      // Wait group for queries, if queries are blocking.
	SuperStepWaiter     SuperStepWaiter                     // Synchronize threads for algorithmic super steps.
	OracleCache         *Graph[V, E, M, N]                  // For debugging: if non-nil, will use this graph as the oracle instead of (re)-computing it.
	warnOutOfOrderInput uint64                              // Detection of out of order input, during streaming; used for a unique notification.
	warnSelectDelete    uint64                              // Detection of deletions that are selective; used for a unique notification.
	warnBackPressure    uint64                              // Detection of back-pressure on the notification queue; used for a unique notification.
	warnZeroTimestamp   uint64                              // Detection of zero timestamp; used for a unique notification.
}

// Graph Thread. Elements here are typically thread-local only (though queues/channels have in/out positions).
type GraphThread[V VPI[V], E EPI[E], M MVI[M], N any] struct {
	_               [0]atomic.Int64                  // Alignment.
	Vertices        []Vertex[V, E]                   // Internal vertex storage (see vertex.go). Ok to mem move, as other threads are not allowed to access.
	VertexMailboxes []*[BUCKET_SIZE]VertexMailbox[M] // For intra-node communication. Must NOT mem move.
	Tidx            uint16                           // Thread self index.
	Status          GraphThreadStatus                // Thread's status. For debugging.
	NumEdges        uint32                           // Thread's current total outgoing edge count (i.e., adds less deletes; ease of access here).
	Command         chan Command                     // Incoming command channel.

	NotificationQueue utils.RingBuffMPSC[Notification[N]] // Inbound notification queue for the thread.
	NotificationBuff  utils.Deque[Notification[N]]        // Overfill buffer for notifications.

	Notifications     []Notification[N] // Regular buffer for notifications. A notification represents a vertex is 'active' as it has work to do (e.g. has mail in its inbox, or the notification itself is important).
	MsgSend           uint64            // Number of messages sent by the thread. A message is a notification genuinely sent (e.g. not discarded due to non-uniqueness).
	MsgRecv           uint64            // Number of messages received by the thread.  A message is a notification genuinely sent (e.g. not discarded due to non-uniqueness).
	TopologyEventBuff []RawEdgeEvent[E] // Buffer for thread topology events that are ready to apply (been remitted).

	VertexStructures  []*[BUCKET_SIZE]VertexStructure // Supplemental vertex structure (see vertex.go). Tracks extra structural properties (e.g. internal to external id).
	VertexMap         map[RawType]uint32              // Per-Vertex: Raw (external) to internal ID.
	VertexPendingBuff [][]uint32                      // Pending topology events; used for offsets into the TopologyEventBuff buffer.
	NumUnique         uint64                          // Used for offset tracking. Number of unique vertices processed; after merging consecutive events for the same vertex. Starts at 1.

	NumOutAdds   uint32                                // Number of outgoing edges added by the thread.
	NumOutDels   uint32                                // Number of outgoing edges deleted by the thread.
	NumInEvents  uint64                                // Thread's total incoming event count. Tracked by the to-remit process. (unused)
	ExpiredEdges []utils.Pair[uint32, RawEdgeEvent[E]] // Set of an edge for a vertex that will need to be removed.
	LoopTimes    []time.Duration                       // For debugging.

	TopologyQueue utils.GrowableRingBuff[RawEdgeEvent[E]]
	FromEmitQueue utils.GrowableRingBuff[TopologyEvent[E]]
	ToRemitQueue  utils.RingBuffSPSC[RawEdgeEvent[E]]

	Response     chan Command // Response channel
	EventActions uint64       // Number of event actions (to or from remitter) performed by the thread.
	_            [6]uint64
}

// Allocates everything needed for a new graph.
func (g *Graph[V, E, M, N]) Init() {
	g.NumThreads = g.Options.NumThreads
	if g.NumThreads == 0 { // Potentially was the default value if not defined.
		g.NumThreads = 1
	} else if g.NumThreads > THREAD_MAX {
		g.NumThreads = THREAD_MAX
		log.Warn().Msg("NumThreads " + utils.V(g.NumThreads) + " > THREAD_MAX " + utils.V(THREAD_MAX) + ", setting to THREAD_MAX. If you need more threads, please adjust THREAD_MAX and recompile.")
	}

	if g.Options.LoadThreads == 0 {
		g.Options.LoadThreads = 1
	}

	g.SuperStepWaiter.Init(int(g.NumThreads))

	notifQueueSize := BASE_SIZE
	if g.Options.QueueMultiplier > 0 {
		notifQueueSize *= (1 << g.Options.QueueMultiplier)
	}

	for t := 0; t < int(g.NumThreads); t++ {
		gt := &g.GraphThreads[t]
		gt.Tidx = uint16(t)
		gt.Command = make(chan Command, 1)
		gt.Response = make(chan Command, 1)
		gt.NumUnique = 1 // 0 is reserved for comparison against zeroed allocation.

		gt.TopologyEventBuff = make([]RawEdgeEvent[E], BASE_SIZE*64)
		gt.Notifications = make([]Notification[N], MSG_MAX)

		gt.NotificationQueue.Init(uint64(notifQueueSize) * uint64(THREAD_MAX/g.NumThreads))

		gt.TopologyQueue.Init(BASE_SIZE, GROW_LIMIT)
		gt.FromEmitQueue.Init(BASE_SIZE, GROW_LIMIT)
		gt.ToRemitQueue.Init(BASE_SIZE * 4 * THREAD_MAX) // TODO: size?

		gt.ExpiredEdges = make([]utils.Pair[uint32, RawEdgeEvent[E]], 0, 512)
		gt.VertexPendingBuff = make([][]uint32, 1024)
		for i := uint32(0); i < 1024; i++ {
			g.GraphThreads[t].VertexPendingBuff[i] = make([]uint32, 0, 512)
		}

		gt.Vertices = make([]Vertex[V, E], 0, (BASE_SIZE * 4))
		gt.VertexMailboxes = make([]*[BUCKET_SIZE]VertexMailbox[M], 0, (BASE_SIZE*4)/BUCKET_SIZE)
		gt.VertexStructures = make([]*[BUCKET_SIZE]VertexStructure, 0, (BASE_SIZE*4)/BUCKET_SIZE)
		gt.VertexMap = make(map[RawType]uint32, (BASE_SIZE * 4))
	}

	g.TerminateData = make([]int64, g.NumThreads)
	g.TerminateView = make([]int64, g.NumThreads)
	g.TerminateVotes = make([]int64, g.NumThreads)
	g.LogEntryChan = make(chan uint64, 64) // TODO: size? Really only needs to be 1 for blocking queries.

	if g.Options.DebugLevel >= 3 || g.Options.Profile {
		log.Debug().Msg("Bit sizes (  Given  ): VertexPropType: " + utils.V(unsafe.Sizeof(*new(V))) + " EdgePropType: " + utils.V(unsafe.Sizeof(*new(E))) + " MsgType: " + utils.V(unsafe.Sizeof(*new(M))) + " NoteType " + utils.V(unsafe.Sizeof(*new(N))))
		log.Debug().Msg("Bit sizes (Ephemeral): TopologyEvent: " + utils.V(unsafe.Sizeof(*new(TopologyEvent[E]))) + " RawEdgeEvent: " + utils.V(unsafe.Sizeof(*new(RawEdgeEvent[E]))) + " Notification: " + utils.V(unsafe.Sizeof(*new(Notification[N]))))
		log.Debug().Msg("Bit sizes ( Derived ): Vertex: " + utils.V(unsafe.Sizeof(*new(Vertex[V, E]))) + " VertexMsg: " + utils.V(unsafe.Sizeof(*new(VertexMailbox[M]))) + " VertexStruc " + utils.V(unsafe.Sizeof(*new(VertexStructure))) + " Edge: " + utils.V(unsafe.Sizeof(*new(Edge[E]))))
		log.Debug().Msg("Bit sizes: Graph: " + utils.V(unsafe.Sizeof(*new(Graph[V, E, M, N]))) + " GraphThread: " + utils.V(unsafe.Sizeof(*new(GraphThread[V, E, M, N]))) + " GraphOptions: " + utils.V(unsafe.Sizeof(*new(GraphOptions))))
		log.Debug().Msg("Bit sizes: RingBuffSPSC: " + utils.V(unsafe.Sizeof(*new(utils.RingBuffSPSC[TopologyEvent[E]]))) + " RingBuffMPSC: " + utils.V(unsafe.Sizeof(*new(utils.RingBuffMPSC[uint32]))) + " GrowableRingBuff: " + utils.V(unsafe.Sizeof(*new(utils.GrowableRingBuff[TopologyEvent[E]]))) + " NotifDeque " + utils.V(unsafe.Sizeof(*new(utils.Deque[Notification[N]]))))
		log.Debug().Msg("Initial caps: FromEmit: " + utils.V(g.GraphThreads[0].FromEmitQueue.EnqCap()) + " ToRemit: " + utils.V(g.GraphThreads[0].ToRemitQueue.EnqCap()))
	}

	if g.Options.DebugLevel >= 3 || g.Options.Profile {
		utils.MemoryStats()
	}

	g.Watch.Start()
}

// --------------- Commands to Graph Threads ---------------

type Command uint8

const (
	ACK Command = iota
	RESUME
	BLOCK_TOP
	BLOCK_TOP_ASYNC // Similar to BLOCK_TOP but does not wait for an ACK
	BLOCK_ALL
	BLOCK_ALG_IF_TOP // Init command, no ack
	BSP_SYNC
	EPOCH
)

func (g *Graph[V, E, M, N]) Broadcast(command Command) {
	for i := uint32(0); i < g.NumThreads; i++ {
		g.GraphThreads[i].Command <- command
	}
}

func (g *Graph[V, E, M, N]) AwaitAck() {
	for i := uint32(0); i < g.NumThreads; i++ {
		<-g.GraphThreads[i].Response
	}
}

func (g *Graph[V, E, M, N]) ExecuteQuery(entry uint64) {
	g.QueryWaiter.Add(1)
	g.LogEntryChan <- entry
	if !QUERY_NON_BLOCKING {
		g.QueryWaiter.Wait() // Wait for the query to finish before processing new events
	}
}

type GraphThreadStatus uint16

const (
	REMIT GraphThreadStatus = iota
	RECV_TOP
	APPLY_TOP
	RECV_MSG
	APPLY_MSG
	RECV_CMD
	BACKOFF_ALG
	BACKOFF_TOP
	DONE
)

func (s GraphThreadStatus) String() string {
	switch s {
	case REMIT:
		return "REMIT"
	case RECV_TOP:
		return "RECV_TOP"
	case APPLY_TOP:
		return "APPLY_TOP"
	case RECV_MSG:
		return "RECV_MSG"
	case APPLY_MSG:
		return "APPLY_MSG"
	case RECV_CMD:
		return "RECV_CMD"
	case BACKOFF_ALG:
		return "BACKOFF_ALG"
	case BACKOFF_TOP:
		return "BACKOFF_TOP"
	case DONE:
		return "DONE"
	default:
		return "?"
	}
}

// --------------- Misc Graph Helper Functions ---------------

// Prints some statistics of the graph
func (g *Graph[V, E, M, N]) ComputeGraphStats() {
	maxOutDegree := make([]uint32, g.NumThreads)
	listOutDegree := make([]uint32, g.NodeVertexCount())

	maxRawId := make([]RawType, g.NumThreads)

	numEdges := uint32(0)
	numSinks := make([]uint32, g.NumThreads)
	numHeads := make([]uint32, g.NumThreads)

	// If theres no deletes then the InEventPos is exactly the in degree.
	// TODO: should track delete a counter as well in the vertex structure, as it could be useful to have exact current in degree for algorithms.
	numDels := uint32(0)
	for t := uint32(0); t < g.NumThreads; t++ {
		numDels += g.GraphThreads[t].NumOutDels
		numEdges += g.GraphThreads[t].NumEdges
	}
	maxInDegree := make([]uint32, g.NumThreads)
	listInDegree := make([]uint32, g.NodeVertexCount())

	g.NodeParallelFor(func(ordinalStart, internalId uint32, gt *GraphThread[V, E, M, N]) int {
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			vertex := &gt.Vertices[i]
			numOutEdges := uint32(len(vertex.OutEdges))
			maxOutDegree[gt.Tidx] = utils.Max(maxOutDegree[gt.Tidx], numOutEdges)
			listOutDegree[ordinalStart+i] = numOutEdges
			vs := gt.VertexStructure(i)

			maxRawId[gt.Tidx] = utils.Max(maxRawId[gt.Tidx], vs.RawId)
			if numOutEdges == 0 {
				numSinks[gt.Tidx]++
			}
			if numDels == 0 || g.Options.TimeRange { // TimeRange edges aren't real deletions, so they always count as an in-edge.
				maxInDegree[gt.Tidx] = utils.Max(maxInDegree[gt.Tidx], vs.InEventPos)
				listInDegree[ordinalStart+i] = vs.InEventPos
				if vs.InEventPos == 0 {
					numHeads[gt.Tidx]++
				}
			}
		}
		return 0
	})

	log.Info().Msg("----GraphStats----")
	log.Info().Msg("Vertices:        " + utils.V(g.NodeVertexCount()))
	log.Info().Msg("Largest raw ID:  " + utils.V(utils.MaxSlice(maxRawId)))
	log.Info().Msg("Edges:           " + utils.V(numEdges))
	log.Info().Msg("Sinks (no out):  " + utils.V(utils.Sum(numSinks)) + "\t    pct: " + utils.F("%6.3f", float64(utils.Sum(numSinks))*100.0/float64(g.NodeVertexCount())))
	log.Info().Msg("MaxOutDeg:       " + utils.V(utils.MaxSlice(maxOutDegree)))
	log.Info().Msg("MedianOutDeg:    " + utils.V(utils.Median(listOutDegree)))
	if numDels == 0 || g.Options.TimeRange {
		log.Info().Msg("Heads (no in):   " + utils.V(utils.Sum(numHeads)) + "\t    pct: " + utils.F("%6.3f", float64(utils.Sum(numHeads))*100.0/float64(g.NodeVertexCount())))
		log.Info().Msg("MaxInDeg:        " + utils.V(utils.MaxSlice(maxInDegree)))
		log.Info().Msg("MedianInDeg:     " + utils.V(utils.Median(listInDegree)))
	}
	log.Info().Msg("----EndStats----")
}

// Very slow / not optimized, but only used to view some stats if needed.
// TODO: should just keep the delete count per vertex
func (g *Graph[V, E, M, N]) InEdgesStats() {
	maxInDegree := 0
	listInDegree := make([]int, 0, g.NodeVertexCount())
	vertInEdges := make(map[uint32][]uint32, g.NodeVertexCount())

	g.NodeForEachVertex(func(_, vidx uint32, vertex *Vertex[V, E]) {
		for _, e := range vertex.OutEdges {
			target := e.Didx
			vertInEdges[target] = append(vertInEdges[target], vidx)
		}
	})
	log.Info().Msg("Computed inbound edges, calculating stats...")

	for _, v := range vertInEdges {
		maxInDegree = utils.Max(len(v), maxInDegree)
		listInDegree = append(listInDegree, len(v))
	}

	log.Info().Msg("MaxInDeg: " + utils.V(maxInDegree))
	log.Info().Msg("MedianInDeg: " + utils.V(utils.Median(listInDegree)))
}

// Debugging function to print the structure of the graph.
func (g *Graph[V, E, M, N]) PrintStructure() {
	g.NodeForEachVertex(func(_, internalId uint32, vertex *Vertex[V, E]) {
		pr := "[" + utils.V(internalId) + "] " + utils.V(g.NodeVertexRawID(internalId)) + ": "
		el := ""
		for _, e := range vertex.OutEdges {
			el += utils.V(g.NodeVertexRawID(e.Didx)) + ", "
		}
		pr += el
		log.Info().Msg(pr)
	})
}

// Debugging function to print properties of the graph.
func (g *Graph[V, E, M, N]) PrintVertexProps(prefix string) {
	g.NodeForEachVertex(func(_, internalId uint32, vertex *Vertex[V, E]) {
		prefix += utils.V(g.NodeVertexRawID(internalId)) + ":" + utils.V(vertex.Property) + ", "
	})
	log.Info().Msg(prefix)
}

// Writes the vertex properties to a file.
// TODO: use buffered writer
func (g *Graph[V, E, M, N]) WriteVertexProps(dynamic bool) {
	var resName string
	if dynamic {
		resName = "dynamic"
	} else {
		resName = "static"
	}
	filename := "results/" + g.PathlessName() + "-props-" + resName + ".txt"

	file := utils.CreateFile(filename)

	g.NodeForEachVertex(func(_, internalId uint32, vertex *Vertex[V, E]) {
		_, err := file.WriteString(utils.V(g.NodeVertexRawID(internalId)) + "," + utils.V(vertex.Property) + "\n")
		if err != nil {
			log.Panic().Err(err).Msg("Error writing to file.")
		}
	})
	file.Close()
}

// Tries to find the name of the graph, without the path or extension.
func (g *Graph[V, E, M, N]) PathlessName() (graphName string) {
	gNameMainT := strings.Split(g.Options.Name, "/")
	gNameMain := gNameMainT[len(gNameMainT)-1]
	gNameMainTD := strings.Split(gNameMain, ".")
	if len(gNameMainTD) > 1 {
		return gNameMainTD[len(gNameMainTD)-2]
	} else {
		return gNameMainTD[0]
	}
}

// For Testing. Helper to check if two graphs have the same structure.
// Note this does not check deep equality of the edges, nor the properties.
// It only checks the base structure of the graph; and if the number of edges is equal for the same vertex.
// TODO: option to check all edges match? (the issue is the order may be different)
func CheckGraphStructureEquality[V VPI[V], E EPI[E], M MVI[M], N any](g1 *Graph[V, E, M, N], g2 *Graph[V, E, M, N]) {
	if g1.NodeVertexCount() != g2.NodeVertexCount() {
		log.Panic().Msg("Graphs have different vertex counts: " + utils.V(g1.NodeVertexCount()) + " vs " + utils.V(g2.NodeVertexCount()))
	}

	g1.NodeForEachVertex(func(i, v uint32, vertex *Vertex[V, E]) {
		g1raw := g1.NodeVertexRawID(v)
		_, g2v := g2.NodeVertexFromRaw(g1raw)

		if len(vertex.OutEdges) != len(g2v.OutEdges) {
			log.Error().Msg("g1:")
			g1.PrintStructure()
			log.Error().Msg("g2:")
			g2.PrintStructure()
			log.Panic().Msg("Vertex rawId " + utils.V(g1raw) + " has different edge counts: " + utils.V(len(vertex.OutEdges)) + " vs " + utils.V(len(g2v.OutEdges)))
		}
	})
}
