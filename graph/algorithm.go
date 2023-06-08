package graph

import (
	"runtime/pprof"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Basic algorithm template. Se cmd/lp-template for better descriptions.
type Algorithm[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnUpdateVertex(g *Graph[V, E, M, N], v *Vertex[V, E], n Notification[N], message M) (sent uint64)
	MessageMerge(incoming M, sidx uint32, existing *M) (newInfo bool)
	MessageRetrieve(existing *M, v *Vertex[V, E]) (outgoing M)
	OnEdgeAdd(g *Graph[V, E, M, N], v *Vertex[V, E], sidx uint32, eidxStart int, message M) (sent uint64)
	OnEdgeDel(g *Graph[V, E, M, N], v *Vertex[V, E], sidx uint32, deletedEdges []Edge[E], message M) (sent uint64)
}

type AlgorithmInitAllMessage[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	InitAllMessage(v *Vertex[V, E], internalId uint32, rawId RawType) (initialMessage M)
}

type AlgorithmBaseVertexMessage[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	BaseVertexMessage(v *Vertex[V, E], internalId uint32, rawId RawType) (baseMessage M)
}

type AlgorithmOnFinish[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnFinish(*Graph[V, E, M, N])
}

type AlgorithmOnCheckCorrectness[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnCheckCorrectness(*Graph[V, E, M, N])
}

type AlgorithmOnOracleCompare[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnOracleCompare(g *Graph[V, E, M, N], oracle *Graph[V, E, M, N])
}

type AlgorithmOnApplyTimeSeries[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnApplyTimeSeries(tse chan TimeseriesEntry[V, E, M, N], wg *sync.WaitGroup)
}

// Wrapper function that will block if the message queue is full.
// TODO: should drain our incoming queue if we are blocked... but how to make another function call without sacrificing the inlining...
func (g *Graph[V, E, M, N]) EnsureSend(want bool, success bool, n Notification[N], s uint32, pos uint64, tidx uint32) (sent uint64) {
	// Why is it this way? Because Go has this weird fetish for the number 80... and functions have to keep under that limit.
	// Dear Go, please support a pragma inline. I'm begging you.
	if want {
		if success {
			// Did you know !success would have added to the cost against inlining?
			// Yum yum look at this beautifully nested code. It's so pretty.
		} else {
			g.GraphThreads[tidx].NotificationQueue.PutSlowMP(n, pos)
		}
		return 1
	}
	return 0
}

// Send a message to a vertex. Will check for uniqueness, only desiring a real message if the target vertex has not yet been activated.
func (g *Graph[V, E, M, N]) UniqueNotification(sidx uint32, n Notification[N], vtm *VertexMessages[M], tidx uint32) (want bool, ok bool, d Notification[N], s uint32, pos uint64, t uint32) {
	if atomic.CompareAndSwapInt32(&vtm.Activity, 0, 1) { // if atomic.SwapInt32(&vtm.Activity, 1) == 0 {
		pos, ok = g.GraphThreads[tidx].NotificationQueue.PutFastMP(n)
		return true, ok, n, sidx, pos, tidx
	}
	return false, false, n, sidx, pos, tidx
}

// As opposed to a unique notification, this will increment activity and always send a notification.
func (g *Graph[V, E, M, N]) ActiveNotification(sidx uint32, n Notification[N], vtm *VertexMessages[M], tidx uint32) (want bool, ok bool, d Notification[N], s uint32, pos uint64, t uint32) {
	atomic.AddInt32(&vtm.Activity, 1)
	pos, ok = g.GraphThreads[tidx].NotificationQueue.PutFastMP(n)
	return true, ok, n, sidx, pos, tidx
}

// Run runs the algorithm on the given graph.
func Run[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], feederWg *sync.WaitGroup, useSync bool, dynamic bool) {
	if g.Options.DebugLevel >= 3 || g.Options.Profile {
		utils.MemoryStats()
	}
	if g.Options.Profile {
		file := utils.CreateFile("algorithm.pprof")
		pprof.StartCPUProfile(file)
		defer file.Close()
	}

	g.AlgTimer.Start()
	if dynamic {
		log.Info().Msg("ConvergeDynamic")
		ConvergeDynamic[EP](alg, g, feederWg)
	} else if useSync && g.Options.SyncPreviousOnly {
		log.Info().Msg("ConvergeSyncPrevOnly")
		ConvergeSyncPrevOnly(alg, g, feederWg)
	} else if useSync {
		log.Info().Msg("ConvergeSync")
		ConvergeSync(alg, g, feederWg)
	} else {
		log.Info().Msg("ConvergeAsync")
		ConvergeAsync(alg, g, feederWg)
	}

	if g.Options.Profile {
		utils.MemoryStats()
		pprof.StopCPUProfile()
	}

	algElapsed := g.AlgTimer.Elapsed()
	gElapsed := g.Watch.Elapsed()
	msgSend := uint64(0)
	for t := 0; t < int(g.NumThreads); t++ {
		msgSend += g.GraphThreads[t].MsgSend
	}

	msg := "Termination: " + utils.V(algElapsed.Milliseconds())
	if !g.Options.Dynamic {
		msg += " Total including streaming: " + utils.V(gElapsed.Milliseconds())
	}
	log.Info().Msg(msg + " Messages: " + utils.V(msgSend))
	log.Trace().Msg(", termination, " + utils.F("%.3f", algElapsed.Seconds()*1000) + ", total, " + utils.F("%.3f", gElapsed.Seconds()*1000))

	if a, ok := any(alg).(AlgorithmOnFinish[V, E, M, N]); ok {
		a.OnFinish(g)
	}

	if g.Options.CheckCorrectness {
		if a, ok := any(alg).(AlgorithmOnCheckCorrectness[V, E, M, N]); ok {
			log.Info().Msg("Checking correctness...")
			a.OnCheckCorrectness(g)
		} else {
			log.Warn().Msg("WARNING: Algorithm does not implement OnCheckCorrectness, but asked to.")
		}
	}

	if g.Options.DebugLevel >= 3 || g.Options.Profile {
		utils.MemoryStats()
	}
}

// Helper function to launch a graph execution.
func LaunchGraphExecution[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](a Algorithm[V, E, M, N], options GraphOptions, initMessages ...map[RawType]M) *Graph[V, E, M, N] {
	g := new(Graph[V, E, M, N])
	g.Options = options
	if len(initMessages) > 0 {
		g.InitMessages = initMessages[0]
	} else {
		g.InitMessages = nil
	}

	Launch[EP](a, g)
	return g
}

// Launch is the main entry point for the framework. It will manage loading the graph, initializing the framework, and running the algorithm.
func Launch[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N]) {
	g.Init()
	CheckAssumptions[EP](alg, g)
	feederWg := new(sync.WaitGroup)
	feederWg.Add(1)

	go LoadGraphStream[EP](g, feederWg)

	if !g.Options.Dynamic { // Build graph dynamically first, before running algorithm
		if g.Options.Profile {
			file := utils.CreateFile("stream.pprof")
			pprof.StartCPUProfile(file)
			defer file.Close()
		}
		g.Broadcast(BLOCK_ALG_IF_TOP)
		ConvergeDynamic[EP](new(blankAlg[V, E, M, N]), g, feederWg)
		for t := 0; t < int(g.NumThreads); t++ {
			g.TerminateVotes[t] = 0
		}
		if g.Options.Profile {
			pprof.StopCPUProfile()
		}
	}

	if g.Options.LogTimeseries {
		loggingWait := new(sync.WaitGroup)
		loggingWait.Add(1)
		entries := make(chan TimeseriesEntry[V, E, M, N]) // No buffering!
		// The LogTimeSeries is a separate thread that will pull information from the graph itself.
		go LogTimeSeries(alg, g, entries)
		// The ApplyTimeSeries is a separate thread that will actually log (to disk) the resultant data.
		if a, ok := any(alg).(AlgorithmOnApplyTimeSeries[V, E, M, N]); ok {
			go a.OnApplyTimeSeries(entries, loggingWait)
		} else {
			log.Warn().Msg("WARNING: Algorithm does not implement OnApplyTimeSeries, but asked to. This will still deep copy all of the vertex properties, and consume resources, but the logging is un-implemented.")
			loggingWait.Done()
		}

		Run[EP](alg, g, feederWg, g.Options.Sync, g.Options.Dynamic)

		close(g.LogEntryChan)
		log.Debug().Msg("Waiting for logging to finish computing and writing to disk.")
		loggingWait.Wait()
	} else {
		Run[EP](alg, g, feederWg, g.Options.Sync, g.Options.Dynamic)
	}

	if g.Options.OracleCompare { // Graph is finished.
		CompareToOracle(alg, g, false, true, false, false)
	}

	g.ComputeGraphStats()

	if g.Options.WriteVertexProps {
		g.WriteVertexProps(g.Options.Dynamic)
	}
}

// For testing. Builds a graph from a pre-defined list of topology events.
func DynamicGraphExecutionFromTestEvents[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], scs []TopologyEvent[E]) {
	g.Init()
	CheckAssumptions[EP](alg, g)

	feederWg := new(sync.WaitGroup)
	feederWg.Add(1)

	go func() {
		order := g.StreamRemitOnly()
		for _, v := range scs {
			switch EventType(v.TypeAndEventIdx & EVENT_TYPE_MASK) {
			case ADD:
				g.SendAdd(v.SrcRaw, v.DstRaw, v.EdgeProperty, order)
				if g.Options.Undirected {
					g.SendAdd(v.DstRaw, v.SrcRaw, v.EdgeProperty, order)
				}
				log.Debug().Msg("add " + utils.V(v.SrcRaw) + " " + utils.V(v.DstRaw) + " " + utils.V(v.EdgeProperty))
			case DEL:
				g.SendDel(v.SrcRaw, v.DstRaw, v.EdgeProperty, order)
				if g.Options.Undirected {
					g.SendDel(v.DstRaw, v.SrcRaw, v.EdgeProperty, order)
				}
				log.Debug().Msg("del " + utils.V(v.SrcRaw) + " " + utils.V(v.DstRaw) + " " + utils.V(v.EdgeProperty))
			}
		}

		for t := uint32(0); t < g.NumThreads; t++ {
			g.GraphThreads[t].FromEmitQueue.Close()
		}
		order.Close()
		feederWg.Done()
	}()

	Run[EP](alg, g, feederWg, false, true)
}

// Some quick checks to make sure timestamp changes are possible if it was declared to use one...
func CheckAssumptions[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N]) {
	event := TopologyEvent[E]{}

	if FAKE_TIMESTAMP || g.Options.TimestampPos != 0 {
		EP(&event.EdgeProperty).ReplaceTimestamp(123)
		if event.EdgeProperty.GetTimestamp() != 123 {
			log.Panic().Msg("Failure: we are using a timestamp strategy, but I could not replace a timestamp in an edge property.")
		}
	}
}

// --------- Blank Algorithm, for loading a stream with no algorithm. ---------
type blankAlg[V VPI[V], E EPI[E], M MVI[M], N any] struct{}

func (e *blankAlg[V, E, M, N]) OnUpdateVertex(*Graph[V, E, M, N], *Vertex[V, E], Notification[N], M) (s uint64) {
	return
}
func (e *blankAlg[V, E, M, N]) MessageMerge(M, uint32, *M) (b bool)     { return false }
func (e *blankAlg[V, E, M, N]) MessageRetrieve(*M, *Vertex[V, E]) (m M) { return }
func (e *blankAlg[V, E, M, N]) OnEdgeAdd(*Graph[V, E, M, N], *Vertex[V, E], uint32, int, M) (s uint64) {
	return
}
func (e *blankAlg[V, E, M, N]) OnEdgeDel(*Graph[V, E, M, N], *Vertex[V, E], uint32, []Edge[E], M) (s uint64) {
	return
}
