package graph

import (
	"math"
	"runtime/pprof"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Basic algorithm template. Se cmd/lp-template for better descriptions.
type Algorithm[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnUpdateVertex(g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], v *Vertex[V, E], vp *V, n Notification[N], mail M) (sent uint64)
	MailMerge(incoming M, sidx uint32, existing *M) (newInfo bool)
	MailRetrieve(existing *M, v *Vertex[V, E], vp *V) (outgoing M)
	OnEdgeAdd(g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], v *Vertex[V, E], vp *V, sidx uint32, eidxStart int, mail M) (sent uint64)
	OnEdgeDel(g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], v *Vertex[V, E], vp *V, sidx uint32, deletedEdges []Edge[E], mail M) (sent uint64)
}

type AlgorithmInitAllMail[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	InitAllMail(v *Vertex[V, E], vp *V, internalId uint32, rawId RawType) (initialMail M)
}

type AlgorithmInitAllNote[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	InitAllNote(v *Vertex[V, E], vp *V, internalId uint32, rawId RawType) (initialNote N)
}

type AlgorithmBaseVertexMailbox[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	BaseVertexMailbox(v *Vertex[V, E], vp *V, internalId uint32, s *VertexStructure) (baseMail M)
}

type AlgorithmOnInEdgeAdd[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnInEdgeAdd(g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], v *Vertex[V, E], vp *V, vidx uint32, pos uint32, event *InputEvent[E])
}

type AlgorithmOnFinish[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnFinish(*Graph[V, E, M, N], *Graph[V, E, M, N], uint64)
}

type AlgorithmOnSuperStepConverged[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnSuperStepConverged(*Graph[V, E, M, N]) (sent uint64)
}

type AlgorithmOnCheckCorrectness[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnCheckCorrectness(*Graph[V, E, M, N])
}

type AlgorithmOnOracleCompare[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnOracleCompare(g *Graph[V, E, M, N], oracle *Graph[V, E, M, N])
}

type AlgorithmOnApplyTimeSeries[V VPI[V], E EPI[E], M MVI[M], N any] interface {
	OnApplyTimeSeries(tse TimeseriesEntry[V, E, M, N])
}

// Wrapper function that will block if the notification queue is full.
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

// Send a notification to a vertex. Will check for uniqueness, only emplacing if the target vertex has not yet been activated.
func (g *Graph[V, E, M, N]) UniqueNotification(sidx uint32, n Notification[N], mailbox *VertexMailbox[M], tidx uint32) (want bool, ok bool, d Notification[N], s uint32, pos uint64, t uint32) {
	if atomic.CompareAndSwapInt32(&mailbox.Activity, 0, 1) {
		pos, ok = g.GraphThreads[tidx].NotificationQueue.PutFastMP(n)
		return true, ok, n, sidx, pos, tidx
	}
	return false, false, n, sidx, pos, tidx
}

// As opposed to a unique notification, this will increment activity and always send a notification.
// This us useful for a "pure-message-passing" strategy.
func (g *Graph[V, E, M, N]) ActiveNotification(sidx uint32, n Notification[N], mailbox *VertexMailbox[M], tidx uint32) (want bool, ok bool, d Notification[N], s uint32, pos uint64, t uint32) {
	atomic.AddInt32(&mailbox.Activity, 1)
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

	algElapsed := g.AlgTimer.Pause()
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
		a.OnFinish(g, g, math.MaxUint64)
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
func LaunchGraphExecution[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](a Algorithm[V, E, M, N], options GraphOptions, initMails map[RawType]M, initNotes map[RawType]N) *Graph[V, E, M, N] {
	g := new(Graph[V, E, M, N])
	g.Options = options
	if len(initMails) > 0 {
		g.InitMails = initMails
	} else if len(initNotes) > 0 {
		g.InitNotes = initNotes
	}

	Launch[EP](a, g)
	return g
}

// Launch is the main entry point for the framework. It will manage loading the graph, initializing the framework, and running the algorithm.
func Launch[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N]) {
	g.Init()
	CheckAssumptions[EP](alg, g)
	g.SourceInit = (g.InitMails != nil || g.InitNotes != nil)
	g.NoteInit = (g.InitNotes != nil)
	if _, ok := any(alg).(AlgorithmInitAllNote[V, E, M, N]); ok {
		g.NoteInit = true
	}

	feederWg := new(sync.WaitGroup)
	feederWg.Add(1)

	go LoadFileGraphStream[EP](g, feederWg)

	if !g.Options.Dynamic { // Build graph dynamically first, before running algorithm
		if g.Options.Profile {
			file := utils.CreateFile("stream.pprof")
			pprof.StartCPUProfile(file)
			defer file.Close()
		}
		g.Broadcast(BLOCK_ALG_IF_EVENTS)
		blank := new(blankAlg[V, E, M, N])
		if aIN, ok := any(alg).(AlgorithmOnInEdgeAdd[V, E, M, N]); ok {
			blank.OnInEdgeAddFunc = aIN.OnInEdgeAdd
		}
		ConvergeDynamic[EP](blank, g, feederWg)
		g.ResetTerminationState()
		if g.Options.Profile {
			pprof.StopCPUProfile()
		}
	}

	if g.Options.LogTimeseries {
		// The LogTimeSeries is a separate thread that will pull information from the graph itself.
		go LogTimeSeries(alg, g)
		if _, ok := any(alg).(AlgorithmOnApplyTimeSeries[V, E, M, N]); !ok {
			log.Warn().Msg("WARNING: Algorithm does not implement OnApplyTimeSeries, but asked to. Timeseries data logging is un-implemented.")
		}
	}

	Run[EP](alg, g, feederWg, g.Options.Sync, g.Options.Dynamic)

	close(g.LogEntryChan)

	g.ComputeGraphStats()

	if g.Options.OracleCompare { // Graph is finished.
		CompareToOracle(alg, g, false, true, false, false)
	}

	if g.Options.WriteVertexProps {
		g.WriteVertexProps(g.Options.Dynamic)
	}
}

// For testing. Builds a graph from a pre-defined list of topology events.
func DynamicGraphExecutionFromTestEvents[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], scs []InputEvent[E]) {
	g.Init()
	CheckAssumptions[EP](alg, g)
	g.SourceInit = (g.InitMails != nil || g.InitNotes != nil)
	g.NoteInit = (g.InitNotes != nil)
	if _, ok := any(alg).(AlgorithmInitAllNote[V, E, M, N]); ok {
		g.NoteInit = true
	}

	feederWg := new(sync.WaitGroup)
	feederWg.Add(1)

	go func() {
		stream := TestGraphStream[EP](g)
		event := 0
		for _, v := range scs {
			v.TypeAndEventIdx |= (uint64(event) << EVENT_TYPE_BITS)
			switch EventType(v.TypeAndEventIdx & EVENT_TYPE_MASK) {
			case ADD:
				g.SendAdd(v.SrcRaw, v.DstRaw, v.EdgeProperty, stream)
				log.Debug().Msg("add " + utils.V(v.SrcRaw) + " " + utils.V(v.DstRaw) + " " + utils.V(v.EdgeProperty))
			case DEL:
				g.SendDel(v.SrcRaw, v.DstRaw, v.EdgeProperty, stream)
				log.Debug().Msg("del " + utils.V(v.SrcRaw) + " " + utils.V(v.DstRaw) + " " + utils.V(v.EdgeProperty))
			}
		}

		stream.Close()
		feederWg.Done()
	}()

	Run[EP](alg, g, feederWg, false, true)
}

// Some quick checks to make sure timestamp changes are possible if it was declared to use one...
func CheckAssumptions[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N]) {
	event := InputEvent[E]{}

	if g.Options.LogicalTime || g.Options.TimestampPos != 0 {
		EP(&event.EdgeProperty).ReplaceTimestamp(123)
		if event.EdgeProperty.GetTimestamp() != 123 {
			log.Panic().Msg("Failure: we are using a timestamp strategy, but I could not replace a timestamp in an edge property.")
		}
	}
	if g.Options.TimeRange {
		EP(&event.EdgeProperty).ReplaceEndTime(1234)
		if event.EdgeProperty.GetEndTime() != 1234 {
			log.Panic().Msg("Failure: we are using a time range strategy, but I could not replace an end time in an edge property.")
		}
	}
}

// --------- Blank Algorithm, for loading a stream with no algorithm. ---------
type blankAlg[V VPI[V], E EPI[E], M MVI[M], N any] struct {
	OnInEdgeAddFunc func(g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], v *Vertex[V, E], vp *V, vidx uint32, pos uint32, event *InputEvent[E])
}

func (e *blankAlg[V, E, M, N]) OnUpdateVertex(*Graph[V, E, M, N], *GraphThread[V, E, M, N], *Vertex[V, E], *V, Notification[N], M) (s uint64) {
	return
}
func (e *blankAlg[V, E, M, N]) MailMerge(M, uint32, *M) (b bool)         { return false }
func (e *blankAlg[V, E, M, N]) MailRetrieve(*M, *Vertex[V, E], *V) (m M) { return }
func (e *blankAlg[V, E, M, N]) OnInEdgeAdd(g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], v *Vertex[V, E], vp *V, vidx uint32, pos uint32, event *InputEvent[E]) {
	if e.OnInEdgeAddFunc != nil {
		e.OnInEdgeAddFunc(g, gt, v, vp, vidx, pos, event)
	}
}
func (e *blankAlg[V, E, M, N]) OnEdgeAdd(*Graph[V, E, M, N], *GraphThread[V, E, M, N], *Vertex[V, E], *V, uint32, int, M) (s uint64) {
	return
}
func (e *blankAlg[V, E, M, N]) OnEdgeDel(*Graph[V, E, M, N], *GraphThread[V, E, M, N], *Vertex[V, E], *V, uint32, []Edge[E], M) (s uint64) {
	return
}
