package graph

import (
	"flag"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
)

type GraphOptions struct {
	NumThreads            uint32  // Number of threads to use for parallelism.
	LoadThreads           uint32  // Number of threads to use for loading the graph.
	Undirected            bool    // Declares if the graph should be treated as undirected (e.g. for construction)
	Dynamic               bool    // Declares if attached algorithms will be treated as dynamic.
	Sync                  bool    // Declares if attached algorithms will be treated as synchronous (iterations), instead of default async.
	SyncPreviousOnly      bool    // For comparative purposes, this would set sync mode to only access state from the previous iteration (and not view any updated state from the current iteration).
	WriteVertexProps      bool    // If true, will print all vertex properties to disk at the end of the algorithm.
	CheckCorrectness      bool    // If true, will run the OnCheckCorrectness provided by the algorithm (might be slow).
	LogTimeseries         bool    // Log a timeseries of vertex properties.
	TimeseriesEdgeCount   bool    // If LogTimeseries enabled, instead uses number of added edges to determine when to log a timeseries. TimeSeriesInterval becomes number of add edge events.
	OracleCompare         bool    // Will compare to computed oracle results at the end, or if creating a timeseries, each time a timeseries is logged.
	OracleCompareSync     bool    // Compares to oracle results on every iteration, when using a synchronous strategy.
	AllowAsyncVertexProps bool    // If true, a query views vertex properties asynchronously; faster, but may be inconsistent for certain algorithms. For PageRank, this strategy works well. Requires a concurrent finish.
	NoConvergeForQuery    bool    // If true, will not finish convergence of the algorithm for the query; useful for an algorithm like PageRank.
	AlgTimeIncludeQuery   bool    // If true, will include time spent on process query in algorithm time
	ColourOutput          bool    // If true, will colour terminal stdout output. Default enabled; can be disabled not supported.
	Profile               bool    // If true, will profile the algorithm and create a pprof file.
	DebugLevel            uint32  // If non-zero, will print extra debug information. 1 for debug, 2 adds extra timing details, 3 adds extra debug behaviour.
	PollingRate           uint32  // How often to print status (in milliseconds) when dynamic graph streaming is running
	QueueMultiplier       uint32  // 2^n Multiplier for the notification queue size.
	TimeSeriesInterval    uint64  // Interval (seconds) for how often to log timeseries.
	TargetRate            float64 // Target rate of events (in events per second). 0 is unbounded.
	InsertDeleteOnExpire  uint64  // If non-zero, will insert deletion of edges that were added before, after passing the expiration duration. (Create a sliding window graph). Needs (Get/Set)Timestamp defined.
	AsyncContinuationTime int64   // If non-zero, will continue the algorithm for AsyncContinuationTime milliseconds before collecting a state (logging a timeseries).
	TimestampPos          int32   // Logical (not zero-indexed) position after [src, dst]. Value 0 means no timestamp in event file or not desired.
	WeightPos             int32   // Logical (not zero-indexed) position after [src, dst]. Value 0 means no weight in event file or not desired.
	Name                  string  // Name of the input graph.
}

// Declare your own flags before you call this function.
func FlagsToOptions() (graphOptions GraphOptions) {
	graphPtr := flag.String("g", "", "Graph file.")

	mqPtr := flag.Int("m", 8, "Multiplier for the notification queue size. 2^n. Default 8, for 256; should cover most graphs. May want to step down for smaller graphs.")

	syncPtr := flag.Bool("s", false, "Use an emulated sync mode, instead of the default async.")
	syncPrevPtr := flag.Bool("sprev", false, "Use an emulated sync mode, but only allow access to the previous iteration's state. (Sets sync.)")
	dynamicPtr := flag.Bool("d", false, "Dynamic.")

	dEdgePtr := flag.Int("de", 0, "Log timeseries data, by edge count change. Provide the delta edges between attempted queries. (Sets dynamic.)")
	dTimePtr := flag.Int("dt", 0, "Log timeseries data, by querying against the given timestamp interval in days. 0 is disabled. (Sets dynamic.)")
	dRatePtr := flag.Float64("dr", 0, "Set a target dynamic rate, with given rate in Edge Per Second. 0 is unbounded. (Sets dynamic.)")
	timePosPtr := flag.Int("pt", 0, "Logical position of timestamp after [src, dst]. \nExample: [src, dst, timestamp], use 1. \nExample: [src, dst, weight, timestamp], use 2. \nValue 0 means no timestamp in events, or not desired.")
	weightPosPtr := flag.Int("pw", 0, "Logical position of weight after [src, dst]. \nExample: [src, dst, weight, timestamp], use 1. \nValue 0 means no weight in events, or not desired.")
	windowPtr := flag.Int("w", 0, "Inject to the stream: deletion of edges that are w days behind the current timestamp (ensure pt is set).")
	pollPtr := flag.Uint("poll", 500, "Polling rate (ms), how often to print status when graph streaming is running.")
	refinePtr := flag.Int("refine", 0, "When collecting a query, wait to refine by this amount (ms). Only used if logging timeseries.")

	undirectedPtr := flag.Bool("u", false, "Interpret the input graph as undirected (add transpose edges as a mirrored event). \nThis is not optimized -- and some algorithms set this by default.")

	oraclePtr := flag.Bool("o", false, "Compare to oracle results (computed via async) upon finishing the algorithm. \nIf timeseries enabled, will run on each logging of data.\n TODO: timers become inaccurate?")
	oracleSyncPtr := flag.Bool("osync", false, "Compare to oracle results for each sync iteration.")
	checkPtr := flag.Bool("c", false, "Check correctness after execution.")
	propPtr := flag.Bool("p", false, "Save vertex properties to disk at the end. Not optimized (warning: old code).")

	profilePtr := flag.Bool("profile", false, "Profile the stream and algorithm, print memory stats, and creates pprof files.")
	pprofPtr := flag.String("pprof", "", "If set, will serve pprof on the given address:port. E.g.\"0.0.0.0:6060\".")
	debugPtr := flag.Int("debug", 0, "Adds extra debug output. Level 0 for info, 1 for debug, 2 adds extra timing details, 3 adds extra debug behaviour.")
	colourPtr := flag.Bool("nc", false, "Removes the colouring from the log output.")

	threadPtr := flag.Int("t", runtime.NumCPU(), "Thread count for the algorithm. For dynamic with many queries, suggest one per real CPU, and leave hyperthreads free to handle queries.")
	threadLoadPtr := flag.Int("tg", 2, "Workers for edge parsing. Note there is always one thread that emits events in sequential order; if set to 1, a single thread handles parsing and emitting. Some tuning/testing needed...")
	flag.Parse()

	if *colourPtr {
		utils.SetLoggerConsole(true)
	}

	utils.SetLevel(*debugPtr)

	if *graphPtr == "" {
		log.Info().Msg("Note: not all options may work for all algorithms.")
		flag.Usage()
		os.Exit(1)
	}
	// Dynamic if any of these are set.
	dynamic := *dynamicPtr || *dRatePtr > 0 || *dEdgePtr > 0 || *dTimePtr > 0
	// Sync if any of these are set.
	useSync := *syncPtr || *syncPrevPtr
	if useSync && dynamic {
		log.Panic().Msg("Cannot use sync mode with dynamic mode.")
	}

	tsInterval := (24 * 60 * 60) * uint64(*dTimePtr)
	if *dEdgePtr > 0 {
		tsInterval = uint64(*dEdgePtr)
	}
	deleteOnExpire := (24 * 60 * 60) * uint64(*windowPtr)

	if *pprofPtr != "" {
		go func() {
			log.Info().Msg("pprof Starting on " + *pprofPtr)
			err := http.ListenAndServe(*pprofPtr, nil)
			if err != nil {
				log.Error().Err(err).Msg("pprof Failed to start.")
			}
		}()
	}

	threadCount := *threadPtr
	if threadCount <= 0 {
		log.Panic().Msg("Invalid thread count.")
	} else if threadCount > runtime.NumCPU() {
		log.Warn().Msg("Thread count is greater than CPU count?")
	}

	if threadCount == runtime.NumCPU() && (tsInterval > 0) {
		log.Warn().Msg("WARNING: Using all threads for dynamic while also logging timeseries. This may cause performance issues. Suggest using half nproc for work threads, saving hyperthreads for logging/queries. Pausing for a few seconds...")
		time.Sleep(5 * time.Second)
	}

	loadThreads := *threadLoadPtr
	if loadThreads <= 0 {
		log.Panic().Msg("Invalid load thread count.")
	} else if loadThreads > runtime.NumCPU() {
		log.Warn().Msg("Load thread count is greater than CPU count?")
	}

	graphOptions = GraphOptions{
		Name:                  *graphPtr,
		NumThreads:            uint32(threadCount),
		Dynamic:               dynamic,
		Sync:                  useSync,
		QueueMultiplier:       uint32(*mqPtr),
		Undirected:            *undirectedPtr,
		WriteVertexProps:      *propPtr,
		TargetRate:            *dRatePtr,
		CheckCorrectness:      *checkPtr,
		DebugLevel:            uint32(*debugPtr),
		LogTimeseries:         (tsInterval > 0),
		TimeseriesEdgeCount:   (*dEdgePtr > 0),
		TimeSeriesInterval:    tsInterval,
		InsertDeleteOnExpire:  deleteOnExpire,
		AsyncContinuationTime: int64(*refinePtr),
		OracleCompare:         *oraclePtr,
		SyncPreviousOnly:      *syncPrevPtr,
		OracleCompareSync:     *oracleSyncPtr,
		PollingRate:           uint32(*pollPtr),
		Profile:               *profilePtr,
		LoadThreads:           uint32(loadThreads),
		TimestampPos:          int32(*timePosPtr),
		WeightPos:             int32(*weightPosPtr),
	}
	return graphOptions
}
