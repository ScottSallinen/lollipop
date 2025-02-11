package main

import (
	"flag"
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
)

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	sourceInit := flag.String("i", "1", "Source init vertex (raw id).")
	graphOptions := graph.FlagsToOptions()
	graphOptions.TimeRange = true
	if graphOptions.TimestampPos == 0 {
		log.Panic().Msg("Timestamp Position must be set for this temporal algorithm.")
	}

	initMails := make(map[graph.RawType]Mail)
	initMails[graph.AsRawTypeString(*sourceInit)] = Mail{Pos: math.MaxUint32}

	g := graph.LaunchGraphExecution(new(TP), graphOptions, initMails, nil)

	if graphOptions.LogTimeseries {
		PrintTimeSeries(true, false, uint64(g.NodeVertexCount()))
	}

	if FUN_STATS {
		FunStatistics(g, math.MaxUint64, true, false)
	}
}
