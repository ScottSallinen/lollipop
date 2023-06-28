package main

import (
	"flag"
	"fmt"
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
)

func assert(cond bool, msg string) {
	if !cond {
		log.Panic().Msg(msg)
	}
}

type VertexType uint8

const (
	EmptyValue = 0
	MaxHeight  = math.MaxUint32

	Normal VertexType = 0
	Source VertexType = 1
	Sink   VertexType = 2
)

// Set these flags before running the algorithm
var (
	initialHeight = int64(0) // The initial height of a vertex that is newly added to the graph.
	sourceRawId   = graph.AsRawType(-1)
	sinkRawId     = graph.AsRawType(-1)
)

func (t VertexType) String() string {
	switch t {
	case Normal:
		return "Normal"
	case Source:
		return "Source"
	case Sink:
		return "Sink"
	default:
		return fmt.Sprintf("%d", t)
	}
}

func main() {
	sourceId := flag.Int("S", -1, "Source vertex (raw id).")
	sinkId := flag.Int("T", -1, "Sink vertex (raw id).")
	initialEstimatedCount := flag.Uint("V", 30, "Initial estimated number of vertices.")
	implementation := flag.String("I", "agg", "Implementation. Can be aggregation (agg) or direct message passing (msg)")
	graphOptions := graph.FlagsToOptions()

	initialHeight = 0
	sourceRawId = graph.RawType(*sourceId)
	sinkRawId = graph.RawType(*sinkId)
	VertexCountHelper.Reset(int64(*initialEstimatedCount))

	switch *implementation {
	case "agg":
		graph.LaunchGraphExecution[*EPropAgg, VPropAgg, EPropAgg, MessageAgg, NoteAgg](new(PushRelabelAgg), graphOptions)
	case "msg":
		graph.LaunchGraphExecution[*EPropMsg, VPropMsg, EPropMsg, MessageMsg, NoteMsg](new(PushRelabelMsg), graphOptions)
	}
}
