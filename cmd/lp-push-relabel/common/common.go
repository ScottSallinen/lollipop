package common

import (
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
	"math"
)

func Assert(cond bool, msg string) {
	if !cond {
		log.Panic().Msg(msg)
	}
}

type VertexType uint8

const (
	EmptyValue = math.MaxUint32
	MaxHeight  = math.MaxUint32

	Normal VertexType = 0
	Source VertexType = 1
	Sink   VertexType = 2
)

// Set these flags before running the algorithm
var (
	InitialHeight = int64(0) // The initial height of a vertex that is newly added to the graph.
	SourceRawId   = graph.AsRawType(-1)
	SinkRawId     = graph.AsRawType(-1)
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
		log.Panic().Msg("Unknown VertexType: " + string(t))
		return string(t)
	}
}
