package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
)

func Assert(cond bool, msg string) {
	if !cond {
		log.Panic().Msg(msg)
	}
}

func AssertC(cond bool) {
	if !cond {
		log.Panic().Msg("Something is wrong. Please see the stack trace.")
	}
}

type VertexType uint8

const (
	EmptyValue = math.MaxUint32
	MaxHeight  = math.MaxInt32 / 2

	Normal VertexType = 0
	Source VertexType = 1
	Sink   VertexType = 2
)

// Set these flags before running the algorithm
var (
	GlobalRelabelingEnabled = true

	SourceRawId = graph.AsRawType(-1)
	SinkRawId   = graph.AsRawType(-1)
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