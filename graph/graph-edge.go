package graph

import (
	"strconv"

	"github.com/ScottSallinen/lollipop/utils"
)

const MAX_ELEMS_PER_EDGE = 5

const DEFAULT_WEIGHT = 1.0

// Edge: Basic edge structure for a graph, with a user-defined property; can be empty struct{}
// We make an unweighted graph simply have weights of 1 (default weight above).
type Edge[E any] struct {
	Didx     uint32
	Pos      uint32 // Unique marker for the in-add-event index, given by the destination. (e.g., if two, it is the second incoming edge the target vertex had seen). Note for deletions, this number does NOT decrease or change, it will continue to increase as it is an event marker -- not exactly the "in-degree" position. It is however the effective in-degree if edges were (or are) never deleted. Note: the first bit is reserved as a flag, so the max value is 2B.
	Property E
}

func (e Edge[E]) String() string {
	idx, tidx := InternalExpand(e.Didx)
	return "{Property: " + utils.V(e.Property) + ", tidx: " + utils.V(tidx) + ", idx: " + utils.V(idx) + ", Pos: " + utils.V(e.Pos) + "}"
}

// EdgeProp interface
type EPI[E any] interface {
	GetTimestamp() uint64 // If called on a non-Timestamp edge, expect 0.
	GetEndTime() uint64   // Time the edge ends ("deleted"). If not set (yet), or called on a non-TimeRange edge, expect 0.
	GetWeight() float64   // If called on a non-weighted edge, expect DEFAULT_WEIGHT.
}

// EdgePropPointer interface. "but but pointer receivers"
type EPP[E any] interface {
	ReplaceTimestamp(uint64)                               // If this is called on a non-timestamped edge, it will do nothing.
	ReplaceEndTime(uint64)                                 // If this is called on a non-TimeRange edge, it will do nothing.
	ReplaceWeight(float64)                                 // Will do nothing if no weight.
	ParseProperty(fields []string, wPos int32, tPos int32) // WARNING! the input []string is ephemeral, do not store it; note assignment of a string variable in Go is a shallow copy.
	*E
}

// TODO: Find a better way to do these for easier custom edges and edge parsing.

/* ------------------ Empty Edge ------------------ */

type EmptyEdge struct{}

func (EmptyEdge) GetTimestamp() uint64                  { return 0 }
func (EmptyEdge) GetEndTime() uint64                    { return 0 }
func (EmptyEdge) GetWeight() float64                    { return DEFAULT_WEIGHT }
func (*EmptyEdge) ReplaceTimestamp(uint64)              {}
func (*EmptyEdge) ReplaceEndTime(uint64)                {}
func (*EmptyEdge) ReplaceWeight(float64)                {}
func (*EmptyEdge) ParseProperty([]string, int32, int32) {}

/* ------------------ Weighted Edge ------------------ */

type WeightedEdge struct {
	Weight float64
}

func (WeightedEdge) GetTimestamp() uint64       { return 0 }
func (WeightedEdge) GetEndTime() uint64         { return 0 }
func (e WeightedEdge) GetWeight() float64       { return e.Weight }
func (*WeightedEdge) ReplaceTimestamp(uint64)   {}
func (*WeightedEdge) ReplaceEndTime(uint64)     {}
func (e *WeightedEdge) ReplaceWeight(w float64) { e.Weight = w }

func (e *WeightedEdge) ParseProperty(fields []string, wPos int32, _ int32) {
	if wPos >= 0 {
		e.Weight, _ = strconv.ParseFloat(fields[wPos], 32)
	}
}

/* ------------------ Timestamp Edge ------------------ */

type TimestampEdge struct {
	ts uint64
}

func (e TimestampEdge) GetTimestamp() uint64 {
	return uint64(e.ts)
}

func (TimestampEdge) GetEndTime() uint64 { return 0 }
func (TimestampEdge) GetWeight() float64 { return DEFAULT_WEIGHT }

func (e *TimestampEdge) ReplaceTimestamp(ts uint64) {
	e.ts = ts
}

func (*TimestampEdge) ReplaceEndTime(uint64) {}
func (*TimestampEdge) ReplaceWeight(float64) {}

func (e *TimestampEdge) ParseProperty(fields []string, _ int32, tPos int32) {
	if tPos >= 0 {
		e.ts, _ = strconv.ParseUint(fields[tPos], 10, 64)
	}
}

/* ------------------ Timestamp Weighted Edge ------------------ */

type TimestampWeightedEdge struct {
	ts     uint64
	Weight float64
}

func (e TimestampWeightedEdge) GetTimestamp() uint64 {
	return uint64(e.ts)
}

func (e TimestampWeightedEdge) GetEndTime() uint64 { return 0 }
func (e TimestampWeightedEdge) GetWeight() float64 { return e.Weight }

func (e *TimestampWeightedEdge) ReplaceTimestamp(ts uint64) {
	e.ts = ts
}

func (e *TimestampWeightedEdge) ReplaceWeight(w float64) {
	e.Weight = w
}

func (*TimestampWeightedEdge) ReplaceEndTime(uint64) {}

func (e *TimestampWeightedEdge) ParseProperty(fields []string, wPos int32, tPos int32) {
	if wPos >= 0 {
		e.Weight, _ = strconv.ParseFloat(fields[wPos], 32)
	}
	if tPos >= 0 {
		e.ts, _ = strconv.ParseUint(fields[tPos], 10, 64)
	}
}

/* ------------------ TimeRange Edge ------------------ */

type TimeRangeEdge struct {
	ts  uint64
	end uint64
}

func (e TimeRangeEdge) GetTimestamp() uint64 {
	return uint64(e.ts)
}

func (e TimeRangeEdge) GetEndTime() uint64 {
	return uint64(e.end)
}

func (TimeRangeEdge) GetWeight() float64 { return DEFAULT_WEIGHT }

func (e *TimeRangeEdge) ReplaceTimestamp(ts uint64) {
	e.ts = ts
}

func (e *TimeRangeEdge) ReplaceEndTime(end uint64) {
	e.end = end
}

func (*TimeRangeEdge) ReplaceWeight(float64) {}

func (e *TimeRangeEdge) ParseProperty(fields []string, _ int32, tPos int32) {
	if tPos >= 0 {
		e.ts, _ = strconv.ParseUint(fields[tPos], 10, 64)
	}
}
