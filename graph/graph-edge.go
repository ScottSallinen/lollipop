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
	Property E // For some reason, it has to be first to avoid alignment issues with empty struct{} ? https://stackoverflow.com/questions/77225870/why-empty-struct-use-no-memory-and-why-empty-struct-will-use-memory-when-it-as
	Didx     uint32
	Pos      uint32 // Unique marker for the in-add-event index, given by the destination. (e.g., if two, it is the second incoming edge the target vertex had seen). Note for deletions, this number does NOT decrease or change, it will continue to increase as it is an event marker -- not exactly the "in-degree" position. It is however the effective in-degree if edges were (or are) never deleted. Note: the first bit is reserved as a flag, so the max value is 2B.
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
	GetRaw() RawType
}

// EdgePropPointer interface. "but but pointer receivers"
type EPP[E any] interface {
	ReplaceTimestamp(uint64)                               // If this is called on a non-timestamped edge, it will do nothing.
	ReplaceEndTime(uint64)                                 // If this is called on a non-TimeRange edge, it will do nothing.
	ReplaceWeight(float64)                                 // Will do nothing if no weight.
	ReplaceRaw(RawType)                                    // Will do nothing if not storing raw.
	ParseProperty(fields []string, wPos int32, tPos int32) // WARNING! the input []string is ephemeral, do not store it; note assignment of a string variable in Go is a shallow copy.
	*E
}

// TODO: Find a better way to do these for easier custom edges and edge parsing. (Is there a way to combine parsing and keep inlining?)

type NoParse struct{} // No additional parsing: no timestamp, no weight, no raw.

func (NoParse) ParseProperty([]string, int32, int32) {}

/* ------------------ Empty Edge ------------------ */

type EmptyEdge struct {
	NoParse
	NoTimestamp
	NoWeight
	NoRaw
}

/* ------------------ Edge Weight ------------------ */

type WithWeight struct {
	Weight float64
}

func (e WithWeight) GetWeight() float64       { return e.Weight }
func (e *WithWeight) ReplaceWeight(w float64) { e.Weight = w }

func (e *WithWeight) ParseProperty(fields []string, wPos int32, _ int32) {
	if wPos >= 0 {
		e.Weight, _ = strconv.ParseFloat(fields[wPos], 32)
	}
}

type NoWeight struct{}

func (NoWeight) GetWeight() float64     { return DEFAULT_WEIGHT }
func (*NoWeight) ReplaceWeight(float64) {}

/* ------------------ Edge Timestamp ------------------ */

type WithTimestamp struct {
	Ts uint64
}

func (e WithTimestamp) GetTimestamp() uint64 {
	return uint64(e.Ts)
}

func (WithTimestamp) GetEndTime() uint64 { return 0 }

func (e *WithTimestamp) ReplaceTimestamp(ts uint64) {
	e.Ts = ts
}

func (*WithTimestamp) ReplaceEndTime(uint64) {}

func (e *WithTimestamp) ParseProperty(fields []string, _ int32, tPos int32) {
	if tPos >= 0 {
		e.Ts, _ = strconv.ParseUint(fields[tPos], 10, 64)
	}
}

type NoTimestamp struct{}

func (NoTimestamp) GetTimestamp() uint64     { return 0 }
func (NoTimestamp) GetEndTime() uint64       { return 0 }
func (*NoTimestamp) ReplaceTimestamp(uint64) {}
func (*NoTimestamp) ReplaceEndTime(uint64)   {}

/* ------------------ Edge Raw ------------------ */

type WithRaw struct {
	Raw RawType
}

func (e WithRaw) GetRaw() RawType       { return e.Raw }
func (e *WithRaw) ReplaceRaw(r RawType) { e.Raw = r }

type NoRaw struct{}

func (NoRaw) GetRaw() (r RawType)   { return r }
func (*NoRaw) ReplaceRaw(r RawType) {}

/* ------------------ Timestamp Weighted Edge ------------------ */

type TimestampWeightedEdge struct {
	WithTimestamp
	WithWeight
	NoRaw
}

/* ------------------ TimeRange Edge ------------------ */

type TimeRangeEdge struct {
	Ts  uint64
	End uint64
}

func (e TimeRangeEdge) GetTimestamp() uint64 {
	return uint64(e.Ts)
}

func (e TimeRangeEdge) GetEndTime() uint64 {
	return uint64(e.End)
}

func (TimeRangeEdge) GetWeight() float64 { return DEFAULT_WEIGHT }

func (e *TimeRangeEdge) ReplaceTimestamp(ts uint64) {
	e.Ts = ts
}

func (e *TimeRangeEdge) ReplaceEndTime(end uint64) {
	e.End = end
}

func (*TimeRangeEdge) ReplaceWeight(float64) {}

func (e *TimeRangeEdge) ParseProperty(fields []string, _ int32, tPos int32) {
	if tPos >= 0 {
		e.Ts, _ = strconv.ParseUint(fields[tPos], 10, 64)
	}
}
