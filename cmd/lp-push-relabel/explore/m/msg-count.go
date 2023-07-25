package m

import (
	"fmt"
	"time"

	"golang.org/x/exp/constraints"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
)

type MsgCount struct {
	// 32 bytes, 1/2 of a cache line
	PositiveFlow uint64
	ZeroFlow     uint64
	NegativeFlow uint64
	Special      uint64
}

type ThreadMsgCount struct {
	Current MsgCount
	Last    MsgCount
}

var (
	ThreadMsgCounts = make([]ThreadMsgCount, graph.THREAD_MAX)
)

func ResetMsgCounts() {
	for i := range ThreadMsgCounts {
		ThreadMsgCounts[i] = ThreadMsgCount{}
	}
}

func IncrementMsgCount[N constraints.Integer](tidx uint32, flow N, special bool) {
	if special {
		ThreadMsgCounts[tidx].Current.Special++
	} else if flow > 0 {
		ThreadMsgCounts[tidx].Current.PositiveFlow++
	} else if flow == 0 {
		ThreadMsgCounts[tidx].Current.ZeroFlow++
	} else {
		ThreadMsgCounts[tidx].Current.NegativeFlow++
	}
}

func LogMsgCount() {
	totals, deltas := MsgCount{}, MsgCount{}
	for i := range ThreadMsgCounts {
		totals.PositiveFlow += ThreadMsgCounts[i].Current.PositiveFlow
		totals.NegativeFlow += ThreadMsgCounts[i].Current.NegativeFlow
		totals.ZeroFlow += ThreadMsgCounts[i].Current.ZeroFlow
		totals.Special += ThreadMsgCounts[i].Current.Special

		deltas.PositiveFlow += ThreadMsgCounts[i].Last.PositiveFlow
		deltas.NegativeFlow += ThreadMsgCounts[i].Last.NegativeFlow
		deltas.ZeroFlow += ThreadMsgCounts[i].Last.ZeroFlow
		deltas.Special += ThreadMsgCounts[i].Last.Special

		ThreadMsgCounts[i].Last = ThreadMsgCounts[i].Current
	}

	deltas.PositiveFlow = totals.PositiveFlow - deltas.PositiveFlow
	deltas.NegativeFlow = totals.NegativeFlow - deltas.NegativeFlow
	deltas.ZeroFlow = totals.ZeroFlow - deltas.ZeroFlow
	deltas.Special = totals.Special - deltas.Special

	log.Info().Msg("----------Stats----------  Total     Delta")
	log.Info().Msg(fmt.Sprintf("Positive Flow: %13d %10d", totals.PositiveFlow, deltas.PositiveFlow))
	log.Info().Msg(fmt.Sprintf("Negative Flow: %13d %10d", totals.NegativeFlow, deltas.NegativeFlow))
	log.Info().Msg(fmt.Sprintf("Zero     Flow: %13d %10d", totals.ZeroFlow, deltas.ZeroFlow))
	log.Info().Msg(fmt.Sprintf("Special:       %13d %10d", totals.Special, deltas.Special))
}

func GoLogMsgCount(exit *bool) {
	go func() {
		for !*exit {
			time.Sleep(5 * time.Second)
			LogMsgCount()
		}
	}()
}
