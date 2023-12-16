package main

import (
	"fmt"
	"time"

	"golang.org/x/exp/constraints"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
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

type Progress struct {
	Time         time.Duration
	SourceSent   int64
	SinkReceived int64
}

type ThreadMsgCounter[N constraints.Integer] struct {
	Counters []ThreadMsgCount
	Progress []Progress
}

func (tmc *ThreadMsgCounter[N]) Reset() {
	tmc.Counters = make([]ThreadMsgCount, graph.THREAD_MAX)
	tmc.Progress = make([]Progress, 0)
}

func (tmc *ThreadMsgCounter[N]) IncrementMsgCount(tidx uint32, flow N, special bool) {
	if special {
		tmc.Counters[tidx].Current.Special++
	} else if flow > 0 {
		tmc.Counters[tidx].Current.PositiveFlow++
	} else if flow == 0 {
		tmc.Counters[tidx].Current.ZeroFlow++
	} else {
		tmc.Counters[tidx].Current.NegativeFlow++
	}
}

func (tmc *ThreadMsgCounter[N]) LogMsgCount() {
	totals, deltas := MsgCount{}, MsgCount{}
	for i := range tmc.Counters {
		totals.PositiveFlow += tmc.Counters[i].Current.PositiveFlow
		totals.NegativeFlow += tmc.Counters[i].Current.NegativeFlow
		totals.ZeroFlow += tmc.Counters[i].Current.ZeroFlow
		totals.Special += tmc.Counters[i].Current.Special

		deltas.PositiveFlow += tmc.Counters[i].Last.PositiveFlow
		deltas.NegativeFlow += tmc.Counters[i].Last.NegativeFlow
		deltas.ZeroFlow += tmc.Counters[i].Last.ZeroFlow
		deltas.Special += tmc.Counters[i].Last.Special

		tmc.Counters[i].Last = tmc.Counters[i].Current
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

func (tmc *ThreadMsgCounter[N]) LogProgress(pr *PushRelabel, g *Graph) {
	if g.AlgTimer.Elapsed() > 8760*time.Hour {
		return // Haven't started
	}
	SourceSent, SinkReceived := int64(0), int64(0)
	sourceId, sinkId := pr.SourceId.Load(), pr.SinkId.Load()
	if sourceId != EmptyValue {
		SourceSent = pr.SourceSupply - g.NodeVertexProperty(sourceId).Excess
	}
	if sinkId != EmptyValue {
		SinkReceived = g.NodeVertexProperty(sinkId).Excess
	}
	EdgesAdded := uint32(0)
	for t := 0; t < int(g.NumThreads); t++ {
		EdgesAdded += g.GraphThreads[t].NumOutAdds
	}
	log.Info().Msg("Current Progress, Time: " + utils.V(g.AlgTimer.Elapsed().Milliseconds()) +
		", SourceSent: " + utils.V(SourceSent) +
		", SinkReceived: " + utils.V(SinkReceived) +
		", EdgesAdded: " + utils.V(EdgesAdded))
	tmc.Progress = append(tmc.Progress, Progress{
		Time:         g.AlgTimer.Elapsed(),
		SourceSent:   SourceSent,
		SinkReceived: SinkReceived,
	})
}

func (tmc *ThreadMsgCounter[N]) GoLogMsgCount(pr *PushRelabel, g *Graph, exit *bool) {
	go func() {
		for !*exit {
			time.Sleep(5 * time.Second)
			tmc.LogMsgCount()
		}
	}()
	go func() {
		for !*exit {
			time.Sleep(100 * time.Millisecond)
			tmc.LogProgress(pr, g)
		}
	}()
}
