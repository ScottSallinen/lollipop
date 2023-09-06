package o

import (
	"sync"

	"github.com/ScottSallinen/lollipop/graph"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

func (pr *PushRelabel) OnApplyTimeSeries(entries chan graph.TimeseriesEntry[VertexProp, EdgeProp, Mail, Note], wg *sync.WaitGroup) {
	var outEntry TsEntry
	for tse := range entries {
		outEntry = TsEntry{
			Name:             tse.Name,
			CurrentMaxFlow:   -1, // -1 indicates source or sink does not exist
			VertexCount:      uint64(tse.GraphView.NodeVertexCount()),
			EdgeCount:        tse.EdgeCount,
			Latency:          tse.Latency,
			CurrentRuntime:   tse.CurrentRuntime,
			AlgTimeSinceLast: tse.AlgTimeSinceLast,
		}

		sourceId, sinkId := pr.SourceId.Load(), pr.SinkId.Load()
		if sourceId != EmptyValue && sinkId != EmptyValue {
			source, sink := tse.GraphView.NodeVertexOrNil(sourceId), tse.GraphView.NodeVertexOrNil(sinkId)
			if source != nil && sink != nil { // sink might not be captured in this snapshot
				sourceEvent := tse.GraphView.NodeVertexStructure(sourceId).CreateEvent
				sinkEvent := tse.GraphView.NodeVertexStructure(sinkId).CreateEvent
				if sourceEvent <= tse.AtEventIndex && sinkEvent <= tse.AtEventIndex { // Make sure they are created before this snapshot is captured
					outEntry.CurrentMaxFlow = sink.Property.Excess
				}
			}
		}

		tse.GraphView = nil
		tse.AlgWaitGroup.Done()

		TsDB = append(TsDB, outEntry)
		PrintTimeSeries(true, false)
	}
	PrintTimeSeries(true, true)
	wg.Done()
}
