package m

import (
	"sync"

	"github.com/ScottSallinen/lollipop/graph"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

func (pr *PushRelabel) OnApplyTimeSeries(entries chan graph.TimeseriesEntry[VertexProp, EdgeProp, Mail, Note], wg *sync.WaitGroup) {
	var outEntry TsEntry
	for tse := range entries {
		sourceId, sinkId := pr.GlobalRelabeling.GetSourceAndSinkInternalIds()
		source, sink := tse.GraphView.NodeVertexOrNil(sourceId), tse.GraphView.NodeVertexOrNil(sinkId)
		skip := source == nil || sink == nil

		if !skip {
			maxFlow := sink.Property.Excess
			outEntry = TsEntry{
				Name:             tse.Name,
				CurrentMaxFlow:   maxFlow,
				VertexCount:      uint64(tse.GraphView.NodeVertexCount()),
				EdgeCount:        tse.EdgeCount,
				Latency:          tse.Latency,
				CurrentRuntime:   tse.CurrentRuntime,
				AlgTimeSinceLast: tse.AlgTimeSinceLast,
			}
		}

		tse.GraphView = nil
		tse.AlgWaitGroup.Done()

		if !skip {
			TsDB = append(TsDB, outEntry)
			if TsWriteEveryUpdate {
				PrintTimeSeries(true, false)
			}
		}
	}
	PrintTimeSeries(true, true)
	wg.Done()
}
