package k

import (
	"sync"

	"github.com/ScottSallinen/lollipop/graph"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

func (pr *PushRelabel) OnApplyTimeSeries(entries chan graph.TimeseriesEntry[VertexProp, EdgeProp, Message, Note], wg *sync.WaitGroup) {
	var outEntry TsEntry
	for tse := range entries {
		// Skip if source or sink are not added yet
		_, source := tse.GraphView.NodeVertexFromRaw(SourceRawId)
		_, sink := tse.GraphView.NodeVertexFromRaw(SinkRawId)
		if source != nil && sink != nil {
			maxFlow := pr.GetMaxFlowValue(tse.GraphView)
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

		if source != nil && sink != nil {
			TsDB = append(TsDB, outEntry)
			if TsWriteEveryUpdate {
				PrintTimeSeries(true, false)
			}
		}
	}
	PrintTimeSeries(true, true)
	wg.Done()
}
