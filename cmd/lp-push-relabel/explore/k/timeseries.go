package k

import (
	"sync"

	"github.com/ScottSallinen/lollipop/graph"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

func (pr *PushRelabel) OnApplyTimeSeries(entries chan graph.TimeseriesEntry[VertexProp, EdgeProp, Mail, Note], wg *sync.WaitGroup) {
	var outEntry TsEntry
	for tse := range entries {
		vertexCount := tse.GraphView.NodeVertexCount()
		_, source := tse.GraphView.NodeVertexFromRaw(SourceRawId)
		_, sink := tse.GraphView.NodeVertexFromRaw(SinkRawId)
		skip := source == nil || sink == nil
		if !skip {
			maxFlow := pr.GetMaxFlowValue(tse.GraphView)
			outEntry = TsEntry{
				Name:             tse.Name,
				CurrentMaxFlow:   maxFlow,
				VertexCount:      uint64(vertexCount),
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

		GlobalRelabelingHelper.UpdateInterval(int64(vertexCount), int64(tse.EdgeCount))
	}
	PrintTimeSeries(true, true)
	wg.Done()
}
