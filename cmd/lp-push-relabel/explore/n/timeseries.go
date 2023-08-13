package n

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
			outEntry.CurrentMaxFlow = tse.GraphView.NodeVertex(sinkId).Property.Excess
		}

		tse.GraphView = nil
		tse.AlgWaitGroup.Done()

		TsDB = append(TsDB, outEntry)
		PrintTimeSeries(true, false)
	}
	PrintTimeSeries(true, true)
	wg.Done()
}
