package m

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

		sourceId, sinkId := pr.GlobalRelabeling.GetSourceAndSinkInternalIds()
		source, sink := tse.GraphView.NodeVertexOrNil(sourceId), tse.GraphView.NodeVertexOrNil(sinkId)
		if source != nil && sink != nil {
			outEntry.CurrentMaxFlow = sink.Property.Excess
		}

		tse.GraphView = nil
		tse.AlgWaitGroup.Done()

		TsDB = append(TsDB, outEntry)
		PrintTimeSeries(true, false)
	}
	PrintTimeSeries(true, true)
	wg.Done()
}
