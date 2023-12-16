package main

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type TsEntry struct {
	Name             time.Time
	CurrentMaxFlow   int64
	VertexCount      uint64
	EdgeCount        uint64
	Latency          time.Duration
	CurrentRuntime   time.Duration
	AlgTimeSinceLast time.Duration
}

var (
	TsDB       = make([]TsEntry, 0)
	TsFileName = ""
)

func TimeSeriesReset() {
	TsDB = TsDB[:0]
	TsFileName = "results/push-relabel-timeseries.csv"
}

func PrintTimeSeries(fileOut bool, stdOut bool) {
	if stdOut {
		println("Timeseries:")
	}

	var f *os.File
	if fileOut {
		f = utils.CreateFile(TsFileName)
		defer f.Close()
	}

	header := "Date,MaxFlow,VertexCount,EdgeCount,Latency,CurrentRuntime,AlgTimeSinceLast"
	if stdOut {
		println(header)
	}
	if fileOut {
		_, err := f.WriteString(header + "\n")
		if err != nil {
			panic(err)
		}
	}

	for _, entry := range TsDB {
		if entry.CurrentMaxFlow < 0 {
			continue
		}
		line := entry.Name.Format("2006-01-02") + "," + strconv.FormatInt(int64(entry.CurrentMaxFlow), 10) + "," +
			strconv.FormatUint(entry.VertexCount, 10) + "," + strconv.FormatUint(entry.EdgeCount, 10) + "," +
			strconv.FormatInt(entry.Latency.Milliseconds(), 10) + "," +
			strconv.FormatInt(entry.CurrentRuntime.Milliseconds(), 10) + "," + strconv.FormatInt(entry.AlgTimeSinceLast.Milliseconds(), 10)

		if stdOut {
			println(line)
		}
		if fileOut {
			f.WriteString(line + "\n")
		}
	}
}

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
					outEntry.CurrentMaxFlow = tse.GraphView.NodeVertexProperty(sinkId).Excess
				}
			}
		}

		tse.GraphView = nil
		tse.AlgWaitGroup.Done() // TODO ???

		TsDB = append(TsDB, outEntry)
		PrintTimeSeries(true, false)
	}
	PrintTimeSeries(true, true)
	wg.Done()
}
