package main

import (
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
)

type NamedEntry struct {
	Name                          time.Time
	VertexCount                   uint64
	EdgeCount                     uint64
	EdgeDeletes                   uint64
	Latency                       time.Duration
	CurrentRuntime                time.Duration
	AlgTimeSinceLast              time.Duration
	WZMin                         uint64
	WZMedian                      uint64
	WZMax                         uint64
	MaxWC                         uint64
	PctVisitedNow                 float64
	NumVisited                    int
	WZPctCurrVCould               float64
	ActiveVertices                uint64
	PctOfActiveIsCurrentVisitable float64
}

const WRITE_EVERY_UPDATE = false // Writes the timeseries file every single time it updates (instead of just at the end).

const FUN_STATS = true // Seriously not optimized, but fun to look at.

var tsDB = make([]NamedEntry, 0)

func (*TP) OnApplyTimeSeries(tse graph.TimeseriesEntry[VertexProperty, EdgeProperty, Mail, Note]) {
	outEntry := NamedEntry{Name: tse.Name, VertexCount: uint64(tse.GraphView.NodeVertexCount()),
		EdgeCount: tse.EdgeCount, EdgeDeletes: tse.EdgeDeletes,
		Latency: tse.Latency, CurrentRuntime: tse.CurrentRuntime, AlgTimeSinceLast: tse.AlgTimeSinceLast}
	if FUN_STATS {
		outEntry.WZMin, outEntry.WZMedian, outEntry.WZMax, outEntry.MaxWC, outEntry.PctVisitedNow, outEntry.NumVisited, outEntry.WZPctCurrVCould, outEntry.VertexCount, outEntry.ActiveVertices, outEntry.PctOfActiveIsCurrentVisitable = FunStatistics(tse.GraphView, tse.AtEventIndex, false, false)
	}
	tse.GraphView = nil
	tsDB = append(tsDB, outEntry)
	if WRITE_EVERY_UPDATE {
		PrintTimeSeries(true, false, 0)
	}
}

func PrintTimeSeries(fileOut bool, stdOut bool, finalVertexCount uint64) {
	if stdOut {
		println("Timeseries:")
	}
	var f *os.File
	if fileOut {
		f = utils.CreateFile("results/temporal-paths-timeseries.csv")
		defer f.Close()
	}

	header := "ts,date,cumRuntimeMS,algTimeSinceLastMS,qLatencyMS,av,v,ae,de,te"
	if FUN_STATS {
		header = header + ",tMin,,MaxReach,MedianReach,PctOfCouldStillCan,PctAllNow,PctEdgeActive,PctOfFinalCould,PctOfActiveIsCurrentVisitable,maxWC"
	}
	if stdOut {
		println(header)
	}
	if fileOut {
		_, err := f.WriteString(header + "\n")
		if err != nil {
			panic(err)
		}
	}

	for i := range tsDB {
		// Tweaked for better excel importing...
		line := tsDB[i].Name.Format(time.RFC3339) + "," + tsDB[i].Name.Format("2006-01-02") + ","
		line = line + strconv.FormatInt(tsDB[i].CurrentRuntime.Milliseconds(), 10) + "," + strconv.FormatInt(tsDB[i].AlgTimeSinceLast.Milliseconds(), 10) + "," + strconv.FormatInt(tsDB[i].Latency.Milliseconds(), 10) + ","
		line = line + strconv.FormatUint(tsDB[i].ActiveVertices, 10) + "," + strconv.FormatUint(tsDB[i].VertexCount, 10) + "," + strconv.FormatUint(tsDB[i].EdgeCount, 10) + "," + strconv.FormatUint(tsDB[i].EdgeDeletes, 10) + "," + strconv.FormatUint(tsDB[i].EdgeDeletes+tsDB[i].EdgeCount, 10)
		if FUN_STATS {
			line = line +
				"," + time.Unix(int64(tsDB[i].WZMin), 0).Format("2006-01-02") +
				"," + tsDB[i].Name.Format("2006-01-02") +
				"," + time.Unix(int64(tsDB[i].WZMax), 0).Format("2006-01-02") +
				"," + time.Unix(int64(tsDB[i].WZMedian), 0).Format("2006-01-02") +
				"," + strconv.FormatFloat(tsDB[i].PctVisitedNow, 'f', 2, 64) +
				"," + strconv.FormatFloat(tsDB[i].WZPctCurrVCould, 'f', 2, 64) +
				"," + strconv.FormatFloat((float64(tsDB[i].EdgeCount)*100.0)/float64(tsDB[i].EdgeDeletes+tsDB[i].EdgeCount), 'f', 2, 64) +
				"," + strconv.FormatFloat((float64(tsDB[i].NumVisited)*100.0)/float64(finalVertexCount), 'f', 2, 64) +
				"," + strconv.FormatFloat(tsDB[i].PctOfActiveIsCurrentVisitable, 'f', 2, 64) +
				"," + strconv.FormatUint(tsDB[i].MaxWC, 10)
		}

		if stdOut {
			println(line)
		}
		if fileOut {
			f.WriteString(line + "\n")
		}
	}
}

func FunStatistics(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], atEventIndex uint64, print bool, printAll bool) (wZMin uint64, wZMedian uint64, wZMax uint64, maxWC uint64, pctVisitedNow float64, visited int, wZPctCurrVCould float64, realVertexCount uint64, activeVertices uint64, pctOfActiveIsCurrentVisitable float64) {
	maxWindows := make([]uint64, g.NumThreads)
	allWindows := make([][]int, g.NumThreads)
	for t := uint32(0); t < g.NumThreads; t++ {
		allWindows[t] = make([]int, 0, len(g.GraphThreads[t].Vertices))
	}
	numVisitableNow := uint64(0)
	numActiveAndVisitable := uint64(0)

	visited = g.NodeParallelFor(func(ordinalStart, _ uint32, gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
		tidx := gt.Tidx
		visitCount := 0
		tNumVisitableNow := uint64(0)
		tVertexCount := uint64(0)
		tActiveVertices := uint64(0)
		tNumActiveAndVisitable := uint64(0)
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			if gt.VertexStructure(i).CreateEvent > atEventIndex {
				continue
			}
			tVertexCount++
			active := false
			for e := range gt.Vertices[i].OutEdges {
				if gt.Vertices[i].OutEdges[e].Property.End == 0 {
					tActiveVertices++
					active = true
					break
				}
			}

			prop := gt.VertexProperty(i)
			if len(prop.Windows) > 0 {
				if active {
					tNumActiveAndVisitable++
				}
				visitCount++
				allWindows[tidx] = append(allWindows[tidx], len(prop.Windows))
				maxWindows[tidx] = utils.Max(maxWindows[tidx], uint64(len(prop.Windows)))
				for w := range prop.Windows {
					if prop.Windows[w].End == math.MaxUint64 {
						tNumVisitableNow++
						break
					}
				}
			}
		}
		atomic.AddUint64(&numActiveAndVisitable, tNumActiveAndVisitable)
		atomic.AddUint64(&numVisitableNow, tNumVisitableNow)
		atomic.AddUint64(&realVertexCount, tVertexCount)
		atomic.AddUint64(&activeVertices, tActiveVertices)
		return visitCount
	})
	for t := uint32(1); t < g.NumThreads; t++ {
		allWindows[0] = append(allWindows[0], allWindows[t]...)
	}
	maxWC = utils.MaxSlice(maxWindows)
	pctVisitedNow = float64(numVisitableNow) / float64(visited) * 100.0
	wZPctCurrVCould = float64(visited) / float64(realVertexCount) * 100.0
	pctOfActiveIsCurrentVisitable = float64(numVisitableNow) / float64(activeVertices) * 100.0

	if print {
		log.Info().Msg("Could Visited: " + utils.F("%10d", visited) +
			", Percent Of Final Graph: " + utils.F("%3.3f", float64(visited)/float64(realVertexCount)*100.0))
		log.Info().Msg("Still Now:     " + utils.F("%10d", numVisitableNow) +
			", Percent Of Final Graph: " + utils.F("%3.3f", float64(numVisitableNow)/float64(realVertexCount)*100.0) +
			", Percent Of Visited: " + utils.F("%3.3f", pctVisitedNow))
		log.Info().Msg("Median Window Size of Visited: " + utils.V(utils.Median(allWindows[0])))
		log.Info().Msg("Max Window Size: " + utils.V(maxWC))
	}

	maxStart := make([]uint64, g.NumThreads)
	minStart := make([]uint64, g.NumThreads)
	maxEnd := make([]uint64, g.NumThreads)
	minEnd := make([]uint64, g.NumThreads)
	maxHops := make([]uint64, g.NumThreads)

	allStart := make([][]uint64, g.NumThreads)
	allEnd := make([][]uint64, g.NumThreads)
	allHops := make([][]uint64, g.NumThreads)

	for t := uint32(0); t < g.NumThreads; t++ {
		allStart[t] = make([]uint64, 0, len(g.GraphThreads[t].Vertices))
		allEnd[t] = make([]uint64, 0, len(g.GraphThreads[t].Vertices))
		allHops[t] = make([]uint64, 0, len(g.GraphThreads[t].Vertices))
		minStart[t] = math.MaxUint64
		minEnd[t] = math.MaxUint64
	}

	for w := 0; w < int(utils.MaxSlice(maxWindows)); w++ {

		visitedW := g.NodeParallelFor(func(ordinalStart, _ uint32, gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
			tidx := gt.Tidx
			vw := 0
			for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
				if gt.VertexStructure(i).CreateEvent > atEventIndex {
					continue
				}
				prop := gt.VertexProperty(i)
				if len(prop.Windows) > w {
					maxStart[tidx] = utils.Max(maxStart[tidx], prop.Windows[w].Start)
					if prop.Windows[w].Start != 0 {
						minStart[tidx] = utils.Min(minStart[tidx], prop.Windows[w].Start)
					}
					allStart[tidx] = append(allStart[tidx], prop.Windows[w].Start)
					maxEnd[tidx] = utils.Max(maxEnd[tidx], prop.Windows[w].End)
					minEnd[tidx] = utils.Min(minEnd[tidx], prop.Windows[w].End)
					allEnd[tidx] = append(allEnd[tidx], prop.Windows[w].End)
					maxHops[tidx] = utils.Max(maxHops[tidx], prop.Windows[w].Hops)
					allHops[tidx] = append(allHops[tidx], prop.Windows[w].Hops)
					vw++
				}
			}
			return vw
		})

		// Compress arrays to first entry.
		for t := uint32(1); t < g.NumThreads; t++ {
			allStart[0] = append(allStart[0], allStart[t]...)
			allEnd[0] = append(allEnd[0], allEnd[t]...)
			allHops[0] = append(allHops[0], allHops[t]...)
		}

		if w == 0 {
			wZMax = utils.MaxSlice(maxStart)
			wZMedian = utils.Median(allStart[0])
			wZMin = utils.MinSlice(minStart)
		}
		if (w == 0 && print) || printAll {
			log.Info().Msg("Window " + utils.V(w) + " Visited: " + utils.V(visitedW) + ", PercentAll: " + utils.F("%.3f", float64(visitedW)/float64(realVertexCount)*100.0) + " PercentVisited: " + utils.F("%.3f", float64(visitedW)/float64(visited)*100.0))
			log.Info().Msg("\tMedian Start Time:   " + utils.V(utils.Median(allStart[0])))
			log.Info().Msg("\tMedian End Time:     " + utils.V(utils.Median(allEnd[0])))
			log.Info().Msg("\tMedian Hop Estimate: " + utils.V(utils.Median(allHops[0])))
			log.Info().Msg("\tMax Start (longest shortest path): " + utils.V(utils.MaxSlice(maxStart)))
			log.Info().Msg("\tMax End   (longest shortest path): " + utils.V(utils.MaxSlice(maxEnd)))
			log.Info().Msg("\tMax Hop Estimate  (longest found): " + utils.V(utils.MaxSlice(maxHops)))
			log.Info().Msg("\tMin Start (earliest shortest path): " + utils.V(utils.MinSlice(minStart)))
			log.Info().Msg("\tMin End   (earliest shortest path): " + utils.V(utils.MinSlice(minEnd)))
		}

		for t := uint32(0); t < g.NumThreads; t++ {
			maxStart[t] = 0
			maxEnd[t] = 0
			maxHops[t] = 0
			allStart[t] = allStart[t][:0]
			allEnd[t] = allEnd[t][:0]
			allHops[t] = allHops[t][:0]
		}
	}
	return wZMin, wZMedian, wZMax, maxWC, pctVisitedNow, visited, wZPctCurrVCould, realVertexCount, activeVertices, pctOfActiveIsCurrentVisitable
}
