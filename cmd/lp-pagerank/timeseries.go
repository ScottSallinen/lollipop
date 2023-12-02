package main

import (
	"os"
	"strconv"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type NamedEntry struct {
	Name             time.Time
	VertexCount      uint64
	Singletons       uint64
	EdgeCount        uint64
	Entry            []utils.Pair[graph.RawType, float64]
	Latency          time.Duration
	CurrentRuntime   time.Duration
	AlgTimeSinceLast time.Duration
}

const WRITE_EVERY_UPDATE = false // Writes the timeseries file every single time it updates (instead of just at the end).
var logWaterfall = false         // Each entry in the timeseries will include the top N vertices sorted by score, formatted for easy import into a spreadsheet.
var logDerivative = false        // Instead computes the derivative of the score of the top N vertices, showing the rate of change over time.

// Test interest array for wikipedia-growth.
const USE_INTEREST_ARRAY = false

var INTEREST_ARRAY = make(map[graph.RawType]bool)

func init() {
	if USE_INTEREST_ARRAY {
		interestVertices := []int{0, 1, 32, 19, 16, 31, 41, 27, 21, 28, 3, 4, 20, 10, 9, 17, 11, 40, 72, 77, 175, 13, 129, 63, 26, 54, 45, 145, 404, 7, 139, 78, 89, 108, 825, 1690, 448, 983, 1663, 363, 9724, 9499, 5880, 13753, 1439, 945, 760, 8661, 3873, 6379, 18416, 25336, 3541, 7200, 17608, 15741, 20875, 20004, 8298, 27029, 15254, 15113, 12977, 30486, 38532, 3144, 396, 43668, 23761, 13951, 29990, 1362, 38806, 5051, 62543, 85033, 499, 3943, 54939, 263, 232670, 123429, 43, 314656, 14253, 95820, 30501, 383716, 35332, 199149, 211505, 22935, 313237, 466, 442851, 451689, 397734, 349826, 304673, 497798, 257353, 392586, 501649, 349720, 68353, 521430, 21027, 516078, 542430, 577785, 434864, 571, 3153, 71051, 57944, 588933, 33196, 157855, 567744, 5031, 36714, 71313, 502309, 52753, 20443, 481735, 342768, 3101, 168555, 605333, 9238, 515209, 95098, 613937, 612915, 299769, 230252, 435664, 588104, 619090, 312562, 210296, 624474, 624470, 624466, 624469, 121663, 528221, 407018, 623114, 616564, 624837, 624461, 78544, 630135, 628067, 500290, 129378, 619293, 527009, 353931, 271789, 272600, 644624, 643442, 644444, 630264, 398223, 614758, 642715, 6652, 144613, 642119, 641390, 651118, 646554, 663304, 646571, 668418, 669013, 653366, 660978, 670054, 243924}
		for _, v := range interestVertices {
			INTEREST_ARRAY[graph.AsRawType(v)] = true
		}
	}
}

var tsDB = make([]NamedEntry, 0)

func (*PageRank) OnApplyTimeSeries(tse graph.TimeseriesEntry[VertexProperty, EdgeProperty, Mail, Note]) {
	var outEntry NamedEntry
	if logWaterfall {
		data := make([]float64, tse.GraphView.NodeVertexCount())
		externs := make([]graph.RawType, tse.GraphView.NodeVertexCount())
		singletons := uint64(0)

		tse.GraphView.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) {
			data[i] = prop.Mass
			externs[i] = tse.GraphView.NodeVertexRawID(v)
			if NORM_IGNORE_SINGLETONS { // Discarded by norm step
				if len(vertex.OutEdges) == 0 && prop.Mass == 0 {
					singletons++
				}
			}
		})

		topN := uint32(10)
		var ranking []utils.Pair[uint32, float64]
		res := make([]utils.Pair[graph.RawType, float64], topN)
		if !USE_INTEREST_ARRAY {
			if uint32(len(data)) < topN {
				topN = uint32(len(data))
			}
			ranking = utils.FindTopNInArray(data, topN)
			for i := range ranking {
				res[i].First = externs[ranking[i].First]
				res[i].Second = ranking[i].Second
			}
		} else {
			for rawId := range INTEREST_ARRAY {
				vidx, vertex := tse.GraphView.NodeVertexFromRaw(rawId)
				if vertex != nil {
					res = append(res, utils.Pair[graph.RawType, float64]{First: rawId, Second: tse.GraphView.NodeVertexProperty(vidx).Mass})
				}
			}
		}

		outEntry = NamedEntry{tse.Name, uint64(tse.GraphView.NodeVertexCount()), singletons, tse.EdgeCount, res, tse.Latency, tse.CurrentRuntime, tse.AlgTimeSinceLast}
	} else {
		outEntry = NamedEntry{tse.Name, uint64(tse.GraphView.NodeVertexCount()), 0, tse.EdgeCount, nil, tse.Latency, tse.CurrentRuntime, tse.AlgTimeSinceLast}
	}
	tse.GraphView = nil
	tsDB = append(tsDB, outEntry)
	if WRITE_EVERY_UPDATE {
		PrintTimeSeries(true, false)
	}
}

// Spits out top10 for each point in time (entry).
// Will print all possible vertices for each row (blanks for non entries) to help with formatting.
func PrintTimeSeries(fileOut bool, stdOut bool) {
	if stdOut {
		println("Timeseries:")
	}
	allVerticesMap := make(map[graph.RawType]int) // Map of real vertex to our array index
	allVerticesArr := make([]float64, 0)

	var f *os.File
	if fileOut {
		f = utils.CreateFile("results/timeseries.csv")
		defer f.Close()
	}

	header := "ts,date,cumRuntimeMS,algTimeSinceLastMS,qLatencyMS,v,e,"
	if logWaterfall {
		if NORM_IGNORE_SINGLETONS {
			header += "singletonCount,"
		}
		header += ","
		for i := range tsDB {
			for _, e := range tsDB[i].Entry {
				if _, ok := allVerticesMap[e.First]; !ok {
					allVerticesMap[e.First] = len(allVerticesArr)
					allVerticesArr = append(allVerticesArr, 0)
					header += e.First.String() + ","
				}
			}
		}
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
	allVerticesArrLast := make([]float64, len(allVerticesArr))
	allVerticesArrCurr := make([]float64, len(allVerticesArr))
	allVerticesArrNext := make([]float64, len(allVerticesArr))

	for i := range tsDB {
		for j := range allVerticesArr {
			allVerticesArr[j] = 0
		}
		// Tweaked for better excel importing...
		line := tsDB[i].Name.Format(time.RFC3339) + "," + tsDB[i].Name.Format("2006-01-02") + ","
		line = line + strconv.FormatInt(tsDB[i].CurrentRuntime.Milliseconds(), 10) + "," + strconv.FormatInt(tsDB[i].AlgTimeSinceLast.Milliseconds(), 10) + "," + strconv.FormatInt(tsDB[i].Latency.Milliseconds(), 10) + ","
		line = line + strconv.FormatUint(tsDB[i].VertexCount, 10) + "," + strconv.FormatUint(tsDB[i].EdgeCount, 10)

		if logWaterfall {
			if NORM_IGNORE_SINGLETONS {
				line = line + "," + strconv.FormatUint(tsDB[i].Singletons, 10)
			}

			if !logDerivative {
				line = line + "," + tsDB[i].Name.Format("2006-01-02") + ","
				for _, e := range tsDB[i].Entry {
					allVerticesArr[allVerticesMap[e.First]] = e.Second
				}
				for j := range allVerticesArr {
					if allVerticesArr[j] != 0 {
						line += utils.F("%.5f,", allVerticesArr[j])
					} else {
						line += ","
					}
				}
			} else {
				// This method calculates the derivative of the value over time (using the two point central difference method)
				if i < len(tsDB)-1 {
					line = line + "," + tsDB[i].Name.Format("2006-01-02") + ","
					for _, e := range tsDB[i+1].Entry {
						allVerticesArrNext[allVerticesMap[e.First]] = e.Second
					}
					if i > 0 {
						for j := range allVerticesArrNext {
							if allVerticesArrNext[j] != 0 && allVerticesArrLast[j] != 0 {
								delta := (100.0 * (allVerticesArrNext[j] - allVerticesArrLast[j])) / (tsDB[i+1].Name.Sub(tsDB[i-1].Name).Hours() / 24)
								line += utils.F("%.10f,", delta)
							} else {
								line += ","
							}
						}
					} else {
						for _, e := range tsDB[i].Entry {
							allVerticesArrCurr[allVerticesMap[e.First]] = e.Second
						}
					}
					allVerticesArrLast = allVerticesArrCurr
					allVerticesArrCurr = allVerticesArrNext
					allVerticesArrNext = make([]float64, len(allVerticesArrLast))
				}
			}
		}
		if stdOut {
			println(line)
		}
		if fileOut {
			f.WriteString(line + "\n")
		}
	}
}
