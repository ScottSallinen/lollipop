package main

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type NamedEntry struct {
	Name             time.Time
	VertexCount      uint64
	RealVertexCount  uint64
	EdgeCount        uint64
	EdgeDeletes      uint64
	AtEventIndex     uint64
	Entry            []utils.Pair[graph.RawType, uint32]
	EntryAll         map[graph.RawType]uint32
	Latency          time.Duration
	CurrentRuntime   time.Duration
	AlgTimeSinceLast time.Duration
}

const WRITE_EVERY_UPDATE = false // Writes the timeseries file every single time it updates (instead of just at the end).

// Top 50 vertices (by PageRank score) in the wikipedia-growth graph
var INTEREST_ARRAY = []int{73, 259, 9479, 6276, 1710, 864, 2169, 110, 10312, 69, 425, 611, 1566, 11297, 1916, 1002, 975, 6413, 526, 5079, 1915, 11956, 2034, 956, 208, 15, 77041, 652, 20, 1352, 1918, 388, 1806, 1920, 3517, 863, 1594, 24772, 2008, 78349, 397, 1923, 1105, 8707, 7, 4336, 1753, 205, 17, 984, 5732, 983, 70, 1924, 111, 51076, 6903, 4083, 1936, 1115, 154942, 1550, 2266, 179, 1933, 37976, 2844, 1934, 57028, 1932, 84204, 1931, 490, 1935, 2312, 1925, 1846, 5081, 1930, 4378, 1917, 68, 3080, 2734, 435, 1482, 1929, 1922, 4104, 2814, 1926, 1919, 1164, 1110, 1928, 2843, 4364, 1921, 4148, 2041}
var INTEREST_MAP = make(map[graph.RawType]int)
var USE_INTEREST = false // Of interest, array for wikipedia-growth.

func init() {
	for i, v := range INTEREST_ARRAY {
		INTEREST_MAP[graph.AsRawType(v)] = i
	}
}

var tsDB = make([]NamedEntry, 0)
var snapshotDB [][]utils.Pair[graph.RawType, uint32]
var snapshotDBAll []map[graph.RawType]uint32

func (*Colouring) OnApplyTimeSeries(entries chan graph.TimeseriesEntry[VertexProperty, EdgeProperty, Mail, Note], wg *sync.WaitGroup) {
	for tse := range entries {
		outEntry := NamedEntry{
			Name:             tse.Name,
			VertexCount:      uint64(tse.GraphView.NodeVertexCount()), // Upper bound / estimate (emit vertices may not actually exist yet -- singletons)
			EdgeCount:        tse.EdgeCount,
			EdgeDeletes:      tse.EdgeDeletes,
			AtEventIndex:     tse.AtEventIndex,
			Latency:          tse.Latency,
			CurrentRuntime:   tse.CurrentRuntime,
			AlgTimeSinceLast: tse.AlgTimeSinceLast,
		}
		if USE_INTEREST {
			outEntry.Entry = make([]utils.Pair[graph.RawType, uint32], len(INTEREST_ARRAY))
			for i := range INTEREST_ARRAY {
				outEntry.Entry[i] = utils.Pair[graph.RawType, uint32]{
					First:  graph.AsRawType(INTEREST_ARRAY[i]),
					Second: EMPTY_VAL,
				}
			}
		} else {
			outEntry.EntryAll = make(map[graph.RawType]uint32)
		}

		tse.GraphView.NodeForEachVertex(func(o, internalId uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
			vertexStructure := tse.GraphView.NodeVertexStructure(internalId)
			if vertexStructure.CreateEvent <= tse.AtEventIndex { // TODO: should probably have the framework provide a better way to address this.
				outEntry.RealVertexCount++
				if USE_INTEREST {
					if raw, ok := INTEREST_MAP[vertexStructure.RawId]; ok {
						outEntry.Entry[raw].Second = vertex.Property.Colour
					}
				} else {
					outEntry.EntryAll[vertexStructure.RawId] = vertex.Property.Colour
				}
			}
		})

		tse.GraphView = nil
		tse.AlgWaitGroup.Done()

		tsDB = append(tsDB, outEntry)
		if WRITE_EVERY_UPDATE {
			PrintTimeSeries(true, false)
		}
	}
	wg.Done()
}

func PrintTimeSeries(fileOut bool, stdOut bool) {
	if stdOut {
		println("Timeseries:")
	}

	var df *os.File
	var sf *os.File
	if fileOut {
		df = utils.CreateFile("results/colouring-timeseries.csv")
		sf = utils.CreateFile("results/colouring-snapshot-timeseries.csv")
		defer df.Close()
		defer sf.Close()
	}

	header := "ts,date,cumRuntimeMS,algTimeSinceLastMS,qLatencyMS,allVC,realVC,EC,"

	if USE_INTEREST {
		header += ","
		for _, raw := range INTEREST_ARRAY {
			header += strconv.FormatUint(uint64(raw), 10) + ","
		}
	} else {
		header += "pctSame,matchedVertices,same,different,colourCount"
	}

	if stdOut {
		println(header)
	}
	if fileOut {
		_, err := df.WriteString(header + "\n")
		if err != nil {
			panic(err)
		}
		_, err = sf.WriteString(header + "\n")
		if err != nil {
			panic(err)
		}
	}

	for i := range tsDB {
		// Tweaked for better excel importing...
		line := tsDB[i].Name.Format(time.RFC3339) + "," + tsDB[i].Name.Format("2006-01-02") + ","
		line = line + strconv.FormatInt(tsDB[i].CurrentRuntime.Milliseconds(), 10) + "," + strconv.FormatInt(tsDB[i].AlgTimeSinceLast.Milliseconds(), 10) + "," + strconv.FormatInt(tsDB[i].Latency.Milliseconds(), 10) + ","
		line = line + strconv.FormatUint(tsDB[i].VertexCount, 10) + "," + strconv.FormatUint(tsDB[i].RealVertexCount, 10) + "," + strconv.FormatUint(tsDB[i].EdgeCount, 10)

		dfLine := line
		sfLine := line

		if USE_INTEREST {
			dfLine += ",,"
			for _, c := range tsDB[i].Entry {
				if c.Second != EMPTY_VAL {
					dfLine += strconv.FormatUint(uint64(c.Second), 10)
				}
				dfLine += ","
			}

			if snapshotDB != nil {
				sfLine += ",,"
				for _, c := range snapshotDB[i] {
					if c.Second != EMPTY_VAL {
						sfLine += strconv.FormatUint(uint64(c.Second), 10)
					}
					sfLine += ","
				}
			}
		} else {
			same := 0
			different := 0
			if i == 0 {
				// Do nothing
			} else {
				for raw, c := range tsDB[i].EntryAll {
					if prevC, in := tsDB[i-1].EntryAll[raw]; in {
						if c == prevC {
							same++
						} else {
							different++
						}
					}
				}
			}
			dfLine += "," + strconv.FormatFloat(float64(same)*100.0/float64(same+different), 'f', 3, 64) + "," + strconv.FormatUint(uint64(same+different), 10) + "," + strconv.FormatUint(uint64(same), 10) + "," + strconv.FormatUint(uint64(different), 10)

			if snapshotDBAll != nil {
				colours := make(map[uint32]int32)
				same := 0
				different := 0
				if i == 0 {
					// Do nothing
				} else {
					for raw, c := range snapshotDBAll[i] {
						colours[c] += 1
						if prevC, in := snapshotDBAll[i-1][raw]; in {
							if c == prevC {
								same++
							} else {
								different++
							}
						}
					}
				}
				sfLine += "," + strconv.FormatFloat(float64(same)*100.0/float64(same+different), 'f', 3, 64) + "," + strconv.FormatUint(uint64(same+different), 10) + "," +
					strconv.FormatUint(uint64(same), 10) + "," + strconv.FormatUint(uint64(different), 10) + "," + strconv.FormatUint(uint64(len(colours)), 10)
			}
		}

		if stdOut {
			println(dfLine)
		}
		if fileOut {
			df.WriteString(dfLine + "\n")
			if snapshotDB != nil || snapshotDBAll != nil {
				sf.WriteString(sfLine + "\n")
			}
		}
	}
}
