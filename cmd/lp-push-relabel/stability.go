package main

import (
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type VertexFlowEntry struct {
	VerticesInFlow   int64
	VerticesSameFlow int64
	VertexCount      int64
}

var CheckStability = false
var VertexFlowDB struct {
	Dynamic       []VertexFlowEntry
	Static        []VertexFlowEntry
	StaticLatency []int64

	LastSnapshotDynamic map[graph.RawType]int64
	LastSnapshotStatic  map[graph.RawType]int64
}

func (pr *PushRelabel) getFlowSnapshot(g *Graph, AtEventIndex uint64, lastSnapshot map[graph.RawType]int64) (snapshot map[graph.RawType]int64, entry VertexFlowEntry) {
	flowSnapshot := make(map[graph.RawType]int64)
	sourceId := pr.SourceId.Load()

	vertexCount := int64(0)
	g.NodeForEachVertex(func(o, internalId uint32, v *Vertex) {
		vertexStructure := g.NodeVertexStructure(internalId)
		if vertexStructure.CreateEvent <= AtEventIndex {
			vertexCount += 1
			NbrIds := make(map[uint32]struct{})
			ResCapMap, ResCapSum := make(map[uint32]int64), int64(0)
			EdgeCapMap, EdgeCapSum := make(map[uint32]int64), int64(0)
			for _, n := range v.Property.Nbrs {
				if n.ResCapOut != 0 {
					AssertC(n.ResCapOut > 0)
					ResCapMap[n.Didx] += n.ResCapOut
					ResCapSum += n.ResCapOut
					NbrIds[n.Didx] = struct{}{}
				}
			}
			for _, e := range v.OutEdges {
				if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == sourceId || v.Property.Type == Sink {
					continue
				}
				EdgeCapMap[e.Didx] += int64(e.Property.Weight)
				EdgeCapSum += int64(e.Property.Weight)
				NbrIds[e.Didx] = struct{}{}
			}

			OutFlowMap, OutFlowSum := make(map[uint32]int64), int64(0)
			InFlowMap, InFlowSum := make(map[uint32]int64), int64(0)
			for id := range NbrIds {
				outFlow := EdgeCapMap[id] - ResCapMap[id]
				if outFlow > 0 {
					OutFlowMap[id] = outFlow
					OutFlowSum += outFlow
				} else if outFlow < 0 {
					InFlowMap[id] = -outFlow
					InFlowSum += -outFlow
				}
			}

			Flow := int64(0)
			if v.Property.Type == Source {
				AssertC(int64(v.Property.Excess) == ResCapSum)
				AssertC(InFlowSum == 0 && EdgeCapSum-int64(v.Property.Excess) == OutFlowSum)
				Flow = OutFlowSum
			} else if v.Property.Type == Sink {
				AssertC(EdgeCapSum+int64(v.Property.Excess) == ResCapSum)
				AssertC(OutFlowSum == 0 && int64(v.Property.Excess) == InFlowSum)
				Flow = InFlowSum
			} else {
				AssertC(ResCapSum == EdgeCapSum)
				AssertC(OutFlowSum == InFlowSum)
				Flow = OutFlowSum
			}

			AssertC(Flow >= 0)
			if Flow > 0 {
				flowSnapshot[vertexStructure.RawId] = Flow
			}
		}
	})

	// for v, f := range lastSnapshot {
	// 	AssertC(f > 0)
	// 	if flowSnapshot[v] != f {
	// 		entry.VerticesDiffFlow += 1
	// 	}
	// }

	for v, f := range flowSnapshot {
		AssertC(f > 0)
		if lastSnapshot[v] > 0 {
			entry.VerticesSameFlow += 1
		}
	}

	entry.VerticesInFlow = int64(len(flowSnapshot))
	entry.VertexCount = vertexCount
	return flowSnapshot, entry
}

// Note OnOracleCompare doesn't make much sense for this algorithm, since it is approximate.
// Instead, we will use this as a way to generate entries for a timeseries of the static solution at each point in time.
func (pr *PushRelabel) OnOracleCompare(g *Graph, oracle *Graph) {
	if !CheckStability {
		return
	}

	AtEventIndex := uint64(0)
	for t := 0; t < int(g.NumThreads); t++ {
		AtEventIndex = utils.Max(AtEventIndex, g.GraphThreads[t].AtEvent)
	}

	VertexFlowDB.StaticLatency = append(VertexFlowDB.StaticLatency, oracle.Watch.Elapsed().Milliseconds())

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		flowSnapshot, entry := pr.getFlowSnapshot(g, AtEventIndex, VertexFlowDB.LastSnapshotDynamic)
		VertexFlowDB.LastSnapshotDynamic = flowSnapshot
		VertexFlowDB.Dynamic = append(VertexFlowDB.Dynamic, entry)
		wg.Done()
	}()
	go func() {
		flowSnapshot, entry := pr.getFlowSnapshot(oracle, AtEventIndex, VertexFlowDB.LastSnapshotStatic)
		VertexFlowDB.LastSnapshotStatic = flowSnapshot
		VertexFlowDB.Static = append(VertexFlowDB.Static, entry)
		wg.Done()
	}()
	wg.Wait()
	runtime.GC()
}

func PrintVertexFlowDB(fileOut bool, stdOut bool) {
	var f *os.File
	if fileOut {
		f = utils.CreateFile("results/push-relabel-timeseries-flow.csv")
		defer f.Close()
	}

	header := "Date,MaxFlow,VertexCount,EdgeCount,Latency,CurrentRuntime,AlgTimeSinceLast"
	header += ",VertexNumInFlowDynamic,VertexFlowSameDynamic,VertexNumInFlowStatic,VertexFlowSameStatic,StaticLatency"
	if stdOut {
		println(header)
	}
	if fileOut {
		_, err := f.WriteString(header + "\n")
		if err != nil {
			panic(err)
		}
	}

	for i, entry := range TsDB {
		RealVertexCount := VertexFlowDB.Dynamic[i].VertexCount // The vertex count computed above (checked CreateEvent)
		AssertC(VertexFlowDB.Dynamic[i].VertexCount == VertexFlowDB.Static[i].VertexCount)

		line := entry.Name.Format("2006-01-02") + "," + strconv.FormatInt(int64(entry.CurrentMaxFlow), 10) + "," +
			strconv.FormatInt(RealVertexCount, 10) + "," + strconv.FormatUint(entry.EdgeCount, 10) + "," +
			strconv.FormatInt(entry.Latency.Milliseconds(), 10) + "," +
			strconv.FormatInt(entry.CurrentRuntime.Milliseconds(), 10) + "," + strconv.FormatInt(entry.AlgTimeSinceLast.Milliseconds(), 10)

		line += "," + strconv.FormatInt(VertexFlowDB.Dynamic[i].VerticesInFlow, 10)
		line += "," + strconv.FormatInt(VertexFlowDB.Dynamic[i].VerticesSameFlow, 10)
		line += "," + strconv.FormatInt(VertexFlowDB.Static[i].VerticesInFlow, 10)
		line += "," + strconv.FormatInt(VertexFlowDB.Static[i].VerticesSameFlow, 10)
		line += "," + strconv.FormatInt(VertexFlowDB.StaticLatency[i], 10)

		if stdOut {
			println(line)
		}
		if fileOut {
			f.WriteString(line + "\n")
		}
	}
}
