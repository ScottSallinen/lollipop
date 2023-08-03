package m

import (
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

var CheckStability = false
var VertexFlowDB struct {
	Dynamic []map[graph.RawType]int64
	Static  []map[graph.RawType]int64
}

func (pr *PushRelabel) getFlowSnapshot(g *Graph, AtEventIndex uint64) map[graph.RawType]int64 {
	flowSnapshot := make(map[graph.RawType]int64)
	sourceId := pr.VertexCount.GetSourceId()

	g.NodeForEachVertex(func(o, internalId uint32, v *Vertex) {
		vertexStructure := g.NodeVertexStructure(internalId)
		if vertexStructure.CreateEvent <= AtEventIndex {
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

			if Flow > 0 {
				flowSnapshot[vertexStructure.RawId] = Flow
			}
		}
	})
	return flowSnapshot
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

	var flowSnapshotDynamic, flowSnapshotStatic map[graph.RawType]int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		flowSnapshotDynamic = pr.getFlowSnapshot(g, AtEventIndex)
		wg.Done()
	}()
	go func() {
		flowSnapshotStatic = pr.getFlowSnapshot(oracle, AtEventIndex)
		wg.Done()
	}()
	wg.Wait()

	VertexFlowDB.Dynamic = append(VertexFlowDB.Dynamic, flowSnapshotDynamic)
	VertexFlowDB.Static = append(VertexFlowDB.Static, flowSnapshotStatic)
	runtime.GC()
}

func PrintVertexFlowDB(fileOut bool, stdOut bool) {
	var f *os.File
	if fileOut {
		f = utils.CreateFile("results/push-relabel-m-timeseries-flow.csv")
		defer f.Close()
	}

	header := "Date,MaxFlow,VertexCount,EdgeCount,Latency,CurrentRuntime,AlgTimeSinceLast"
	header += "VertexNumInFlow,VertexPercentDiff"
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
		line := entry.Name.Format("2006-01-02") + "," + strconv.FormatInt(int64(entry.CurrentMaxFlow), 10) + "," +
			strconv.FormatUint(entry.VertexCount, 10) + "," + strconv.FormatUint(entry.EdgeCount, 10) + "," +
			strconv.FormatInt(entry.Latency.Milliseconds(), 10) + "," +
			strconv.FormatInt(entry.CurrentRuntime.Milliseconds(), 10) + "," + strconv.FormatInt(entry.AlgTimeSinceLast.Milliseconds(), 10)
		// VertexFlowDynamic := &VertexFlowDB.Dynamic[i]
		// VertexFlowStatic := &VertexFlowDB.Dynamic[i]
		// line += "," + VertexFlowDB[i].Dynamic

		if stdOut {
			println(line)
		}
		if fileOut {
			f.WriteString(line + "\n")
		}
	}
}
