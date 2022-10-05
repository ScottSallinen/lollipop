package graph

import (
	"bufio"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
)

// SendAdd: Direct add for debugging
func (g *Graph[VertexProp, EdgeProp, MsgType]) SendAdd(srcRaw uint32, dstRaw uint32, EdgeProperty EdgeProp) {
	g.ThreadStructureQ[g.RawIdToThreadIdx(srcRaw)] <- StructureChange[EdgeProp]{Type: ADD, SrcRaw: srcRaw, DstRaw: dstRaw, EdgeProperty: EdgeProperty}
}

// SendDel: Direct delete for debugging
func (g *Graph[VertexProp, EdgeProp, MsgType]) SendDel(srcRaw uint32, dstRaw uint32) {
	g.ThreadStructureQ[g.RawIdToThreadIdx(srcRaw)] <- StructureChange[EdgeProp]{Type: DEL, SrcRaw: srcRaw, DstRaw: dstRaw}
}

// DynamicEdgeEnqueuer reads edges in the file and writes corresponding StructureChange to the ThreadStructureQ of the source vertex's thread
func (g *Graph[VertexProp, EdgeProp, MsgType]) DynamicEdgeEnqueuer(graphName string, edgeParser EdgeParserFunc[EdgeProp], wg *sync.WaitGroup, idx uint64, enqCount uint64, result chan uint64) {
	file, err := os.Open(graphName)
	enforce.ENFORCE(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := uint64(0)
	mLines := uint64(0)
	var lineText string
	undirected := g.Options.Undirected
	for scanner.Scan() {
		lines++
		if lines%enqCount != idx {
			continue
		}
		mLines++
		lineText = scanner.Text()
		if strings.HasPrefix(lineText, "#") {
			continue
		}
		rawEdge := edgeParser(lineText)

		// TODO: Option to remove self reference edges?
		// if rawEdge.SrcRaw == rawEdge.DstRaw {
		//   continue
		// }

		g.ThreadStructureQ[g.RawIdToThreadIdx(rawEdge.SrcRaw)] <- StructureChange[EdgeProp]{Type: ADD, SrcRaw: rawEdge.SrcRaw, DstRaw: rawEdge.DstRaw, EdgeProperty: rawEdge.EdgeProperty}
		if undirected {
			g.ThreadStructureQ[g.RawIdToThreadIdx(rawEdge.DstRaw)] <- StructureChange[EdgeProp]{Type: ADD, SrcRaw: rawEdge.DstRaw, DstRaw: rawEdge.SrcRaw, EdgeProperty: rawEdge.EdgeProperty}
		}
	}
	result <- mLines
	wg.Done()
}

// LoadGraphDynamic starts multiple DynamicEdgeEnqueuer to read edges stored in the file. When it returns, all edges
// are read.
func (g *Graph[VertexProp, EdgeProp, MsgType]) LoadGraphDynamic(graphName string, edgeParser EdgeParserFunc[EdgeProp], feederWg *sync.WaitGroup) {
	// The enqueue count here should actually be just 1 to honour an event log properly.
	// If order is irrelevant, then we can scrape through it potentially faster with more..
	// perhaps this should be parameterized.
	// Also, it may be reasonable to have multiple files as input sources, each could have it's own enqueuer.
	// .. assuming the sources are independent.
	//enqCount := mathutils.Max(uint64(THREADS/4), 1)
	enqCount := uint64(1)

	m1 := time.Now()

	resultchan := make(chan uint64, enqCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(enqCount))
	for i := uint64(0); i < enqCount; i++ {
		go g.DynamicEdgeEnqueuer(graphName, edgeParser, &enqWg, i, enqCount, resultchan)
	}
	enqWg.Wait()

	close(resultchan)
	lines := uint64(0)
	for e := range resultchan {
		lines += e
	}

	t1 := time.Since(m1)
	info("Read ", lines, " edges in (ms) ", t1.Milliseconds(), " fromWatch ", g.Watch.Elapsed())

	for i := 0; i < THREADS; i++ {
		close(g.ThreadStructureQ[i])
	}
	feederWg.Done()
}
