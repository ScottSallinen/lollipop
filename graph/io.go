package graph

import (
	"bufio"
	"encoding/gob"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
)

type RawEdge[EdgeProp any] struct {
	SrcRaw       uint32
	DstRaw       uint32
	EdgeProperty EdgeProp
}

// EdgeDequeuer reads edges from queuechan and update the graph structure correspondingly
func (g *Graph[VertexProp, EdgeProp, MsgType]) EdgeDequeuer(queuechan chan RawEdge[EdgeProp], deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		srcIdx := g.VertexMap[uint32(qElem.SrcRaw)]
		dstIdx := g.VertexMap[uint32(qElem.DstRaw)]

		g.Vertices[srcIdx].OutEdges = append(g.Vertices[srcIdx].OutEdges, Edge[EdgeProp]{Property: qElem.EdgeProperty, Destination: uint32(dstIdx)})
	}
	deqWg.Done()
}

// EdgeEnqueuer reads edges in the file and writes them to queuechans
func (g *Graph[VertexProp, EdgeProp, MsgType]) EdgeEnqueuer(queuechans []chan RawEdge[EdgeProp], graphName string, edgeParser EdgeParserFunc[EdgeProp], wg *sync.WaitGroup, idx uint64, enqCount uint64, deqCount uint64, result chan uint64) {
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
		// 	continue
		// }

		queuechans[uint64(rawEdge.SrcRaw)%deqCount] <- rawEdge
		if undirected {
			queuechans[uint64(rawEdge.DstRaw)%deqCount] <- RawEdge[EdgeProp]{SrcRaw: rawEdge.DstRaw, DstRaw: rawEdge.SrcRaw, EdgeProperty: rawEdge.EdgeProperty}
		}
	}
	result <- mLines
	wg.Done()
}

func (g *Graph[VertexProp, EdgeProp, MsgType]) LoadVertexMap(graphName string, edgeParser EdgeParserFunc[EdgeProp]) {
	vmap := graphName + ".vmap"
	file, err := os.Open(vmap)
	if err != nil {
		// The file does not exist
		g.BuildMap(graphName, edgeParser)

		// write the VertexMap
		newFile, err := os.Create(vmap)
		enforce.ENFORCE(err)
		defer newFile.Close()
		err = gob.NewEncoder(newFile).Encode(g.VertexMap)
		enforce.ENFORCE(err)
	} else {
		// The file exists, load the VertexMap stored in the file
		defer file.Close()
		g.VertexMap = make(map[uint32]uint32)
		err = gob.NewDecoder(file).Decode(&g.VertexMap)
		enforce.ENFORCE(err)
		g.Vertices = make([]Vertex[VertexProp, EdgeProp], len(g.VertexMap))
		for k, v := range g.VertexMap {
			g.Vertices[v] = Vertex[VertexProp, EdgeProp]{Id: k}
		}
	}
}

// BuildMap reads all edges stored in the file and constructs the VertexMap for all vertices found
func (g *Graph[VertexProp, EdgeProp, MsgType]) BuildMap(graphName string, edgeParser EdgeParserFunc[EdgeProp]) {
	work := make(chan RawEdge[EdgeProp], 256)
	go func() {
		file, err := os.Open(graphName)
		enforce.ENFORCE(err)
		defer file.Close()

		scanner := bufio.NewScanner(file)
		lines := uint32(0)
		for scanner.Scan() {
			lines++
			t := scanner.Text()
			if strings.HasPrefix(t, "#") {
				continue
			}
			rawEdge := edgeParser(t)
			work <- RawEdge[EdgeProp]{rawEdge.SrcRaw, rawEdge.DstRaw, rawEdge.EdgeProperty}
		}
		close(work)
	}()

	for elem := range work {
		srcRaw := elem.SrcRaw
		dstRaw := elem.DstRaw
		if _, ok := g.VertexMap[srcRaw]; !ok {
			sidx := uint32(len(g.VertexMap))
			g.VertexMap[srcRaw] = sidx
			g.Vertices = append(g.Vertices, Vertex[VertexProp, EdgeProp]{Id: srcRaw})
		}
		if _, ok := g.VertexMap[dstRaw]; !ok {
			didx := uint32(len(g.VertexMap))
			g.VertexMap[dstRaw] = didx
			g.Vertices = append(g.Vertices, Vertex[VertexProp, EdgeProp]{Id: dstRaw})
		}
	}
}

// LoadGraphStatic loads the graph store in the file. The graph structure is updated to reflect the complete graph
// before returning.
func (g *Graph[VertexProp, EdgeProp, MsgType]) LoadGraphStatic(graphName string, edgeParser EdgeParserFunc[EdgeProp]) {
	deqCount := mathutils.Max(uint64(THREADS), 1)
	enqCount := mathutils.Max(uint64(THREADS/2), 1)

	m0 := time.Now()
	g.VertexMap = make(map[uint32]uint32)

	g.LoadVertexMap(graphName, edgeParser)
	t0 := time.Since(m0)
	info("Built map (ms) ", t0.Milliseconds())
	m1 := time.Now()

	queuechans := make([]chan RawEdge[EdgeProp], deqCount)
	var deqWg sync.WaitGroup
	deqWg.Add(int(deqCount))
	for i := uint64(0); i < deqCount; i++ {
		queuechans[i] = make(chan RawEdge[EdgeProp], 4096)
		go g.EdgeDequeuer(queuechans[i], &deqWg)
	}

	resultchan := make(chan uint64, enqCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(enqCount))
	for i := uint64(0); i < enqCount; i++ {
		go g.EdgeEnqueuer(queuechans, graphName, edgeParser, &enqWg, i, enqCount, deqCount, resultchan)
	}
	enqWg.Wait()
	for i := uint64(0); i < deqCount; i++ {
		close(queuechans[i])
	}
	close(resultchan)
	lines := uint64(0)
	for e := range resultchan {
		lines += e
	}
	deqWg.Wait()

	t1 := time.Since(m1)
	info("Read ", lines, " edges in (ms) ", t1.Milliseconds())
}
