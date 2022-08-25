package graph

import (
	"bufio"
	"encoding/gob"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
)

type RawEdge struct {
	SrcRaw uint32
	DstRaw uint32
	Weight float64
}

// EdgeDequeuer reads edges from queuechan and update the graph structure correspondingly
func (g *Graph[VertexProp, EdgeProp]) EdgeDequeuer(queuechan chan RawEdge, deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		srcIdx := g.VertexMap[uint32(qElem.SrcRaw)]
		dstIdx := g.VertexMap[uint32(qElem.DstRaw)]

		g.Vertices[srcIdx].OutEdges = append(g.Vertices[srcIdx].OutEdges, NewEdge[EdgeProp](uint32(dstIdx), qElem.Weight))
	}
	deqWg.Done()
}

// EdgeEnqueuer reads edges in the file and writes them to queuechans
func EdgeEnqueuer(queuechans []chan RawEdge, graphName string, undirected bool, wg *sync.WaitGroup, idx uint64, enqCount uint64, deqCount uint64, result chan uint64) {
	file, err := os.Open(graphName)
	enforce.ENFORCE(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := uint64(0)
	mLines := uint64(0)
	var lineText string
	//var stringFields []string
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
		stringFields := strings.Fields(lineText)
		sflen := len(stringFields)
		enforce.ENFORCE(sflen == 2 || sflen == 3)
		src, _ := strconv.Atoi(stringFields[0])
		dst, _ := strconv.Atoi(stringFields[1])

		weight := 1.0
		if sflen == 3 {
			weight, _ = strconv.ParseFloat(stringFields[2], 64)
		}
		//stringFields = nil

		// TODO: Deal with multi-graphs :)
		//if src == dst {
		//	continue
		//}

		queuechans[uint64(src)%deqCount] <- RawEdge{uint32(src), uint32(dst), weight}
		if undirected {
			queuechans[uint64(dst)%deqCount] <- RawEdge{uint32(dst), uint32(src), weight}
		}
	}
	result <- mLines
	wg.Done()
}

func (g *Graph[VertexProp, EdgeProp]) LoadVertexMap(graphName string) {
	vmap := graphName + ".vmap"
	file, err := os.Open(vmap)
	if err != nil {
		// The file does not exist
		g.BuildMap(graphName)

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
func (g *Graph[VertexProp, EdgeProp]) BuildMap(graphName string) {
	file, err := os.Open(graphName)
	enforce.ENFORCE(err)
	defer file.Close()

	work := make(chan RawEdge, 256)
	go func(mWork chan RawEdge) {
		for elem := range mWork {
			srcRaw := elem.SrcRaw
			dstRaw := elem.DstRaw
			if _, ok := g.VertexMap[uint32(srcRaw)]; !ok {
				sidx := uint32(len(g.VertexMap))
				g.VertexMap[uint32(srcRaw)] = sidx
				g.Vertices = append(g.Vertices, Vertex[VertexProp, EdgeProp]{Id: uint32(srcRaw)})
			}
			if _, ok := g.VertexMap[uint32(dstRaw)]; !ok {
				didx := uint32(len(g.VertexMap))
				g.VertexMap[uint32(dstRaw)] = didx
				g.Vertices = append(g.Vertices, Vertex[VertexProp, EdgeProp]{Id: uint32(dstRaw)})
			}
		}
	}(work)

	scanner := bufio.NewScanner(file)
	lines := uint32(0)
	for scanner.Scan() {
		lines++
		t := scanner.Text()
		if strings.HasPrefix(t, "#") {
			continue
		}
		s := strings.Fields(t)
		enforce.ENFORCE(len(s) == 2 || len(s) == 3)
		src, _ := strconv.Atoi(s[0])
		dst, _ := strconv.Atoi(s[1])

		work <- RawEdge{uint32(src), uint32(dst), 0.0}
	}
	close(work)
}

// LoadGraphStatic loads the graph store in the file. The graph structure is updated to reflect the complete graph
// before returning.
func (g *Graph[VertexProp, EdgeProp]) LoadGraphStatic(graphName string, undirected bool) {
	deqCount := mathutils.MaxUint64(uint64(THREADS), 1)
	enqCount := mathutils.MaxUint64(uint64(THREADS/2), 1)

	m0 := time.Now()
	g.VertexMap = make(map[uint32]uint32)

	g.LoadVertexMap(graphName)
	t0 := time.Since(m0)
	info("Built map (ms) ", t0.Milliseconds())
	m1 := time.Now()

	queuechans := make([]chan RawEdge, deqCount)
	var deqWg sync.WaitGroup
	deqWg.Add(int(deqCount))
	for i := uint64(0); i < deqCount; i++ {
		queuechans[i] = make(chan RawEdge, 4096)
		go g.EdgeDequeuer(queuechans[i], &deqWg)
	}

	resultchan := make(chan uint64, enqCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(enqCount))
	for i := uint64(0); i < enqCount; i++ {
		go EdgeEnqueuer(queuechans, graphName, undirected, &enqWg, i, enqCount, deqCount, resultchan)
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
