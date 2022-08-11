package graph

import (
	"bufio"
	"encoding/gob"
	"fmt"
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

func (g *Graph) EdgeDequeuer(queuechan chan RawEdge, deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		srcIdx := g.VertexMap[uint32(qElem.SrcRaw)]
		dstIdx := g.VertexMap[uint32(qElem.DstRaw)]

		g.Vertices[srcIdx].OutEdges = append(g.Vertices[srcIdx].OutEdges, NewEdge(uint32(dstIdx), qElem.Weight))
	}
	deqWg.Done()
}

func EdgeEnqueuer(queuechans []chan RawEdge, graphName string, wg *sync.WaitGroup, idx uint64, enqCount uint64, deqCount uint64, result chan uint64) {
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
	}
	result <- mLines
	wg.Done()
}

func (g *Graph) LoadVertexMap(graphName string) {
	vmap := graphName + ".vmap"
	file, err := os.Open(vmap)
	if err != nil {
		g.BuildMap(graphName)

		// write
		newFile, err := os.Create(vmap)
		enforce.ENFORCE(err)
		defer newFile.Close()
		err = gob.NewEncoder(newFile).Encode(g.VertexMap)
		enforce.ENFORCE(err)
	} else {
		defer file.Close()
		g.VertexMap = make(map[uint32]uint32)
		err = gob.NewDecoder(file).Decode(&g.VertexMap)
		enforce.ENFORCE(err)
		g.Vertices = make([]Vertex, len(g.VertexMap))
		for k, v := range g.VertexMap {
			g.Vertices[v] = Vertex{Id: k}
		}
	}
}

func (g *Graph) BuildMap(graphName string) {
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
				g.Vertices = append(g.Vertices, Vertex{Id: uint32(srcRaw)})
			}
			if _, ok := g.VertexMap[uint32(dstRaw)]; !ok {
				didx := uint32(len(g.VertexMap))
				g.VertexMap[uint32(dstRaw)] = didx
				g.Vertices = append(g.Vertices, Vertex{Id: uint32(dstRaw)})
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

func (g *Graph) LoadGraphStatic(graphName string) {
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
		go EdgeEnqueuer(queuechans, graphName, &enqWg, i, enqCount, deqCount, resultchan)
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

func (g *Graph) WriteVertexProps(fname string) {
	f, err := os.Create(fname)
	enforce.ENFORCE(err)
	defer f.Close()
	for vidx := range g.Vertices {
		_, err := f.WriteString(fmt.Sprintf("%d %.4f %.4f\n", g.Vertices[vidx].Id, g.Vertices[vidx].Value, g.Vertices[vidx].Residual))
		enforce.ENFORCE(err)
	}
}
