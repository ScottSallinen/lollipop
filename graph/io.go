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

type QueueElem struct {
	SrcRaw uint32
	DstRaw uint32
}

func (g *Graph) EdgeDequeuer(queuechan chan QueueElem, deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		srcIdx, _ := g.VertexMap[uint32(qElem.SrcRaw)]
		dstIdx, _ := g.VertexMap[uint32(qElem.DstRaw)]

		g.Vertices[srcIdx].OutEdges = append(g.Vertices[srcIdx].OutEdges, Edge{Target: uint32(dstIdx)}) //, Prop: 0.0
	}
	deqWg.Done()
	return
}

func (g *Graph) EdgeEnqueuer(queuechans []chan QueueElem, graphName string, wg *sync.WaitGroup, idx uint32, enqCount uint32, deqCount uint32, result chan uint32) {
	file, err := os.Open(graphName)
	enforce.ENFORCE(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := uint32(0)
	mLines := uint32(0)
	for scanner.Scan() {
		lines++
		if lines%enqCount != idx {
			continue
		}
		mLines++
		t := scanner.Text()
		if strings.HasPrefix(t, "#") {
			continue
		}
		s := strings.Fields(t)
		enforce.ENFORCE(len(s) == 2 || len(s) == 3)
		src, _ := strconv.Atoi(s[0])
		dst, _ := strconv.Atoi(s[1])

		//if src == dst {
		//	continue
		//}

		queuechans[uint32(src)%deqCount] <- QueueElem{uint32(src), uint32(dst)}
	}
	result <- mLines
	wg.Done()
	return
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

	work := make(chan QueueElem, 256)
	go func(mWork chan QueueElem) {
		for elem := range mWork {
			srcRaw := elem.SrcRaw
			dstRaw := elem.DstRaw
			if sidx, ok := g.VertexMap[uint32(srcRaw)]; !ok {
				sidx = uint32(len(g.VertexMap))
				g.VertexMap[uint32(srcRaw)] = sidx
				g.Vertices = append(g.Vertices, Vertex{Id: uint32(srcRaw)})
			}
			if didx, ok := g.VertexMap[uint32(dstRaw)]; !ok {
				didx = uint32(len(g.VertexMap))
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

		work <- QueueElem{uint32(src), uint32(dst)}
	}
	close(work)
	return
}

func (g *Graph) LoadGraphStatic(graphName string) {
	deqCount := mathutils.Max(uint32(THREADS), 1)
	enqCount := mathutils.Max(uint32(THREADS/2), 1)

	m0 := time.Now()
	g.VertexMap = make(map[uint32]uint32)

	g.LoadVertexMap(graphName)
	t0 := time.Since(m0)
	info("Built map (ms) ", t0.Milliseconds())
	m1 := time.Now()

	queuechans := make([]chan QueueElem, deqCount)
	var deqWg sync.WaitGroup
	deqWg.Add(int(deqCount))
	for i := uint32(0); i < deqCount; i++ {
		queuechans[i] = make(chan QueueElem, 256)
		go g.EdgeDequeuer(queuechans[i], &deqWg)
	}

	resultchan := make(chan uint32, enqCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(enqCount))
	for i := uint32(0); i < enqCount; i++ {
		go g.EdgeEnqueuer(queuechans, graphName, &enqWg, i, enqCount, deqCount, resultchan)
	}
	enqWg.Wait()
	for i := uint32(0); i < deqCount; i++ {
		close(queuechans[i])
	}
	close(resultchan)
	lines := uint32(0)
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
		_, err := f.WriteString(fmt.Sprintf("%d %.4f %.4f\n", g.Vertices[vidx].Id, g.Vertices[vidx].Properties.Value, g.Vertices[vidx].Properties.Residual))
		enforce.ENFORCE(err)
	}
}
