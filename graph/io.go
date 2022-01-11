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
)

type QueueElem struct {
	src uint32
	dst uint32
}

/*
func (g *Graph) EdgeConsumeLocking(src uint32, dst uint32) {
	srcIdx := uint32(0)
	dstIdx := uint32(0)

	g.GraphLock.RLock()
	srcIdx, srcOk := g.VertexMap[uint32(src)]
	dstIdx, dstOk := g.VertexMap[uint32(dst)]
	g.GraphLock.RUnlock()

	if !srcOk || !dstOk {
		g.GraphLock.Lock()
		srcIdx, srcOk = g.VertexMap[uint32(src)]
		if !srcOk {
			srcIdx = uint32(len(g.VertexMap))
			g.VertexMap[uint32(src)] = srcIdx
			g.Vertices = append(g.Vertices, Vertex{Id: uint32(src)})
		}
		dstIdx, dstOk = g.VertexMap[uint32(dst)]
		if !dstOk {
			dstIdx = uint32(len(g.VertexMap))
			g.VertexMap[uint32(dst)] = dstIdx
			g.Vertices = append(g.Vertices, Vertex{Id: uint32(dst)})
		}
		g.GraphLock.Unlock()
	}

	g.GraphLock.RLock()
	//g.Vertices[srcIdx].Mu.Lock()
	g.Vertices[srcIdx].OutEdges = append(g.Vertices[srcIdx].OutEdges, Edge{Target: uint32(dstIdx), Prop: 0.0})
	//g.Vertices[srcIdx].Mu.Unlock()
	g.GraphLock.RUnlock()
}
*/

func (g *Graph) EdgeConsume(src uint32, dst uint32) {
	srcIdx, _ := g.VertexMap[uint32(src)]
	dstIdx, _ := g.VertexMap[uint32(dst)]

	g.Vertices[srcIdx].OutEdges = append(g.Vertices[srcIdx].OutEdges, Edge{Target: uint32(dstIdx)}) //, Prop: 0.0
}

func (g *Graph) EdgeDequeuer(queuechan chan QueueElem) {
	for qElem := range queuechan {
		g.EdgeConsume(qElem.src, qElem.dst)
	}
}

func (g *Graph) EdgeEnqueuer(queuechans []chan QueueElem, graphName string, wg *sync.WaitGroup, idx uint32, eqCount uint32, dqCount uint32, result chan uint32) {
	file, err := os.Open(graphName)
	enforce.ENFORCE(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := uint32(0)
	mLines := uint32(0)
	for scanner.Scan() {
		lines++
		if lines%eqCount != idx {
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

		queuechans[uint32(src)%dqCount] <- QueueElem{uint32(src), uint32(dst)}
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
			src := elem.src
			dst := elem.dst
			if srcIdx, ok := g.VertexMap[uint32(src)]; !ok {
				srcIdx = uint32(len(g.VertexMap))
				g.VertexMap[uint32(src)] = srcIdx
				g.Vertices = append(g.Vertices, Vertex{Id: uint32(src)})
			}
			if dstIdx, ok := g.VertexMap[uint32(dst)]; !ok {
				dstIdx = uint32(len(g.VertexMap))
				g.VertexMap[uint32(dst)] = dstIdx
				g.Vertices = append(g.Vertices, Vertex{Id: uint32(dst)})
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

func LoadGraph(graphName string) *Graph {
	m0 := time.Now()
	g := &Graph{}
	g.VertexMap = make(map[uint32]uint32)

	g.LoadVertexMap(graphName)
	t0 := time.Since(m0)
	info("Built map in ", t0)
	m1 := time.Now()

	dqCount := uint32(32)

	queuechans := make([]chan QueueElem, dqCount)
	for i := uint32(0); i < dqCount; i++ {
		queuechans[i] = make(chan QueueElem, 256)
		go g.EdgeDequeuer(queuechans[i])
	}

	eqCount := uint32(16)
	resultchan := make(chan uint32, eqCount)
	var eqWg sync.WaitGroup
	eqWg.Add(int(eqCount))
	for i := uint32(0); i < eqCount; i++ {
		go g.EdgeEnqueuer(queuechans, graphName, &eqWg, i, eqCount, dqCount, resultchan)
	}
	eqWg.Wait()
	for i := uint32(0); i < dqCount; i++ {
		close(queuechans[i])
	}
	close(resultchan)
	lines := uint32(0)
	for e := range resultchan {
		lines += e
	}

	t1 := time.Since(m1)
	info("Read ", lines, " edges in ", t1)
	return g
}

func (g *Graph) WriteVertexProps(fname string) {
	f, err := os.Create(fname)
	enforce.ENFORCE(err)
	defer f.Close()
	for vidx := range g.Vertices {
		_, err := f.WriteString(fmt.Sprintf("%d %.3f\n", g.Vertices[vidx].Id, g.Vertices[vidx].Properties.Value))
		enforce.ENFORCE(err)
	}
}
