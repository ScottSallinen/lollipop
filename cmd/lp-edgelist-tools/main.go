package main

import (
	"bufio"
	"flag"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

func LineDequeuer(queueChan chan string, lineList *[]string, deqWg *sync.WaitGroup) {
	for qElem := range queueChan {
		*lineList = append(*lineList, qElem)
	}
	deqWg.Done()
}

func LineEnqueuer(queueChans []chan string, graphName string, undirected bool, wg *sync.WaitGroup, idx uint64, enqCount uint64, deqCount uint64, result chan uint64) {
	file := utils.OpenFile(graphName)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := uint64(0)
	mLines := uint64(0)
	for scanner.Scan() {
		lines++
		if lines%enqCount != idx {
			continue
		}
		mLines++
		lineText := scanner.Text()
		if strings.HasPrefix(lineText, "#") {
			continue
		}
		queueChans[lines%deqCount] <- lineText
	}
	result <- mLines
	wg.Done()
}

func LoadLineList(graphName string, threads int) (finalList []string) {
	qCount := utils.Max(uint64(threads), 1)
	m1 := time.Now()

	lineLists := make([][]string, threads)

	queueChans := make([]chan string, qCount)
	var deqWg sync.WaitGroup
	deqWg.Add(int(qCount))
	for i := uint64(0); i < qCount; i++ {
		queueChans[i] = make(chan string, 4096)
		go LineDequeuer(queueChans[i], &lineLists[i], &deqWg)
	}

	resultChan := make(chan uint64, qCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(qCount))
	for i := uint64(0); i < qCount; i++ {
		go LineEnqueuer(queueChans, graphName, false, &enqWg, i, qCount, qCount, resultChan)
	}
	enqWg.Wait()
	for i := uint64(0); i < qCount; i++ {
		close(queueChans[i])
	}
	close(resultChan)
	lines := uint64(0)
	for e := range resultChan {
		lines += e
	}
	deqWg.Wait()

	t1 := time.Since(m1)
	log.Info().Msg("Read " + utils.V(lines) + " lines in (ms) " + utils.V(t1.Milliseconds()))

	for i := range lineLists {
		log.Info().Msg(utils.V(i) + ":" + utils.V(len(lineLists[i])))
		finalList = append(finalList, lineLists[i]...)
	}
	return finalList
}

type IndexedStrings struct {
	Strings []string
	Idx     []int
}

func (s IndexedStrings) Swap(i, j int) {
	s.Strings[i], s.Strings[j] = s.Strings[j], s.Strings[i]
	s.Idx[i], s.Idx[j] = s.Idx[j], s.Idx[i]
}

var tsPos = 0

func (s IndexedStrings) Less(i, j int) bool {
	fields := make([]string, graph.MAX_ELEMS_PER_EDGE)
	utils.FastFields(fields, []byte(s.Strings[i]))
	ts1 := fields[tsPos]
	utils.FastFields(fields, []byte(s.Strings[j]))
	ts2 := fields[tsPos]
	return ts1 < ts2
}

func (s IndexedStrings) Len() int {
	return len(s.Strings)
}

func main() {
	gPtr := flag.String("g", "data/test.txt", "Graph file")
	sortPtr := flag.Bool("sort", false, "Sort by timestamp instead of default shuffle.")
	tPosPtr := flag.Int("pt", 2, "Absolute position of timestamp (when sorting by timestamp). Example: [src, dst, timestamp]: use 2.")
	tPtr := flag.Int("t", runtime.NumCPU(), "Thread count")
	flag.Parse()

	lineList := LoadLineList(*gPtr, *tPtr)
	rand.NewSource(time.Now().UTC().UnixNano())
	suffix := ".shuffled"

	if *sortPtr {
		tsPos = *tPosPtr
		idxStr := IndexedStrings{lineList, make([]int, len(lineList))}
		for i := range idxStr.Idx {
			idxStr.Idx[i] = i
		}
		log.Info().Msg("Sorting...")
		sort.Stable(idxStr)
		log.Info().Msg("Sorted.")
		lineList = idxStr.Strings
		suffix = ".sorted"
	} else {
		log.Info().Msg("Shuffling...")
		rand.Shuffle(len(lineList), func(i, j int) { lineList[i], lineList[j] = lineList[j], lineList[i] })
		log.Info().Msg("Shuffled.")
		suffix = ".shuffled"
	}

	log.Info().Msg("Writing lines: " + utils.V(len(lineList)))

	f := utils.OpenFile(*gPtr + suffix)

	defer f.Close()
	for line := range lineList {
		_, err := f.WriteString(lineList[line] + "\n")
		if err != nil {
			log.Fatal().Err(err).Msg("Error writing line")
		}
	}
}
