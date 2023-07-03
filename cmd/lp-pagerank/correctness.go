package main

import (
	"os"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

var oracleFile *os.File // File for oracle results

// PrintTopN: Prints the top N vertices and their scores.
func PrintTopN(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], size uint32) {
	data := make([]float64, g.NodeVertexCount())
	vIds := make([]uint32, g.NodeVertexCount())
	g.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
		data[i] = vertex.Property.Mass
		vIds[i] = v
	})
	topN := size
	if uint32(len(data)) < topN {
		topN = uint32(len(data))
	}
	res := utils.FindTopNInArray(data, topN)
	log.Info().Msg("Top N:")
	log.Info().Msg("pos,   rawId,           score")
	for i := uint32(0); i < topN; i++ {
		log.Info().Msg(utils.V(i) + "," + utils.F("%10s", g.NodeVertexRawID(vIds[res[i].First]).String()) + "," + utils.F("%16.6f", res[i].Second))
	}
}

// Performs some sanity checks for correctness.
func (*PageRank) OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	sum := 0.0
	remain := 0.0
	nEdges := 0
	singletons := 0
	g.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
		nEdges += len(vertex.OutEdges)
		sum += vertex.Property.Mass
		if vertex.Property.Mass == 0 {
			singletons++
		}
		remain += vertex.Property.InFlow
	})
	normFactor := float64(g.NodeVertexCount())
	if NORMALIZE || PPR {
		normFactor = 1
	}
	divFactor := float64(g.NodeVertexCount())
	if PPR {
		divFactor = 1
	}
	totalAbs := (sum) / normFactor // Only this value is normalized in OnFinish
	totalRemain := (remain) / divFactor
	total := totalAbs + totalRemain

	compVal := EPSILON
	if PPR {
		compVal = INITMASS * 0.1
	}
	log.Info().Msg("Total sum score: " + utils.V(totalAbs))
	if !utils.FloatEquals(total, INITMASS, compVal) {
		log.Warn().Msg("nVertices: " + utils.V(g.NodeVertexCount()) + " nEdges: " + utils.V(nEdges))
		log.Warn().Msg("Total remainder: " + utils.V(totalRemain))
		log.Warn().Msg("Total sum mass: " + utils.V(total))
		log.Panic().Msg("final mass not equal to init")
	}
}

// Compares the results of the algorithm to an oracle solution.
func (*PageRank) OnOracleCompare(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], oracle *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	ia := make([]float64, oracle.NodeVertexCount())
	ib := make([]float64, g.NodeVertexCount())
	numEdges := uint64(0)
	singletons := 0

	g.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
		ib[i] = vertex.Property.Mass
		numEdges += uint64(len(vertex.OutEdges))
		if vertex.Property.Mass == 0 { // Ignore singletons
			singletons++
		}
	})

	oracle.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
		ia[i] = vertex.Property.Mass
	})

	log.Info().Msg("V " + utils.V(uint64(g.NodeVertexCount())) + " E " + utils.V(numEdges) + " Singletons " + utils.V(singletons))
	avgL1Diff, medianL1Diff, percentile95L1 := utils.ResultCompare(ia, ib, singletons)
	log.Info().Msg("AverageL1Diff " + utils.F("%.3e", avgL1Diff) + " MedianL1Diff " + utils.F("%.3e", medianL1Diff) + " 95pL1Diff " + utils.F("%.3e", percentile95L1))

	topN := 1000
	topM := 10
	if len(ia) < topN {
		topN = len(ia)
	}
	if len(ia) < topM {
		topM = len(ia)
	}

	iaTop := utils.FindTopNInArray(ia, uint32(topN))
	ibTop := utils.FindTopNInArray(ib, uint32(topN))

	iaTopRank := make([]int, topN)
	ibTopRank := make([]int, topN)
	iamTopRank := make([]int, topM)
	ibmTopRank := make([]int, topM)
	for i := 0; i < topN; i++ {
		iaTopRank[i] = int(iaTop[i].First)
		ibTopRank[i] = int(ibTop[i].First)
	}
	for i := 0; i < topM; i++ {
		iamTopRank[i] = int(iaTop[i].First)
		ibmTopRank[i] = int(ibTop[i].First)
	}

	mRBO6 := utils.CalculateRBO(iaTopRank, ibTopRank, 0.6)
	mRBO5 := utils.CalculateRBO(iamTopRank, ibmTopRank, 0.5)
	log.Info().Msg("top" + utils.V(topN) + ".RBO6 " + utils.F("%.4f", mRBO6*100.0) + " top" + utils.V(topM) + ".RBO5 " + utils.F("%.4f", mRBO5*100.0))

	wStr := utils.F("%.3e", avgL1Diff) + utils.F("%.3e", percentile95L1) + utils.F("%.4f", mRBO6*100.0) + utils.F("%.4f", mRBO5*100.0)

	oracleFile.WriteString(wStr + "\n")

	log.Info().Msg("Given:")
	PrintTopN(g, 10)
	log.Info().Msg("Oracle:")
	PrintTopN(oracle, 10)
}
