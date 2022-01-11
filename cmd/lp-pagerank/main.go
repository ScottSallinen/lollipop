package main

import (
	"flag"
	"log"
	"math"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
)

// DAMPINGFACTOR Damping Factor
const DAMPINGFACTOR = float64(0.85)
const EPSILON = float64(0.001)
const PRINTPROPS = false

func OnInit(g *graph.Graph, data interface{}) error {
	for vidx := range g.Vertices {
		g.Vertices[vidx].Properties.Residual = 0.0
		g.Vertices[vidx].Properties.Latent = 0.0
		g.Vertices[vidx].Properties.Value = 0.0
		g.Vertices[vidx].Scratch = 1.0
	}

	return nil
}

func OnVisitVertex(g *graph.Graph, vidx uint32, data interface{}) int {
	vertex := &g.Vertices[vidx]
	vertex.Properties.Residual += data.(float64)

	toDistribute := DAMPINGFACTOR * (vertex.Properties.Residual)
	toAbsorb := (1.0 - DAMPINGFACTOR) * (vertex.Properties.Residual)

	myDelta := math.Abs(toAbsorb / (vertex.Properties.Value - toAbsorb))
	if myDelta > EPSILON {
		vertex.Properties.Value += toAbsorb
		vertex.Properties.Residual = 0.0

		if len(vertex.OutEdges) > 0 {
			distribute := toDistribute / float64(len(vertex.OutEdges))
			for eidx := range vertex.OutEdges {
				target := vertex.OutEdges[eidx].Target
				g.OnQueueVisit(g, vidx, target, distribute)
			}
		} else {
			vertex.Properties.Latent += toDistribute
		}
		return len(vertex.OutEdges)
	}
	return 0
}

func OnFini(g *graph.Graph, data interface{}) error {
	/// fix sink nodes -- grouped sinks
	numSinks := 0
	globalLatent := 0.0
	nonSinkSum := 0.0

	/// One pass over all vertices -- compute some global totals.
	for vidx := range g.Vertices {
		if len(g.Vertices[vidx].OutEdges) == 0 { /// Sink vertex
			globalLatent += g.Vertices[vidx].Properties.Latent
			numSinks++
		} else {
			nonSinkSum += g.Vertices[vidx].Properties.Value
		}
	}
	/// The total value in the non-sink graph.

	//log.Println(numSinks, globalLatent, nonSinkSum)

	/// Note: the amount latent here was already pre-dampened, so the retainment percent must be computed by the raw mass, so we undampen for that calculation (multiply by 1/d).
	/// The subtraction of 1.0*sinks is because we discount each sink node's contribution of 1u of mass from the amount latent.
	/// We divide by the size of the non-sink graph for the final retainment percent.
	retainSumPct := ((globalLatent * (1.0 / DAMPINGFACTOR)) - 1.0*float64(numSinks)) / float64((len(g.Vertices) - numSinks))

	SinkQuota := float64(1.0) / float64(len(g.Vertices)-1)
	NormalQuota := float64(len(g.Vertices)-numSinks) / float64(len(g.Vertices)-1)

	geometricLatentSum := globalLatent / (1.0 - DAMPINGFACTOR*(SinkQuota*(float64(numSinks-1))+(NormalQuota*retainSumPct)))

	/// One pass over all vertices -- make adjustment based on sink/non-sink status.
	for vidx := range g.Vertices {
		var toAbsorb float64
		if len(g.Vertices[vidx].OutEdges) != 0 {
			/// All vertices that are NOT a sink node
			toAbsorb = (NormalQuota) * (geometricLatentSum * (1.0 - retainSumPct)) * (g.Vertices[vidx].Properties.Value / nonSinkSum)
		} else {
			/// All vertices that are a sink node
			/// Relative 'power' of this sink compared to others determines its retainment. Note: we undampen for this ratio as well.
			relativeSinkPowerPct := (g.Vertices[vidx].Properties.Latent*(1.0/DAMPINGFACTOR) - 1.0) / ((globalLatent * (1.0 / DAMPINGFACTOR)) - float64(numSinks)*1.0)
			toAbsorb = (1.0 - DAMPINGFACTOR) * (SinkQuota) * (geometricLatentSum) * (1.0 - g.Vertices[vidx].Properties.Latent/globalLatent)
			toAbsorb += (1.0 - DAMPINGFACTOR) * (NormalQuota) * (geometricLatentSum) * (retainSumPct) * (relativeSinkPowerPct)
		}
		g.Vertices[vidx].Properties.Value += toAbsorb
		//g.Vertices[vidx].Properties.Value /= float64(len(g.Vertices))
	}

	if PRINTPROPS {
		g.PrintVertexProps("fff : ")
		g.PrintVertexPropsNorm("fff : ", float64(len(g.Vertices))) //float64(len(g.Vertices))
	}

	return nil
}

func OnCheckCorrectness(g *graph.Graph) error {
	sum := 0.0
	resid := 0.0
	for vidx := range g.Vertices {
		sum += g.Vertices[vidx].Properties.Value
		resid += g.Vertices[vidx].Properties.Residual
	}
	totalAbs := (sum) / float64(len(g.Vertices))
	totalResid := (resid) / float64(len(g.Vertices))
	total := totalAbs + totalResid
	log.Println("Total absorbed: \t", totalAbs)
	log.Println("Total residual: \t", totalResid)
	log.Println("Total mass: \t", total)

	enforce.ENFORCE(total > 0.999 && total < 1.001)
	return nil
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	flag.Parse()
	gName := *gptr
	doAsync := *aptr
	g := graph.LoadGraph(gName)

	frame := framework.Framework{}
	frame.OnInit = OnInit
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFini = OnFini
	frame.OnCheckCorrectness = OnCheckCorrectness

	//g.ComputeInEdges()
	g.ComputeGraphStats(false, false)

	frame.Run(g, doAsync)

	g.WriteVertexProps("results/props-c.txt")
}
