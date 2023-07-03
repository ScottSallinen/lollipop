package main

import (
	"flag"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

func ComputeGraphColouringStat(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	maxColour := uint32(0)
	allColours := make([]uint32, 1, 64)
	g.NodeForEachVertex(func(_, _ uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
		vColour := vertex.Property.Colour
		if int(vColour) >= len(allColours) {
			allColours = append(allColours, make([]uint32, int(vColour)+1-len(allColours))...)
		}
		allColours[vColour]++
		if vColour > maxColour {
			maxColour = vColour
		}
	})
	nColours := len(allColours)
	log.Info().Msg("Colour distribution (0 to " + utils.V(nColours) + "): ")
	log.Info().Msg(utils.V(allColours))
	log.Info().Msg("Max colour: " + utils.V(maxColour) + " Number of colours: " + utils.V(nColours) + " Ratio: " + utils.V(float64(maxColour+1)/float64(nColours)))
}

func (*Colouring) OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	g.NodeForEachVertex(func(i, sidx uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
		colour := vertex.Property.Colour
		rawId := g.NodeVertexRawID(sidx)
		outDegree := uint32(len(vertex.OutEdges))
		_, tidx := graph.InternalExpand(sidx)
		inDegree := g.GraphThreads[tidx].VertexStructure(sidx).InEventPos
		if colour == EMPTY_VAL {
			log.Panic().Msg("vertex rawId " + utils.V(rawId) + " is not coloured. dg " + utils.V(outDegree) + " internalIdx: " + utils.V(sidx) + " tidx " + utils.V(tidx))
		} else if colour > outDegree && colour > inDegree {
			log.Error().Msg("vertex rawId " + utils.V(rawId) + " has a colour " + utils.V(colour) + " that is larger than its own degree: (out: " + utils.V(outDegree) + " in: " + utils.V(inDegree) + ")")
			mailbox, _ := g.NodeVertexMailbox(sidx)
			log.Error().Msg(utils.V(mailbox.Inbox.NbrScratch))
			log.Panic().Msg("")
		}
		for _, e := range vertex.OutEdges {
			didx := e.Didx
			target := g.NodeVertex(didx)
			if colour == target.Property.Colour && g.NodeVertexRawID(didx) != rawId {
				log.Error().Msg("An edge exists from vertex Source " + utils.V(sidx) + " [raw " + utils.V(rawId) + "] and Target " + utils.V(didx) + " [raw " + utils.V(g.NodeVertexRawID(didx)) + "] which have the same colour " + utils.V(colour))
				mailbox, _ := g.NodeVertexMailbox(didx)
				log.Error().Msg("Target has view of Source: " + utils.V(mailbox.Inbox.NbrScratch[e.Pos]))
				for _, te := range target.OutEdges {
					if te.Didx == sidx {
						log.Error().Msg("Found edge from target to source")
						selfMailbox, _ := g.NodeVertexMailbox(sidx)
						log.Error().Msg("Source has view of target: " + utils.V(selfMailbox.Inbox.NbrScratch[te.Pos]))
						log.Panic().Msg("")
					}
				}
				// TODO: Might have something to do with the occurrence of expired edges, which is thread-independent.
				// Since both undirected edges need to expire for the algorithm to correct itself? If only one exists, this can happen...
				// Need to investigate that further.
				log.Error().Msg("But no edge from target to source? Undirected edge not completed yet?")
			}
		}
	})

	ComputeGraphColouringStat(g)
}

// Note OnOracleCompare doesn't make much sense for this algorithm, since it is approximate.
// Though it may be interesting to see how values change over time.
// (dynamic is likely to be more stable and have less variance between points in time compared to snap-shotting).

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	useMsgStrategy := flag.Bool("msg", false, "Use direct messaging strategy instead of mailbox and merging.")
	options := graph.FlagsToOptions()
	options.Undirected = true // undirected should always be true.
	if *useMsgStrategy {
		if options.Sync {
			log.Panic().Msg("Cannot use a messaging strategy with synchronous iterations.")
		}
		graph.LaunchGraphExecution[*EPropMsg, VPropMsg, EPropMsg, MailMsg, NoteMsg](new(ColouringMsg), options)
	} else {
		graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(Colouring), options)
	}

}
