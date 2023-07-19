package k

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

func resetHeights(g *Graph) {
	g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) (accumulated int) {
		for i := 0; i < len(gt.Vertices); i++ {
			v := &gt.Vertices[i].Property
			v.Height = MaxHeight
			for j := range v.Nbrs {
				v.Nbrs[j].Height = MaxHeight
			}
		}
		return 0
	})

	_, source := g.NodeVertexFromRaw(SourceRawId)
	source.Property.Height = VertexCountHelper.GetMaxVertexCount()
	source.Property.HeightChanged = true
	source.Property.UnknownPosCount = math.MaxUint32 - 1

	_, sink := g.NodeVertexFromRaw(SinkRawId)
	sink.Property.Height = 0
	sink.Property.HeightChanged = true
	sink.Property.UnknownPosCount = math.MaxUint32 - 1
}

func sendMsgToActiveVertices(g *Graph) (activeCount int) {
	return g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) (accumulated int) {
		for i := 0; i < len(gt.Vertices); i++ {
			v := &gt.Vertices[i].Property
			if v.Excess > 0 {
				v.UnknownPosCount = math.MaxUint32 - 1

				id := threadOffset | uint32(i)
				mailbox, tidx := g.NodeVertexMailbox(id)
				g.GraphThreads[tidx].MsgSend += g.EnsureSend(g.ActiveNotification(id, graph.Notification[Note]{Target: id}, mailbox, tidx))

				accumulated++
			}
		}
		return accumulated
	})
}
