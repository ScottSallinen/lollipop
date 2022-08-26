package common

import (
	"fmt"
	"math/rand"
	"os"
	"strings"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

func ExtractGraphName(graphFilename string) (graphName string) {
	gNameMainT := strings.Split(graphFilename, "/")
	gNameMain := gNameMainT[len(gNameMainT)-1]
	gNameMainTD := strings.Split(gNameMain, ".")
	if len(gNameMainTD) > 1 {
		return gNameMainTD[len(gNameMainTD)-2]
	} else {
		return gNameMainTD[0]
	}
}

func ShuffleSC[EdgeProp any](sc []graph.StructureChange[EdgeProp]) {
	for i := range sc {
		j := rand.Intn(i + 1)
		sc[i], sc[j] = sc[j], sc[i]
	}
}

func InjectDeletesRetainFinalStructure[EdgeProp any](sc []graph.StructureChange[EdgeProp], chance float64) []graph.StructureChange[EdgeProp] {
	availableAdds := make([]graph.StructureChange[EdgeProp], len(sc))
	var previousAdds []graph.StructureChange[EdgeProp]
	var returnSC []graph.StructureChange[EdgeProp]

	copy(availableAdds, sc)
	ShuffleSC(availableAdds)

	for len(availableAdds) > 0 {
		if len(previousAdds) > 0 && rand.Float64() < chance {
			// chance for del
			ShuffleSC(previousAdds)
			idx := len(previousAdds) - 1
			injDel := graph.StructureChange[EdgeProp]{Type: graph.DEL, SrcRaw: previousAdds[idx].SrcRaw, DstRaw: previousAdds[idx].DstRaw, EdgeProperty: previousAdds[idx].EdgeProperty}
			returnSC = append(returnSC, injDel)
			availableAdds = append(availableAdds, previousAdds[idx])
			previousAdds = previousAdds[:idx]
		} else {
			ShuffleSC(availableAdds)
			idx := len(availableAdds) - 1
			returnSC = append(returnSC, availableAdds[idx])
			previousAdds = append(previousAdds, availableAdds[idx])
			availableAdds = availableAdds[:idx]
		}
	}
	return returnSC
}

func WriteVertexProps[VertexProp any](g *graph.Graph[VertexProp], graphName string, dynamic bool) {
	var resName string
	if dynamic {
		resName = "dynamic"
	} else {
		resName = "static"
	}
	filename := "results/" + graphName + "-props-" + resName + ".txt"

	f, err := os.Create(filename)
	enforce.ENFORCE(err)
	defer f.Close()

	for vidx := range g.Vertices {
		_, err := f.WriteString(fmt.Sprintf("%d - %v\n", g.Vertices[vidx].Id, &g.Vertices[vidx].Property))
		enforce.ENFORCE(err)
	}
}
