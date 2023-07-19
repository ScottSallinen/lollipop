package main

import (
	"flag"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	plotPtr := flag.Bool("plot", false, "Provide the timeseries the top N vertices at a query, and provide everything to plot a top-N-vertex-over-time graph. \n(Note this adds un-optimized logging overhead.)")
	plotdtPtr := flag.Bool("plotdt", false, "Provide the timeseries the top N vertices at a query, which will provide a derivative: rate-of-change in top-N-vertex-over-time. \n(Note this adds un-optimized logging overhead.)")
	graphOptions := graph.FlagsToOptions()

	logWaterfall = *plotPtr || *plotdtPtr
	logDerivative = *plotdtPtr

	graphOptions.AllowAsyncVertexProps = true // True for pagerank
	graphOptions.NoConvergeForQuery = true    // True for pagerank

	// Only for PPR. Default is nil.
	var initMsgs map[graph.RawType]Mail

	if PPR { // PPR Testing stuff
		// wikipedia-growth example
		interestArray := []int{73, 259, 9479, 6276, 1710, 864, 2169, 110, 10312, 69, 425, 611, 1566, 11297, 1916, 1002, 975, 6413, 526, 5079, 1915, 11956, 2034, 956, 208, 15, 77041, 652, 20, 1352, 1918, 388, 1806, 1920, 3517, 863, 1594, 24772, 2008, 78349, 397, 1923, 1105, 8707, 7, 4336, 1753, 205, 17, 984, 5732, 983, 70, 1924, 111, 51076, 6903, 4083, 1936, 1115, 154942, 1550, 2266, 179, 1933, 37976, 2844, 57028, 1934, 1932, 84204, 1931, 490, 1935, 2312, 1925, 1846, 5081, 1930, 4378, 1917, 68, 3080, 2734, 435, 1482, 1929, 1922, 4104, 2814, 1926, 1919, 1164, 1110, 1928, 2843, 4364, 1921, 4148, 2041}
		initMap := make(map[graph.RawType]Mail)
		for _, v := range interestArray {
			initMap[graph.AsRawType(v)] = Mail{INITMASS / float64(len(interestArray))}
		}
		initMsgs = initMap
	}

	if graphOptions.OracleCompare || graphOptions.OracleCompareSync {
		oracleFile := utils.CreateFile("results/oracleComp.csv")
		oracleFile.WriteString("AvgL1,P95L1,1000.RBO6,10.RBO5\n")
		defer oracleFile.Close()
	}

	g := graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(PageRank), graphOptions, initMsgs, nil)

	if graphOptions.LogTimeseries {
		PrintTimeSeries(true, false)
	}
	PrintTopN(g, 10)
}
