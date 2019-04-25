package metric

import (
	"fmt"
	"sort"

	"github.com/rcrowley/go-metrics"
)

// Enable enables or disables metric collection.
func Enable(b bool) func() {
	if !b {
		return func() {}
	}
	registerAll()
	return printMetrics
}

var AppendBatchSizesHistogram metrics.Histogram = metrics.NilHistogram{}
var ApplyBatchSizesHistogram metrics.Histogram = metrics.NilHistogram{}

func registerAll() {
	AppendBatchSizesHistogram = metrics.NewRegisteredHistogram(
		"append_batch_sizes",
		metrics.DefaultRegistry,
		metrics.NewUniformSample(1024),
	)
	ApplyBatchSizesHistogram = metrics.NewRegisteredHistogram(
		"apply_batch_sizes",
		metrics.DefaultRegistry,
		metrics.NewUniformSample(1024),
	)
}

func printMetrics() {
	fmt.Println(`
-------------------------------------------------
                     Metrics	 
-------------------------------------------------`)
	var names []string
	metrics.Each(func(s string, _ interface{}) {
		names = append(names, s)
	})
	sort.Strings(names)
	for _, s := range names {
		i := metrics.Get(s)
		fmt.Printf("* %s:\n", s)
		switch m := i.(type) {
		case metrics.Histogram:
			fmt.Printf("      mean: %v\n", m.Mean())
			fmt.Printf("       p50: %v\n", m.Percentile(0.50))
			fmt.Printf("       p90: %v\n", m.Percentile(0.90))
			fmt.Printf("       p99: %v\n", m.Percentile(0.99))
			fmt.Printf("    stddev: %v\n", m.StdDev())
		default:
			panic(fmt.Sprintf("unknown metric type %T", i))
		}
	}
	fmt.Println("-------------------------------------------------\n")
}
