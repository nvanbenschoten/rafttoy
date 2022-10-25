package metric

import (
	"fmt"
	"sort"
	"time"

	"github.com/rcrowley/go-metrics"
)

var enabled bool

// Enable enables or disables metric collection.
func Enable(b bool) func() {
	if !b {
		enabled = false
		return func() {}
	}
	enabled = true
	registerAll()
	return printMetrics
}

// Enabled returns whether metric collection is enabled.
func Enabled() bool {
	return enabled
}

// AppendBatchSizesHistogram records the size of Raft entry batches
// as they are appended to the Raft log.
var AppendBatchSizesHistogram metrics.Histogram = metrics.NilHistogram{}

// ApplyBatchSizesHistogram records the size of Raft entry batches
// as they are applied to the storage engine.
var ApplyBatchSizesHistogram metrics.Histogram = metrics.NilHistogram{}

// PipelineLatencyHistogram records the latency of single Raft proposal
// pipeline iterations.
var PipelineLatencyHistogram metrics.Histogram = metrics.NilHistogram{}

// AppendLatencyHistogram records the latency of a log append batch.
var AppendLatencyHistogram metrics.Histogram = metrics.NilHistogram{}

// ApplyLatencyHistogram records the latency of a log apply batch.
var ApplyLatencyHistogram metrics.Histogram = metrics.NilHistogram{}

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
	PipelineLatencyHistogram = metrics.NewRegisteredHistogram(
		"pipeline_latency_us",
		metrics.DefaultRegistry,
		metrics.NewUniformSample(1024),
	)
	AppendLatencyHistogram = metrics.NewRegisteredHistogram(
		"append_latency_us",
		metrics.DefaultRegistry,
		metrics.NewUniformSample(1024),
	)
	ApplyLatencyHistogram = metrics.NewRegisteredHistogram(
		"apply_latency_us",
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
	fmt.Printf("-------------------------------------------------\n\n")
}

// MeasureLat measures the delay between the time that the function is first
// called and the time that the returned closure is called. This value is
// recorded into the provided histogram.
//
// Suggested use looks like:
//
//	defer metric.MeasureLat(metric.AppendBatchSizesHistogram)()
func MeasureLat(h metrics.Histogram) func() {
	if !Enabled() {
		return func() {}
	}
	start := time.Now()
	return func() {
		lat := time.Since(start)
		h.Update(int64(lat / time.Microsecond))
	}
}
