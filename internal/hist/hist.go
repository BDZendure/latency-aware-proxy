// Package hist provides bounded-memory latency histograms. Quantiles are upper
// bucket boundaries with at most 1% relative error plus one nanosecond. Histograms
// are not safe for concurrent mutation; callers must serialize recording.
package hist

import (
	"math"
	"time"
)

const buckets = 4500

var logStep = math.Log(1.01)

type Histogram struct {
	counts [buckets]uint64
	count  uint64
}

func (h *Histogram) Record(d time.Duration) {
	if d < 0 {
		d = 0
	}
	i := int(math.Ceil(math.Log1p(float64(d)) / logStep))
	if i >= buckets {
		i = buckets - 1
	}
	h.counts[i]++
	h.count++
}
func (h *Histogram) Count() uint64 { return h.count }

// Quantile returns milliseconds. Empty histograms return zero.
func (h *Histogram) Quantile(q float64) float64 {
	if h.count == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	rank := uint64(math.Ceil(q * float64(h.count)))
	if rank == 0 {
		rank = 1
	}
	var n uint64
	for i, c := range h.counts {
		n += c
		if n >= rank {
			return math.Min(math.Expm1(float64(i)*logStep), float64(math.MaxInt64)) / float64(time.Millisecond)
		}
	}
	return 0
}
