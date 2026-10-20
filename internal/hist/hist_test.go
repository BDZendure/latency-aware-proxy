package hist

import (
	"testing"
	"time"
)

func TestQuantilesAndExtremes(t *testing.T) {
	var h Histogram
	if h.Quantile(.99) != 0 {
		t.Fatal("empty")
	}
	for _, v := range []time.Duration{0, time.Microsecond, time.Millisecond, time.Second, time.Duration(1<<63 - 1)} {
		h.Record(v)
	}
	if h.Count() != 5 {
		t.Fatal(h.Count())
	}
	if q := h.Quantile(.5); q < 1 || q > 1.01 {
		t.Fatal(q)
	}
	if h.Quantile(1) < float64(time.Duration(1<<63-1))/float64(time.Millisecond) {
		t.Fatal("overflow")
	}
}
func TestHistogramPrecisionAndRank(t *testing.T) {
	for _, d := range []time.Duration{time.Nanosecond, 5 * time.Nanosecond, 17 * time.Microsecond, 10 * time.Millisecond, 24 * time.Hour, time.Duration(1<<63 - 1)} {
		var h Histogram
		h.Record(d)
		q := h.Quantile(.5) * float64(time.Millisecond)
		if q < float64(d)-1 || q > float64(d)*1.01+1 {
			t.Fatalf("duration %v quantile ns %f", d, q)
		}
	}
	var h Histogram
	for i := 0; i < 99; i++ {
		h.Record(time.Millisecond)
	}
	h.Record(time.Second)
	if h.Quantile(.99) > 1.01 || h.Quantile(1) < 1000 {
		t.Fatal("quantile rank")
	}
}
