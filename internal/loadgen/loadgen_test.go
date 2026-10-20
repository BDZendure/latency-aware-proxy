package loadgen

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestFixedRateConservationAndRecords(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer s.Close()
	c := DefaultConfig()
	c.URL = s.URL
	c.Duration = 200 * time.Millisecond
	c.Rate = 50
	c.Warmup = 40 * time.Millisecond
	c.Records = filepath.Join(t.TempDir(), "records.csv")
	got, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if got.Offered != 10 || got.Outcomes.Success != 10 || got.Warmup.Offered != 2 || got.Warmup.Outcomes.Success != 2 {
		t.Fatalf("%+v", got)
	}
	if got.Throughput != 50 || got.SuccessLatencyMS.P99 <= 0 {
		t.Fatal(got)
	}
	f, _ := os.Open(c.Records)
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil || len(rows) != 13 {
		t.Fatal(len(rows), err)
	}
	b, _ := json.Marshal(got)
	var parsed map[string]any
	json.Unmarshal(b, &parsed)
	out := parsed["outcomes"].(map[string]any)
	if len(out) != 7 {
		t.Fatal(out)
	}
}
func TestTimeoutRejectionAndMissConservation(t *testing.T) {
	for _, kind := range []string{"timeout", "rejection", "deadline", "http", "miss", "redirect", "body"} {
		t.Run(kind, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "timeout", "miss":
					time.Sleep(30 * time.Millisecond)
				case "rejection":
					w.Header().Set("X-Latency-Proxy-Error", "overload")
					w.WriteHeader(503)
				case "deadline":
					w.Header().Set("X-Latency-Proxy-Error", "deadline")
					w.WriteHeader(504)
				case "http":
					w.WriteHeader(503)
				case "redirect":
					w.Header().Set("Location", "/again")
					w.WriteHeader(302)
				case "body":
					w.Write(make([]byte, 4096))
				}
			}))
			defer s.Close()
			c := DefaultConfig()
			c.URL = s.URL
			c.Rate = 200
			c.Duration = 50 * time.Millisecond
			c.Workers = 16
			if kind == "miss" {
				c.Workers = 1
			}
			if kind == "timeout" {
				c.Timeout = time.Millisecond
			}
			if kind == "body" {
				c.MaxResponseBytes = 32
			}
			got, err := Run(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			if got.Offered != 10 || got.Outcomes.Total() != 10 {
				t.Fatal(got)
			}
			switch kind {
			case "timeout":
				if got.Outcomes.ClientTimeout == 0 {
					t.Fatal(got)
				}
			case "miss":
				if got.Outcomes.GeneratorMiss == 0 {
					t.Fatal(got)
				}
			case "rejection":
				if got.Outcomes.ProxyRejection != 10 {
					t.Fatal(got)
				}
			case "deadline":
				if got.Outcomes.ProxyTimeout != 10 {
					t.Fatal(got)
				}
			case "http":
				if got.Outcomes.HTTPError != 10 {
					t.Fatal(got)
				}
			case "redirect":
				if got.Outcomes.Success != 10 {
					t.Fatal(got)
				}
			case "body":
				if got.Outcomes.TransportError != 10 {
					t.Fatal(got)
				}
			}
		})
	}
}
func TestScheduleCostAndValidation(t *testing.T) {
	c := DefaultConfig()
	c.Duration = time.Second
	c.Rate = 10
	c.SpikeStart = 200 * time.Millisecond
	c.SpikeEnd = 400 * time.Millisecond
	c.SpikeFactor = 2
	if d := scheduledOffset(4, c, true); d != 300*time.Millisecond {
		t.Fatal(d)
	}
	for i := int64(0); i < 100; i++ {
		if cost(i, 1, .1) != cost(i, 1, .1) {
			t.Fatal("nondeterministic")
		}
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.Rate = 0 }, func(c *Config) { c.Workers = 0 }, func(c *Config) { c.HeavyFraction = 2 }, func(c *Config) { c.URL = "bad" }, func(c *Config) { c.SpikeStart = -1 }} {
		bad := c
		mutate(&bad)
		if _, err := Run(context.Background(), bad); err == nil {
			t.Fatal("accepted config")
		}
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("disk failed") }
func TestRecordsFailureAndCancellation(t *testing.T) {
	c := DefaultConfig()
	c.URL = "http://127.0.0.1:1"
	c.Duration = time.Millisecond
	c.RecordsWriter = brokenWriter{}
	if _, err := Run(context.Background(), c); err == nil {
		t.Fatal("lost write error")
	}
	c.RecordsWriter = nil
	c.Duration = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, err := Run(ctx, c)
	if !errors.Is(err, context.Canceled) || s.Outcomes.Total() != s.Offered {
		t.Fatal(s, err)
	}
}
func TestSpikeScheduleAndMissRows(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(10 * time.Millisecond); w.Write([]byte("ok")) }))
	defer s.Close()
	c := DefaultConfig()
	c.URL = s.URL
	c.Duration = 100 * time.Millisecond
	c.Rate = 100
	c.SpikeStart = 20 * time.Millisecond
	c.SpikeEnd = 40 * time.Millisecond
	c.SpikeFactor = 2
	c.Workers = 1
	c.MaxLag = 0
	c.Records = filepath.Join(t.TempDir(), "records.csv")
	summary, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Offered != 12 || summary.Outcomes.GeneratorMiss != 12 || summary.AllLatencyCount != 0 || summary.ScheduledLatencyCount != 0 {
		t.Fatal(summary)
	}
	f, err := os.Open(c.Records)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"0.000000", "10.000000", "20.000000", "25.000000", "30.000000", "35.000000", "40.000000", "50.000000", "60.000000", "70.000000", "80.000000", "90.000000"}
	for i, row := range rows[1:] {
		if row[4] != expected[i] || row[5] != "" || row[2] != "generator_miss" {
			t.Fatal(row)
		}
	}
}
func TestExplicitStartAt(t *testing.T) {
	arrivals := make(chan time.Time, 4)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { arrivals <- time.Now(); w.Write([]byte("ok")) }))
	defer s.Close()
	c := DefaultConfig()
	c.URL = s.URL
	c.StartAt = time.Now().Add(50 * time.Millisecond)
	c.Duration = 20 * time.Millisecond
	c.Rate = 100
	got, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if got.Offered != 2 || got.Outcomes.Success != 2 {
		t.Fatal(got)
	}
	for i := 0; i < 2; i++ {
		if arrival := <-arrivals; arrival.Before(c.StartAt) {
			t.Fatal("request arrived before scheduled origin", arrival, c.StartAt)
		}
	}
	if got.StartAt != c.StartAt.UTC().Format(time.RFC3339Nano) {
		t.Fatal(got.StartAt)
	}
}
func TestTransportAndBodyTimeout(t *testing.T) {
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	target := closed.URL
	closed.Close()
	c := DefaultConfig()
	c.URL = target
	c.Duration = 20 * time.Millisecond
	c.Rate = 100
	summary, err := Run(context.Background(), c)
	if err != nil || summary.Outcomes.TransportError != 2 {
		t.Fatal(summary, err)
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer s.Close()
	c.URL = s.URL
	c.Timeout = 5 * time.Millisecond
	summary, err = Run(context.Background(), c)
	if err != nil || summary.Outcomes.ClientTimeout != 2 {
		t.Fatal(summary, err)
	}
}
func TestRejectsOverlappingOutputPaths(t *testing.T) {
	c := DefaultConfig()
	c.URL = "http://localhost:1234"
	c.Output = "summary.json"
	c.Records = "./summary.json"
	if err := c.Validate(); err == nil {
		t.Fatal("JSON output would overwrite CSV records")
	}
}

func TestTinyRateOffersOnlyInitialRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer server.Close()
	for _, rate := range []float64{1e-12, math.SmallestNonzeroFloat64} {
		t.Run(strconv.FormatFloat(rate, 'g', -1, 64), func(t *testing.T) {
			config := DefaultConfig()
			config.URL = server.URL
			config.Duration = time.Millisecond
			config.Rate = rate
			config.Workers = 4
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			summary, err := Run(ctx, config)
			if err != nil || summary.Offered != 1 || summary.Outcomes.Total() != 1 {
				t.Fatalf("tiny-rate run must finish with one offer: offered=%d outcomes=%+v err=%v", summary.Offered, summary.Outcomes, err)
			}
		})
	}
}
