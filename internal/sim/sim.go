// Package sim implements a bounded backend with deterministic service costs.
package sim

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type Phase struct {
	StartSeconds float64 `json:"start_seconds"`
	EndSeconds   float64 `json:"end_seconds"`
	Multiplier   float64 `json:"multiplier"`
	Status       int     `json:"status"`
	Drop         bool    `json:"drop"`
}
type Config struct {
	ServiceMS          float64 `json:"service_ms"`
	JitterMS           float64 `json:"jitter_ms"`
	Capacity           int     `json:"capacity"`
	Queue              int     `json:"queue"`
	ResponseBytes      int     `json:"response_bytes"`
	Seed               int64   `json:"seed"`
	HeavyMultiplier    float64 `json:"heavy_multiplier"`
	Mode               string  `json:"mode"`
	Phases             []Phase `json:"phases"`
	HealthFollowsFault bool    `json:"health_follows_fault"`
}

func DefaultConfig() Config {
	return Config{ServiceMS: 10, Capacity: 8, Queue: 64, ResponseBytes: 128, Seed: 1, HeavyMultiplier: 10, Mode: "sleep"}
}
func finiteRange(v, lo, hi float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= lo && v <= hi
}
func (c Config) Validate() error {
	if !finiteRange(c.ServiceMS, 0, 60000) || !finiteRange(c.JitterMS, 0, 60000) || !finiteRange(c.HeavyMultiplier, 1, 1000) {
		return fmt.Errorf("invalid service, jitter, or heavy multiplier")
	}
	if c.Capacity < 1 || c.Capacity > 4096 || c.Queue < 0 || c.Queue > 65536 || c.ResponseBytes < 0 || c.ResponseBytes > 16<<20 {
		return fmt.Errorf("capacity, queue, or response size out of bounds")
	}
	if c.Mode != "sleep" && c.Mode != "cpu" {
		return fmt.Errorf("mode must be sleep or cpu")
	}
	if len(c.Phases) > 1000 {
		return fmt.Errorf("too many phases")
	}
	for i, p := range c.Phases {
		if !finiteRange(p.StartSeconds, 0, 86400*365) || !finiteRange(p.EndSeconds, p.StartSeconds, 86400*365) || p.EndSeconds <= p.StartSeconds || !finiteRange(p.Multiplier, 0, 1000) || (p.Status != 0 && (p.Status < 400 || p.Status > 599)) {
			return fmt.Errorf("invalid phase %d", i)
		}
		if i > 0 && p.StartSeconds < c.Phases[i-1].EndSeconds {
			return fmt.Errorf("phases must be ordered and nonoverlapping")
		}
	}
	return nil
}

type Stats struct {
	Requests  uint64 `json:"requests"`
	Completed uint64 `json:"completed"`
	Rejected  uint64 `json:"rejected"`
	Canceled  uint64 `json:"canceled"`
	Faulted   uint64 `json:"faulted"`
	Inflight  int64  `json:"inflight"`
	Queued    int64  `json:"queued"`
}
type Handler struct {
	config                                           Config
	start                                            time.Time
	admission                                        chan struct{}
	service                                          chan struct{}
	body                                             []byte
	requests, completed, rejected, canceled, faulted atomic.Uint64
	inflight, queued                                 atomic.Int64
}

func New(c Config) (*Handler, error) {
	return NewAt(c, time.Time{})
}

// NewAt sets the common experiment origin. A zero origin means now. Before a
// future origin, health and work use baseline behavior without phase faults.
// The absolute target is converted once to a monotonic clock deadline.
func NewAt(c Config, origin time.Time) (*Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	c.Phases = append([]Phase(nil), c.Phases...)
	start := time.Now()
	if !origin.IsZero() {
		until := time.Until(origin)
		start = time.Now().Add(until)
	}
	h := &Handler{config: c, start: start, admission: make(chan struct{}, c.Capacity+c.Queue), service: make(chan struct{}, c.Capacity), body: make([]byte, c.ResponseBytes)}
	for i := range h.body {
		h.body[i] = 'x'
	}
	return h, nil
}
func (h *Handler) Snapshot() Stats {
	return Stats{h.requests.Load(), h.completed.Load(), h.rejected.Load(), h.canceled.Load(), h.faulted.Load(), h.inflight.Load(), h.queued.Load()}
}
func (h *Handler) phase() Phase {
	elapsed := time.Since(h.start).Seconds()
	for _, p := range h.config.Phases {
		if elapsed >= p.StartSeconds && elapsed < p.EndSeconds {
			return p
		}
	}
	return Phase{Multiplier: 1}
}
func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// CPU mode performs 100,000 mixing rounds per nominal millisecond. This is a
// deterministic work budget, not a wall-clock guarantee: calibrate the nominal
// service_ms against the benchmark machine and hold GOMAXPROCS constant.
func cpuIterations(d time.Duration) uint64 { return uint64(d) / 10 }

var cpuSink atomic.Uint64

func (h *Handler) serviceDuration(id int64, heavy bool, mult float64) time.Duration {
	jitter := (float64(mix(uint64(id)^uint64(h.config.Seed))>>11)/float64(uint64(1)<<53)*2 - 1) * h.config.JitterMS
	ms := math.Max(0, h.config.ServiceMS+jitter)
	if heavy {
		ms *= h.config.HeavyMultiplier
	}
	if mult > 0 {
		ms *= mult
	}
	return time.Duration(ms * float64(time.Millisecond))
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := h.phase()
	switch r.URL.Path {
	case "/healthz":
		if h.config.HealthFollowsFault && (p.Drop || p.Status != 0 || p.Multiplier > 1) {
			http.Error(w, "fault", 503)
			return
		}
		w.WriteHeader(200)
		return
	case "/stats":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(h.Snapshot())
		return
	}
	h.requests.Add(1)
	id := int64(0)
	if raw := r.URL.Query().Get("id"); raw != "" {
		var err error
		id, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "invalid id", 400)
			return
		}
	}
	if p.Drop {
		h.faulted.Add(1)
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
				return
			}
		}
		http.Error(w, "connection drop unavailable", 503)
		return
	}
	if p.Status != 0 {
		h.faulted.Add(1)
		http.Error(w, "injected fault", p.Status)
		return
	}
	select {
	case h.admission <- struct{}{}:
		defer func() { <-h.admission }()
	default:
		h.rejected.Add(1)
		http.Error(w, "queue full", 503)
		return
	}
	h.queued.Add(1)
	select {
	case h.service <- struct{}{}:
		h.queued.Add(-1)
		defer func() { <-h.service }()
	case <-r.Context().Done():
		h.queued.Add(-1)
		h.canceled.Add(1)
		return
	}
	h.inflight.Add(1)
	defer h.inflight.Add(-1)
	d := h.serviceDuration(id, r.URL.Query().Get("cost") == "heavy", p.Multiplier)
	if h.config.Mode == "cpu" {
		var x uint64 = 1
		for i, n := uint64(0), cpuIterations(d); i < n; i++ {
			if i%1024 == 0 && r.Context().Err() != nil {
				h.canceled.Add(1)
				return
			}
			x = mix(x)
		}
		cpuSink.Store(x)
	} else {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			h.canceled.Add(1)
			return
		}
	}
	if r.Context().Err() != nil {
		h.canceled.Add(1)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(h.body)))
	if _, err := w.Write(h.body); err == nil {
		h.completed.Add(1)
	}
}
