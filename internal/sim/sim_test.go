package sim

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDefaultsAndValidation(t *testing.T) {
	c := DefaultConfig()
	if c.ServiceMS != 10 || c.Capacity != 8 || c.Queue != 64 || c.ResponseBytes != 128 {
		t.Fatal(c)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.Capacity = 0 }, func(c *Config) { c.Queue = -1 }, func(c *Config) { c.ResponseBytes = 1 << 30 }, func(c *Config) { c.ServiceMS = -1 }, func(c *Config) { c.Phases = []Phase{{StartSeconds: 2, EndSeconds: 1}} }} {
		bad := c
		mutate(&bad)
		if _, err := New(bad); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
}
func TestWorkHealthFaultAndStats(t *testing.T) {
	c := DefaultConfig()
	c.ServiceMS = 0
	c.ResponseBytes = 321
	h, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(h)
	defer s.Close()
	r, err := http.Get(s.URL + "/any?cost=heavy&id=2")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if len(b) != 321 {
		t.Fatal(len(b))
	}
	r, _ = http.Get(s.URL + "/stats")
	var stats Stats
	json.NewDecoder(r.Body).Decode(&stats)
	r.Body.Close()
	if stats.Completed != 1 || stats.Inflight != 0 {
		t.Fatal(stats)
	}
	c.Phases = []Phase{{StartSeconds: 0, EndSeconds: 10, Multiplier: 10, Status: 502}}
	h, _ = New(c)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/work", nil))
	if w.Code != 502 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	c.HealthFollowsFault = true
	h, _ = New(c)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}
func TestCapacityOverflowAndCancellation(t *testing.T) {
	c := DefaultConfig()
	c.ServiceMS = 1000
	c.Capacity = 1
	c.Queue = 0
	h, _ := New(c)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/work", nil).WithContext(ctx))
	}()
	deadline := time.Now().Add(time.Second)
	for h.Snapshot().Inflight != 1 {
		if time.Now().After(deadline) {
			t.Fatal("not started")
		}
		time.Sleep(time.Millisecond)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/work", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not cancel")
	}
	if h.Snapshot().Inflight != 0 {
		t.Fatal(h.Snapshot())
	}
}
func TestJitterIsDeterministic(t *testing.T) {
	c := DefaultConfig()
	c.JitterMS = 5
	h, _ := New(c)
	a := h.serviceDuration(42, false, 1)
	if a != h.serviceDuration(42, false, 1) || a < 5*time.Millisecond || a > 15*time.Millisecond {
		t.Fatal(a)
	}
	if delta := h.serviceDuration(42, true, 1) - a*10; delta < 0 || delta > 10 {
		t.Fatal("heavy cost", delta)
	}
}
func TestCPUWorkHasDeterministicIterationBudget(t *testing.T) {
	if cpuIterations(10*time.Millisecond) != 1000000 || cpuIterations(100*time.Millisecond) != 10000000 {
		t.Fatal("CPU work budget must scale by nominal service cost")
	}
	c := DefaultConfig()
	c.Mode = "cpu"
	c.ServiceMS = 60000
	h, _ := New(c)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/work", nil).WithContext(ctx))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("CPU loop ignored cancellation")
	}
}
func TestQueuedCancellationAndDroppedConnection(t *testing.T) {
	c := DefaultConfig()
	c.ServiceMS = 60000
	c.Capacity = 1
	c.Queue = 1
	h, _ := New(c)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	done1 := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/work", nil).WithContext(ctx1))
		close(done1)
	}()
	deadline := time.Now().Add(time.Second)
	for h.Snapshot().Inflight != 1 {
		if time.Now().After(deadline) {
			t.Fatal("not started")
		}
		time.Sleep(time.Millisecond)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/work", nil).WithContext(ctx2))
		close(done2)
	}()
	for h.Snapshot().Queued != 1 {
		if time.Now().After(deadline) {
			t.Fatal("not queued")
		}
		time.Sleep(time.Millisecond)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/work", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	cancel2()
	select {
	case <-done2:
	case <-time.After(time.Second):
		t.Fatal("queue cancellation ignored")
	}
	cancel1()
	<-done1
	if h.Snapshot().Queued != 0 || h.Snapshot().Inflight != 0 {
		t.Fatal(h.Snapshot())
	}
	c.Phases = []Phase{{StartSeconds: 0, EndSeconds: 10, Drop: true}}
	h, _ = New(c)
	s := httptest.NewServer(h)
	defer s.Close()
	if r, err := http.Get(s.URL + "/work"); err == nil {
		r.Body.Close()
		t.Fatal("drop returned HTTP response")
	}
}
func TestExplicitPhaseOrigin(t *testing.T) {
	c := DefaultConfig()
	c.ServiceMS = 0
	c.Phases = []Phase{{StartSeconds: 0, EndSeconds: 10, Status: 502}}
	future, err := NewAt(c, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	future.ServeHTTP(w, httptest.NewRequest("GET", "/work", nil))
	if w.Code != 200 {
		t.Fatal("pre-start baseline", w.Code)
	}
	active, err := NewAt(c, time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	active.ServeHTTP(w, httptest.NewRequest("GET", "/work", nil))
	if w.Code != 502 {
		t.Fatal("phase origin ignored", w.Code)
	}
}

func TestMaximumCombinedServiceDurationFits(t *testing.T) {
	config := DefaultConfig()
	config.ServiceMS = 60000
	config.JitterMS = 60000
	config.HeavyMultiplier = 1000
	config.Phases = []Phase{{StartSeconds: 0, EndSeconds: 1, Multiplier: 1000}}
	handler, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	const maximum = 120000000000 * time.Millisecond
	for id := int64(0); id < 1000; id++ {
		duration := handler.serviceDuration(id, true, 1000)
		if duration < 0 || duration > maximum {
			t.Fatalf("service duration overflow: %v", duration)
		}
	}
	if maximum <= 0 || maximum >= time.Duration(math.MaxInt64) {
		t.Fatal("combined service bound exceeds duration range")
	}
}
