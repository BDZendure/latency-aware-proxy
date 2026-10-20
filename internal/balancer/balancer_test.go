package balancer

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestReferenceRecurrence(t *testing.T) {
	now := time.Now()
	e := estimate{value: float64(50 * time.Millisecond), stamp: now}
	e.observe(now, float64(200*time.Millisecond), 10*time.Second)
	if e.value != float64(200*time.Millisecond) {
		t.Fatal("peak not immediate", e.value)
	}
	e.observe(now.Add(time.Second), float64(10*time.Millisecond), 10*time.Second)
	want := float64(200*time.Millisecond)*math.Exp(-0.1) + float64(10*time.Millisecond)*(1-math.Exp(-0.1))
	if math.Abs(e.value-want) > 1e-6 {
		t.Fatalf("got %v want %v", e.value, want)
	}
	e.observe(now.Add(time.Second), 1, 10*time.Second)
	if math.Abs(e.value-want) > 1e-6 {
		t.Fatal("zero elapsed must preserve lower estimate")
	}
	e.observe(now.Add(11*time.Second), 0, 10*time.Second)
	if math.Abs(e.value-want/math.E) > 1e-6 {
		t.Fatal("idle decay", e.value)
	}
	e.observe(now, e.value+1, 10*time.Second)
	if e.value != want/math.E+1 {
		t.Fatal("backward clock peak")
	}
}

func config(policy string) Config {
	return Config{Policy: policy, Backends: []string{"a", "b", "c"}, Limit: 2, Cold: 50 * time.Millisecond, Tau: 10 * time.Second, ErrorPenalty: time.Second, TimeoutPenalty: time.Second, FailureThreshold: 3, RecoveryThreshold: 2, Seed: 1}
}

func TestRoundRobinCapacityAndIdempotentRelease(t *testing.T) {
	b, err := New(config("round-robin"))
	if err != nil {
		t.Fatal(err)
	}
	var leases []*Lease
	for i := 0; i < 6; i++ {
		l, e := b.Reserve()
		if e != nil {
			t.Fatal(e)
		}
		if l.Index != i%3 {
			t.Fatalf("rotation %d -> %d", i, l.Index)
		}
		leases = append(leases, l)
	}
	if _, e := b.Reserve(); !errors.Is(e, ErrCapacity) {
		t.Fatal(e)
	}
	for _, l := range leases {
		l.Complete(Canceled)
		l.Complete(Failure)
	}
	for _, s := range b.Snapshot() {
		if s.Outstanding != 0 || s.Failures != 0 {
			t.Fatal(s)
		}
	}
}

func TestLeastOutstandingAndHealth(t *testing.T) {
	b, _ := New(config("least-outstanding"))
	a, _ := b.Reserve()
	second, _ := b.Reserve()
	if a.Index == second.Index {
		t.Fatal("did not choose idle backend")
	}
	a.Complete(Success)
	second.Complete(Success)
	for i := 0; i < 3; i++ {
		b.Probe(0, false)
	}
	if b.Snapshot()[0].Healthy {
		t.Fatal("failed probe did not exclude")
	}
	b.Probe(0, true)
	if b.Snapshot()[0].Healthy {
		t.Fatal("restored too early")
	}
	b.Probe(0, false)
	b.Probe(0, true)
	if b.Snapshot()[0].Healthy {
		t.Fatal("probe streak not reset")
	}
	b.Probe(0, true)
	if !b.Snapshot()[0].Healthy {
		t.Fatal("failed to restore")
	}
	for i := 0; i < 3; i++ {
		b.Probe(i, false)
		b.Probe(i, false)
		b.Probe(i, false)
	}
	if _, e := b.Reserve(); !errors.Is(e, ErrUnhealthy) {
		t.Fatal(e)
	}
}

func TestPeakScoreAndColdDecay(t *testing.T) {
	now := time.Now()
	c := config("peak-ewma")
	c.Backends = []string{"a", "b"}
	c.Now = func() time.Time { return now }
	b, _ := New(c)
	if b.Snapshot()[0].EstimateNS != float64(50*time.Millisecond) {
		t.Fatal("cold prior")
	}
	b.nodes[0].ewma.value = float64(time.Millisecond)
	b.nodes[1].ewma.value = float64(100 * time.Millisecond)
	l, _ := b.Reserve()
	if l.Index != 0 {
		t.Fatal("wrong peak score")
	}
	now = now.Add(200 * time.Millisecond)
	l.Complete(Success)
	l, _ = b.Reserve()
	if l.Index != 1 {
		t.Fatal("peak did not change selection")
	}
	l.Complete(Canceled)
	old := b.nodes[0].ewma.value
	now = now.Add(10 * time.Second)
	b.mu.Lock()
	score := b.score(0, now)
	again := b.score(0, now)
	b.mu.Unlock()
	if math.Abs(score-old/math.E) > 1e-6 || score != again {
		t.Fatal("read-time decay", score, old)
	}
	b.nodes[0].out = 2
	b.mu.Lock()
	score = b.score(0, now)
	b.mu.Unlock()
	if math.Abs(score-3*old/math.E) > 1e-6 {
		t.Fatal("outstanding multiplier")
	}
}

func TestConcurrentReservationBound(t *testing.T) {
	b, _ := New(config("peak-ewma"))
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Go(func() {
			l, e := b.Reserve()
			if e != nil {
				return
			}
			for _, s := range b.Snapshot() {
				if s.Outstanding > 2 {
					t.Error("overbooked")
				}
			}
			l.Complete(Canceled)
		})
	}
	wg.Wait()
	for _, s := range b.Snapshot() {
		if s.Outstanding != 0 {
			t.Fatal("leaked", s)
		}
	}
}

func TestInvalidConfiguration(t *testing.T) {
	for _, change := range []func(*Config){func(c *Config) { c.Policy = "bogus" }, func(c *Config) { c.Tau = 0 }, func(c *Config) { c.Limit = 0 }, func(c *Config) { c.Backends = []string{"a", "a"} }} {
		c := config("peak-ewma")
		change(&c)
		if _, e := New(c); e == nil {
			t.Fatal("accepted invalid config")
		}
	}
}

func BenchmarkSelection(b *testing.B) {
	for _, p := range []string{"round-robin", "least-outstanding", "peak-ewma"} {
		b.Run(p, func(b *testing.B) {
			lb, _ := New(config(p))
			b.ReportAllocs()
			for b.Loop() {
				l, _ := lb.Reserve()
				l.Complete(Success)
			}
		})
	}
}
