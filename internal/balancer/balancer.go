// Package balancer owns routing, eligibility, and exactly-once backend reservations.
package balancer

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

var (
	ErrCapacity  = errors.New("all healthy backends at capacity")
	ErrUnhealthy = errors.New("no healthy backend")
)

type Outcome uint8

const (
	Success Outcome = iota
	Failure
	TimedOut
	Canceled
)

type Config struct {
	Policy                                  string
	Backends                                []string
	Limit                                   int
	Cold, Tau, ErrorPenalty, TimeoutPenalty time.Duration
	FailureThreshold, RecoveryThreshold     int
	Seed                                    uint64
	Now                                     func() time.Time
}

type node struct {
	address                                 string
	out                                     int
	healthy                                 bool
	passive, probeFailures, probeSuccess    int
	ewma                                    estimate
	selected, completed, failures, canceled uint64
}

type Balancer struct {
	mu       sync.Mutex
	cfg      Config
	nodes    []node
	rng      *rand.Rand
	next     int
	eligible []int
}

func New(c Config) (*Balancer, error) {
	if c.Policy != "round-robin" && c.Policy != "least-outstanding" && c.Policy != "peak-ewma" {
		return nil, fmt.Errorf("unknown policy %q", c.Policy)
	}
	if len(c.Backends) == 0 || c.Limit <= 0 || c.Cold <= 0 || c.Tau <= 0 || c.ErrorPenalty <= 0 || c.TimeoutPenalty <= 0 || c.FailureThreshold <= 0 || c.RecoveryThreshold <= 0 {
		return nil, errors.New("backend pool, limits, durations, and health thresholds must be positive")
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	b := &Balancer{cfg: c, rng: rand.New(rand.NewPCG(c.Seed, c.Seed^0x9e3779b97f4a7c15)), eligible: make([]int, 0, len(c.Backends))}
	seen := make(map[string]bool)
	now := c.Now()
	for _, a := range c.Backends {
		if a == "" || seen[a] {
			return nil, errors.New("empty or duplicate backend")
		}
		seen[a] = true
		b.nodes = append(b.nodes, node{address: a, healthy: true, ewma: estimate{value: float64(c.Cold), stamp: now}})
	}
	return b, nil
}

// Lease represents a reserved backend slot, including transport and body read.
// Complete can safely be called more than once, including from deferred cleanup.
type Lease struct {
	Index   int
	Address string
	b       *Balancer
	start   time.Time
	once    sync.Once
}

func (b *Balancer) Reserve() (*Lease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.eligible = b.eligible[:0]
	healthy := false
	for i := range b.nodes {
		n := &b.nodes[i]
		if n.healthy {
			healthy = true
			if n.out < b.cfg.Limit {
				b.eligible = append(b.eligible, i)
			}
		}
	}
	if len(b.eligible) == 0 {
		if healthy {
			return nil, ErrCapacity
		}
		return nil, ErrUnhealthy
	}
	now := b.cfg.Now()
	pick := b.eligible[0]
	switch b.cfg.Policy {
	case "round-robin":
		for offset := 0; offset < len(b.nodes); offset++ {
			i := (b.next + offset) % len(b.nodes)
			if n := &b.nodes[i]; n.healthy && n.out < b.cfg.Limit {
				pick = i
				b.next = (i + 1) % len(b.nodes)
				break
			}
		}
	case "least-outstanding":
		ties := 1
		for _, i := range b.eligible[1:] {
			if b.nodes[i].out < b.nodes[pick].out {
				pick = i
				ties = 1
			} else if b.nodes[i].out == b.nodes[pick].out {
				ties++
				if b.rng.IntN(ties) == 0 {
					pick = i
				}
			}
		}
	case "peak-ewma":
		a := b.rng.IntN(len(b.eligible))
		pick = b.eligible[a]
		if len(b.eligible) > 1 {
			other := b.rng.IntN(len(b.eligible) - 1)
			if other >= a {
				other++
			}
			i := b.eligible[other]
			sa, sb := b.score(pick, now), b.score(i, now)
			if sb < sa || (sa == sb && b.rng.IntN(2) == 0) {
				pick = i
			}
		}
	}
	n := &b.nodes[pick]
	n.out++
	n.selected++
	return &Lease{Index: pick, Address: n.address, b: b, start: now}, nil
}

func (b *Balancer) score(i int, now time.Time) float64 {
	n := &b.nodes[i]
	n.ewma.observe(now, 0, b.cfg.Tau)
	return n.ewma.value * float64(n.out+1)
}

func (l *Lease) Complete(outcome Outcome) {
	l.once.Do(func() {
		b := l.b
		b.mu.Lock()
		defer b.mu.Unlock()
		n := &b.nodes[l.Index]
		n.out--
		n.completed++
		if outcome == Canceled {
			n.canceled++
			return
		}
		now := b.cfg.Now()
		elapsed := max(now.Sub(l.start), 0)
		if outcome == Failure || outcome == TimedOut {
			n.failures++
			n.passive++
			if n.passive >= b.cfg.FailureThreshold {
				n.healthy = false
				n.probeSuccess = 0
			}
			penalty := b.cfg.ErrorPenalty
			if outcome == TimedOut {
				penalty = b.cfg.TimeoutPenalty
			}
			elapsed = max(elapsed, penalty)
		} else {
			n.passive = 0
		}
		n.ewma.observe(now, float64(elapsed), b.cfg.Tau)
	})
}

// Probe is separate from user reservations and never changes latency estimates.
func (b *Balancer) Probe(i int, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := &b.nodes[i]
	if success {
		n.probeFailures = 0
		n.probeSuccess++
		if !n.healthy && n.probeSuccess >= b.cfg.RecoveryThreshold {
			n.healthy = true
			n.passive = 0
			n.probeFailures = 0
			n.probeSuccess = 0
		}
	} else {
		n.probeSuccess = 0
		n.probeFailures++
		if n.probeFailures >= b.cfg.FailureThreshold {
			n.healthy = false
		}
	}
}

type Stats struct {
	Address     string  `json:"address"`
	Outstanding int     `json:"outstanding"`
	Healthy     bool    `json:"healthy"`
	EstimateNS  float64 `json:"estimate_ns"`
	Selected    uint64  `json:"selected"`
	Completed   uint64  `json:"completed"`
	Failures    uint64  `json:"failures"`
	Canceled    uint64  `json:"canceled"`
}

// Snapshot does not update estimates: scraping metrics cannot affect routing.
func (b *Balancer) Snapshot() []Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Stats, len(b.nodes))
	for i, n := range b.nodes {
		out[i] = Stats{n.address, n.out, n.healthy, n.ewma.value, n.selected, n.completed, n.failures, n.canceled}
	}
	return out
}
