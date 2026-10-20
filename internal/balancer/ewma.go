package balancer

import (
	"math"
	"time"
)

// estimate follows the pinned Finagle recurrence; callers hold Balancer.mu.
type estimate struct {
	value float64
	stamp time.Time
}

func (e *estimate) observe(now time.Time, rtt float64, tau time.Duration) {
	delta := max(now.Sub(e.stamp), 0)
	if rtt > e.value {
		e.value = rtt
	} else {
		w := math.Exp(-float64(delta) / float64(tau))
		e.value = e.value*w + rtt*(1-w)
	}
	e.stamp = now
}
