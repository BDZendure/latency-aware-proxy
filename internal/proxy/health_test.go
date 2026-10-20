package proxy

import (
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestPassiveHealthStreaksAndPeriodicRecovery(t *testing.T) {
	var failData, healthyProbe atomic.Bool
	p, address := configuredProxy(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			if !healthyProbe.Load() {
				w.WriteHeader(503)
			}
			return
		}
		if failData.Load() {
			w.WriteHeader(503)
		}
		w.Write([]byte("body"))
	}, func(c *Config) {
		c.FailureThreshold, c.RecoveryThreshold = 2, 2
		c.ProbeIntervalMS, c.ProbeTimeoutMS = 10, 100
	})
	request := func(want int) {
		t.Helper()
		resp, err := http.Get("http://" + address)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatal(resp.Status)
		}
	}
	// A success resets the passive failure streak.
	for _, fail := range []bool{true, false, true} {
		failData.Store(fail)
		want := 200
		if fail {
			want = 503
		}
		request(want)
		if !p.Balancer.Snapshot()[0].Healthy {
			t.Fatal("nonconsecutive passive failures ejected backend")
		}
	}
	request(503)
	before := p.Balancer.Snapshot()[0]
	if before.Healthy || before.Failures != 3 || before.Completed != 4 {
		t.Fatal(before)
	}
	request(503)
	if p.Balancer.Snapshot()[0].Selected != before.Selected {
		t.Fatal("ejected backend still receives traffic")
	}
	p.StartHealth()
	healthyProbe.Store(true)
	until := time.Now().Add(time.Second)
	for !p.Balancer.Snapshot()[0].Healthy && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	after := p.Balancer.Snapshot()[0]
	if !after.Healthy || after.EstimateNS != before.EstimateNS || after.Completed != before.Completed || after.Selected != before.Selected {
		t.Fatal("probes did not recover independently of traffic accounting", before, after)
	}
	failData.Store(false)
	request(200)
}

func TestInflightSuccessDoesNotUndoPassiveEjection(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	p, address := configuredProxy(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(started)
			<-release
		} else {
			w.WriteHeader(503)
		}
	}, func(c *Config) { c.FailureThreshold = 1 })
	done := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + address + "/slow")
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow request did not start")
	}
	resp, err := http.Get("http://" + address + "/fail")
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	st := p.Balancer.Snapshot()[0]
	if st.Healthy || st.Outstanding != 0 || st.Completed != 2 || st.Failures != 1 {
		t.Fatal("in-flight success revived ejected backend or leaked capacity", st)
	}
}
