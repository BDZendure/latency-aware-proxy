package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func setup(t *testing.T, h http.HandlerFunc, change func(*Config)) (*Proxy, *httptest.Server) {
	t.Helper()
	up := httptest.NewServer(h)
	t.Cleanup(up.Close)
	c := DefaultConfig()
	c.Backends = []string{up.URL}
	if change != nil {
		change(&c)
	}
	p, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.Close)
	s := httptest.NewServer(p)
	t.Cleanup(s.Close)
	return p, s
}

func TestForwardingAndUntrustedHeaders(t *testing.T) {
	p, s := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/a%2Fb?x=1&x=2" || r.Method != "POST" {
			t.Error(r.URL, r.Method)
		}
		b, _ := io.ReadAll(r.Body)
		if string(b) != "hello" {
			t.Error(string(b))
		}
		if r.Header.Get("X-Untrusted") != "" || strings.Contains(r.Header.Get("X-Forwarded-For"), "spoof") {
			t.Error(r.Header)
		}
		w.Header().Set("X-Latency-Proxy-Error", "deadline")
		w.Header().Set("Connection", "X-Remove")
		w.Header().Set("X-Remove", "secret")
		w.Write([]byte("ok"))
	}, nil)
	r, _ := http.NewRequest("POST", s.URL+"/a%2Fb?x=1&x=2", strings.NewReader("hello"))
	r.Header.Set("Connection", "X-Untrusted")
	r.Header.Set("X-Untrusted", "bad")
	r.Header.Set("X-Forwarded-For", "spoof")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(b) != "ok" || resp.Header.Get("X-Remove") != "" || resp.Header.Get("X-Latency-Proxy-Error") != "" {
		t.Fatal(resp.Status, b, resp.Header)
	}
	if p.Balancer.Snapshot()[0].Outstanding != 0 {
		t.Fatal("reservation leaked")
	}
}

func TestOutstandingIncludesResponseBodyAndRejectsCapacity(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	p, s := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("first"))
		w.(http.Flusher).Flush()
		close(started)
		<-release
		w.Write([]byte("last"))
	}, func(c *Config) { c.BackendLimit = 1 })
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, e := http.Get(s.URL)
		if e == nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
	}()
	<-started
	if p.Balancer.Snapshot()[0].Outstanding != 1 {
		t.Fatal("released at headers")
	}
	r, e := http.Get(s.URL)
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if r.StatusCode != 503 || r.Header.Get("X-Latency-Proxy-Error") != "capacity" {
		t.Fatal(r.Status, r.Header)
	}
	close(release)
	<-done
	if p.Balancer.Snapshot()[0].Outstanding != 0 {
		t.Fatal("leak")
	}
}

func TestDeadlineAndClientCancellation(t *testing.T) {
	for _, cancelClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "client"}[cancelClient], func(t *testing.T) {
			started := make(chan struct{})
			p, s := setup(t, func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }, func(c *Config) { c.RequestTimeoutMS = 80 })
			originalHandler := p.reverse.ErrorHandler
			p.reverse.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
				s := r.Context().Value(stateKey{}).(*requestState)
				t.Logf("upstream error=%v request context=%v incoming context=%v", e, r.Context().Err(), s.incoming.Err())
				originalHandler(w, r, e)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, _ := http.NewRequestWithContext(ctx, "GET", s.URL, nil)
			done := make(chan *http.Response, 1)
			go func() { resp, _ := http.DefaultClient.Do(r); done <- resp }()
			<-started
			if cancelClient {
				cancel()
			}
			resp := <-done
			if !cancelClient {
				if resp == nil || resp.StatusCode != 504 || resp.Header.Get("X-Latency-Proxy-Error") != "deadline" {
					t.Fatal(resp)
				}
				resp.Body.Close()
			}
			until := time.Now().Add(time.Second)
			for p.Balancer.Snapshot()[0].Outstanding != 0 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			st := p.Balancer.Snapshot()[0]
			if st.Outstanding != 0 || (cancelClient && (st.Failures != 0 || st.Canceled != 1)) || (!cancelClient && st.Failures != 1) {
				t.Fatal(st)
			}
		})
	}
}

func TestBodyBoundsAndTruncation(t *testing.T) {
	for _, kind := range []string{"input", "output", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			p, s := setup(t, func(w http.ResponseWriter, r *http.Request) {
				if kind == "truncated" {
					w.Header().Set("Content-Length", "20")
					w.Write([]byte("short"))
					return
				}
				w.Write([]byte(strings.Repeat("x", 100)))
			}, func(c *Config) { c.MaxRequestBytes = 16; c.MaxResponseBytes = 16 })
			body := ""
			want := 502
			if kind == "input" {
				body = strings.Repeat("b", 17)
				want = 413
			}
			r, e := http.Post(s.URL, "text/plain", strings.NewReader(body))
			if e != nil {
				t.Fatal(e)
			}
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
			if r.StatusCode != want {
				t.Fatal(r.Status)
			}
			if p.Balancer.Snapshot()[0].Outstanding != 0 {
				t.Fatal("leak")
			}
		})
	}
}

func TestConnectionPooling(t *testing.T) {
	var connections atomic.Int64
	up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	up.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			connections.Add(1)
		}
	}
	up.Start()
	defer up.Close()
	c := DefaultConfig()
	c.Backends = []string{up.URL}
	p, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	s := httptest.NewServer(p)
	defer s.Close()
	for i := 0; i < 5; i++ {
		r, e := http.Get(s.URL)
		if e != nil {
			t.Fatal(e)
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	if connections.Load() != 1 {
		t.Fatal("pool not reused", connections.Load())
	}
}

func TestAdmissionAndForcedClose(t *testing.T) {
	started := make(chan struct{})
	p, s := setup(t, func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }, func(c *Config) { c.GlobalLimit = 1 })
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, e := http.Get(s.URL)
		if e == nil {
			resp.Body.Close()
		}
	}()
	<-started
	r, e := http.Get(s.URL)
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if r.StatusCode != 503 || r.Header.Get("X-Latency-Proxy-Error") != "admission" {
		t.Fatal(r.Status, r.Header)
	}
	p.Close()
	<-done
	if p.Balancer.Snapshot()[0].Outstanding != 0 {
		t.Fatal("shutdown leaked")
	}
}

func TestConfigurationValidation(t *testing.T) {
	for _, url := range []string{"https://example.com", "http://u:p@host", "http://host/path", "http://host?a=b", "http://"} {
		c := DefaultConfig()
		c.Backends = []string{url}
		if _, e := New(c); e == nil {
			t.Fatal("accepted", url)
		}
	}
}
