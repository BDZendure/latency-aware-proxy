// Package proxy implements bounded HTTP/1.1 forwarding around the Go reverse proxy.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"latencyproxy/internal/balancer"
)

const ErrorHeader = "X-Latency-Proxy-Error"

type stateKey struct{}
type requestState struct {
	lease    *balancer.Lease
	incoming context.Context
	p        *Proxy
	marker   string
}

type Proxy struct {
	Config              Config
	Balancer            *balancer.Balancer
	transport           *http.Transport
	probeTransport      *http.Transport
	reverse             *httputil.ReverseProxy
	requests, responses *buffers
	slots               chan struct{}
	root                context.Context
	cancel              context.CancelFunc
	draining            atomic.Bool
	startHealth         sync.Once
	healthWG            sync.WaitGroup
	mu                  sync.Mutex
	counts              map[string]uint64
	handlers            sync.WaitGroup
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func New(c Config) (*Proxy, error) {
	if e := c.Validate(); e != nil {
		return nil, e
	}
	b, e := balancer.New(balancer.Config{Policy: c.Policy, Backends: c.Backends, Limit: c.BackendLimit, Cold: ms(c.ColdMS), Tau: ms(c.DecayMS), ErrorPenalty: ms(c.FailurePenaltyMS), TimeoutPenalty: ms(c.RequestTimeoutMS), FailureThreshold: c.FailureThreshold, RecoveryThreshold: c.RecoveryThreshold, Seed: c.Seed})
	if e != nil {
		return nil, e
	}
	ctx, cancel := context.WithCancel(context.Background())
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: ms(c.RequestTimeoutMS), KeepAlive: 30 * time.Second}).DialContext, Protocols: protocols, MaxConnsPerHost: c.BackendLimit, MaxIdleConnsPerHost: c.BackendLimit, MaxIdleConns: c.BackendLimit * len(c.Backends), IdleConnTimeout: ms(c.IdleTimeoutMS), ResponseHeaderTimeout: ms(c.RequestTimeoutMS), MaxResponseHeaderBytes: int64(c.MaxHeaderBytes), DisableCompression: true}
	probe := tr.Clone()
	probe.MaxConnsPerHost = 1
	probe.MaxIdleConnsPerHost = 1
	probe.MaxIdleConns = len(c.Backends)
	p := &Proxy{Config: c, Balancer: b, transport: tr, probeTransport: probe, requests: newBuffers(c.GlobalLimit, c.MaxRequestBytes), responses: newBuffers(c.GlobalLimit, c.MaxResponseBytes), slots: make(chan struct{}, c.GlobalLimit), root: ctx, cancel: cancel, counts: make(map[string]uint64)}
	p.reverse = &httputil.ReverseProxy{Transport: bufferTransport{p}, ErrorLog: log.New(io.Discard, "", 0), Rewrite: func(pr *httputil.ProxyRequest) {
		s := pr.In.Context().Value(stateKey{}).(*requestState)
		u, _ := url.Parse(s.lease.Address)
		pr.SetURL(u)
		pr.SetXForwarded()
		pr.Out.Header.Del(ErrorHeader)
	}, ModifyResponse: func(r *http.Response) error { r.Header.Del(ErrorHeader); r.Header.Del("Alt-Svc"); return nil }, ErrorHandler: func(w http.ResponseWriter, r *http.Request, e error) {
		s := r.Context().Value(stateKey{}).(*requestState)
		status, marker := http.StatusBadGateway, "upstream"
		outcome := classify(r.Context(), s)
		if outcome == balancer.TimedOut || (outcome == balancer.Failure && isTimeout(e)) {
			status, marker = http.StatusGatewayTimeout, "deadline"
		}
		s.marker = marker
		writeError(w, status, marker)
	}}
	return p, nil
}

func writeError(w http.ResponseWriter, status int, marker string) {
	if marker == "deadline" {
		// The upstream budget has expired. Allow a bounded interval to deliver 504.
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	}
	w.Header().Set(ErrorHeader, marker)
	http.Error(w, http.StatusText(status), status)
}
func (p *Proxy) count(key string) { p.mu.Lock(); p.counts[key]++; p.mu.Unlock() }

type writer struct {
	http.ResponseWriter
	status int
	err    error
}

func (w *writer) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *writer) WriteHeader(code int) {
	if code >= 200 {
		if w.status != 0 {
			return
		}
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *writer) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	n, e := w.ResponseWriter.Write(b)
	if e != nil {
		w.err = e
	}
	return n, e
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.handlers.Add(1)
	defer p.handlers.Done()
	entered := time.Now()
	p.count("received")
	controller := http.NewResponseController(w)
	deadline := entered.Add(ms(p.Config.RequestTimeoutMS))
	_ = controller.SetReadDeadline(deadline)
	_ = controller.SetWriteDeadline(deadline)
	tw := &writer{ResponseWriter: w}
	s := &requestState{incoming: r.Context(), p: p}
	admitted, bodyRead := false, false
	// Until the complete request body is accepted, do not let net/http drain
	// unread input before delivering an error response.
	w.Header().Set("Connection", "close")
	defer func() {
		// Flush buffered output while admission is still held. Keep the write
		// deadline for net/http's final framing flush after this handler returns.
		if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			tw.err = err
		}
		if admitted {
			switch {
			case p.root.Err() != nil:
				p.count("canceled")
			case s.marker == "deadline":
				// net/http also cancels the incoming context on a body-read
				// deadline. Preserve the known timeout outcome in that case.
				p.count("timeout")
			case s.incoming.Err() != nil:
				p.count("canceled")
			case tw.err != nil:
				p.count("downstream_error")
			case tw.status >= 400:
				p.count("http_error")
			default:
				p.count("success")
			}
		}
		if !bodyRead {
			// Body.Close may otherwise drain an unread upload. The connection is
			// already marked for closure, so no further input is useful.
			_ = controller.SetReadDeadline(time.Now())
			_ = r.Body.Close()
		}
		if admitted {
			<-p.slots
		}
	}()
	if p.draining.Load() {
		p.count("rejected_shutdown")
		writeError(tw, 503, "shutdown")
		return
	}
	select {
	case p.slots <- struct{}{}:
		admitted = true
	default:
		p.count("rejected_admission")
		writeError(tw, 503, "admission")
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	stop := context.AfterFunc(p.root, cancel)
	defer stop()
	if r.Method == http.MethodConnect || r.Header.Get("Upgrade") != "" {
		writeError(tw, 400, "unsupported")
		return
	}
	if r.ContentLength > int64(p.Config.MaxRequestBytes) {
		writeError(tw, 413, "request_body")
		return
	}
	buf := p.requests.get()
	defer p.requests.put(buf)
	n, e := readBounded(r.Body, buf)
	if e != nil || ctx.Err() != nil {
		if errors.Is(e, errBodyLimit) {
			writeError(tw, 413, "request_body")
		} else if errors.Is(ctx.Err(), context.DeadlineExceeded) || isTimeout(e) {
			s.marker = "deadline"
			writeError(tw, 504, "deadline")
		} else {
			writeError(tw, 400, "request_body")
		}
		return
	}
	bodyRead = true
	_ = r.Body.Close()
	w.Header().Del("Connection")
	// Only a fully read, accepted body permits clearing the read deadline.
	// Otherwise net/http's background read can falsely cancel an active backend.
	_ = controller.SetReadDeadline(time.Time{})
	if s.incoming.Err() != nil || p.root.Err() != nil {
		return
	}
	l, e := p.Balancer.Reserve()
	if e != nil {
		marker := "capacity"
		if errors.Is(e, balancer.ErrUnhealthy) {
			marker = "unhealthy"
		}
		p.count("rejected_" + marker)
		s.marker = marker
		writeError(tw, 503, marker)
		return
	}
	s.lease = l
	defer l.Complete(balancer.Canceled)
	r = r.Clone(context.WithValue(ctx, stateKey{}, s))
	r.Body = io.NopCloser(bytes.NewReader(buf[:n]))
	r.ContentLength = int64(n)
	r.GetBody = nil
	p.reverse.ServeHTTP(tw, r)
}

func isTimeout(e error) bool { var ne net.Error; return errors.As(e, &ne) && ne.Timeout() }
func classify(ctx context.Context, s *requestState) balancer.Outcome {
	if s.incoming.Err() != nil || s.p.root.Err() != nil {
		return balancer.Canceled
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return balancer.TimedOut
	}
	return balancer.Failure
}

func (p *Proxy) Snapshot() map[string]any {
	p.mu.Lock()
	counts := make(map[string]uint64, len(p.counts))
	for k, v := range p.counts {
		counts[k] = v
	}
	p.mu.Unlock()
	return map[string]any{"policy": p.Config.Policy, "active": len(p.slots), "counters": counts, "backends": p.Balancer.Snapshot()}
}

func (p *Proxy) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p.Snapshot())
	})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !p.draining.Load() {
			for _, s := range p.Balancer.Snapshot() {
				if s.Healthy {
					w.Write([]byte("ready\n"))
					return
				}
			}
		}
		http.Error(w, "unavailable", 503)
	})
	return mux
}

func (p *Proxy) Drain() { p.draining.Store(true) }
func (p *Proxy) Close() {
	p.Drain()
	p.cancel()
	p.healthWG.Wait()
	p.transport.CloseIdleConnections()
	p.probeTransport.CloseIdleConnections()
}
