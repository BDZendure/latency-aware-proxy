package proxy

import (
	"bytes"
	"errors"
	"net/http"
	"strconv"

	"latencyproxy/internal/balancer"
)

type bufferTransport struct{ p *Proxy }

func (t bufferTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	s := r.Context().Value(stateKey{}).(*requestState)
	resp, e := t.p.transport.RoundTrip(r)
	if e != nil {
		out := classify(r.Context(), s)
		if out == balancer.Failure && isTimeout(e) {
			out = balancer.TimedOut
		}
		s.lease.Complete(out)
		return nil, e
	}
	if resp.StatusCode == http.StatusSwitchingProtocols {
		_ = resp.Body.Close()
		s.lease.Complete(balancer.Failure)
		return nil, errors.New("upstream protocol upgrade unsupported")
	}
	buf := t.p.responses.get()
	n, e := readBounded(resp.Body, buf)
	closeErr := resp.Body.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil || r.Context().Err() != nil {
		t.p.responses.put(buf)
		s.lease.Complete(classify(r.Context(), s))
		if e == nil {
			e = r.Context().Err()
		}
		return nil, e
	}
	out := balancer.Success
	if resp.StatusCode >= 500 {
		out = balancer.Failure
	}
	s.lease.Complete(out)
	resp.Body = &bufferedBody{Reader: bytes.NewReader(buf[:n]), pool: t.p.responses, data: buf}
	resp.TransferEncoding = nil
	resp.Header.Del("Transfer-Encoding")
	resp.Trailer.Del(ErrorHeader)
	if r.Method != "HEAD" && resp.StatusCode != 204 && resp.StatusCode != 304 {
		if len(resp.Trailer) != 0 {
			// HTTP/1.1 trailers require chunked framing, even with a buffered body.
			resp.ContentLength = -1
			resp.Header.Del("Content-Length")
		} else {
			resp.ContentLength = int64(n)
			resp.Header.Set("Content-Length", strconv.Itoa(n))
		}
	}
	return resp, nil
}
