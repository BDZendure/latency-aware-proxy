package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Exercise the production server deadlines, not httptest's default server.
func configuredProxy(t *testing.T, upstream http.HandlerFunc, change func(*Config), wrap ...func(net.Listener) net.Listener) (*Proxy, string) {
	t.Helper()
	up := httptest.NewServer(upstream)
	t.Cleanup(up.Close)
	c := DefaultConfig()
	c.Backends = []string{up.URL}
	if change != nil {
		change(&c)
	}
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	for _, apply := range wrap {
		ln = apply(ln)
	}
	srv := server(c, p)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(limitListener(ln, c.ConnectionLimit)) }()
	t.Cleanup(func() {
		_ = srv.Close()
		if err := <-done; err != http.ErrServerClosed {
			t.Error(err)
		}
	})
	return p, ln.Addr().String()
}

func TestUnreadRejectedBodyHasBoundedResponse(t *testing.T) {
	for _, kind := range []string{"oversized", "connect", "upgrade", "admission", "shutdown"} {
		t.Run(kind, func(t *testing.T) {
			p, address := configuredProxy(t, func(w http.ResponseWriter, r *http.Request) {
				t.Error("rejected request reached upstream")
			}, func(c *Config) { c.RequestTimeoutMS = 50; c.GlobalLimit = 1 })
			method, length, extra, want := "POST", 65537, "", 413
			switch kind {
			case "connect":
				method, length, want = "CONNECT", 10, 400
			case "upgrade":
				length, extra, want = 10, "Connection: Upgrade\r\nUpgrade: websocket\r\n", 400
			case "admission":
				p.slots <- struct{}{}
				defer func() { <-p.slots }()
				length, want = 10, 503
			case "shutdown":
				p.Drain()
				length, want = 10, 503
			}
			conn, err := net.Dial("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(250 * time.Millisecond))
			fmt.Fprintf(conn, "%s / HTTP/1.1\r\nHost: example\r\nContent-Length: %d\r\n%s\r\n", method, length, extra)
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("unread body stalled rejection beyond request budget: %v", err)
			}
			defer resp.Body.Close()
			if _, err := io.ReadAll(resp.Body); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != want || !resp.Close {
				t.Fatalf("want %d with closed connection, got %s close=%v", want, resp.Status, resp.Close)
			}
		})
	}
}

func TestBufferedResponsePreservesTrailers(t *testing.T) {
	for _, announced := range []bool{true, false} {
		t.Run(fmt.Sprint("announced=", announced), func(t *testing.T) {
			_, address := configuredProxy(t, func(w http.ResponseWriter, r *http.Request) {
				if announced {
					w.Header().Set("Trailer", "X-Checksum, "+ErrorHeader)
				}
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				w.Write([]byte("ok"))
				if announced {
					w.Header().Set("X-Checksum", "abc")
					w.Header().Set(ErrorHeader, "spoof")
				} else {
					w.Header().Set(http.TrailerPrefix+"X-Checksum", "abc")
					w.Header().Set(http.TrailerPrefix+ErrorHeader, "spoof")
				}
			}, nil)
			resp, err := http.Get("http://" + address)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || string(body) != "ok" {
				t.Fatalf("body=%q error=%v", body, err)
			}
			if resp.Trailer.Get("X-Checksum") != "abc" || resp.Trailer.Get(ErrorHeader) != "" || resp.Header.Get(ErrorHeader) != "" {
				t.Fatalf("headers=%v trailers=%v", resp.Header, resp.Trailer)
			}
			if resp.ContentLength != -1 || !strings.EqualFold(strings.Join(resp.TransferEncoding, ","), "chunked") {
				t.Fatalf("trailers require chunked framing: length=%d encoding=%v", resp.ContentLength, resp.TransferEncoding)
			}
		})
	}
}

func TestPartialChunkedUploadTimeoutAndCancellation(t *testing.T) {
	for _, cancelClient := range []bool{false, true} {
		t.Run(fmt.Sprint("cancel=", cancelClient), func(t *testing.T) {
			p, address := configuredProxy(t, func(w http.ResponseWriter, r *http.Request) {
				t.Error("partial upload reached upstream")
			}, func(c *Config) { c.RequestTimeoutMS = 60 })
			conn, err := net.Dial("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(300 * time.Millisecond))
			fmt.Fprint(conn, "POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhi")
			if cancelClient {
				conn.Close()
			} else {
				resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 504 || resp.Header.Get(ErrorHeader) != "deadline" || !resp.Close {
					t.Fatal(resp.Status, resp.Header, resp.Close)
				}
			}
			until := time.Now().Add(time.Second)
			for time.Now().Before(until) {
				p.mu.Lock()
				finished := p.counts["canceled"]+p.counts["timeout"] > 0
				p.mu.Unlock()
				if finished && len(p.slots) == 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if len(p.slots) != 0 || p.Balancer.Snapshot()[0].Completed != 0 {
				t.Fatal("partial upload leaked capacity or affected backend estimator", p.Snapshot())
			}
			p.mu.Lock()
			got := p.counts["timeout"]
			if cancelClient {
				got = p.counts["canceled"]
			}
			p.mu.Unlock()
			if got != 1 {
				t.Fatalf("wrong terminal outcome for cancel=%v: %v", cancelClient, p.Snapshot())
			}
		})
	}
}

func TestBodylessResponseFramingAndConnectionReuse(t *testing.T) {
	_, address := configuredProxy(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/head":
			w.Header().Set("Content-Length", "42")
			w.WriteHeader(200)
		case "/204":
			w.WriteHeader(204)
		case "/304":
			w.Header().Set("Content-Length", "42")
			w.WriteHeader(304)
		default:
			w.Write([]byte("ok"))
		}
	}, nil)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)
	for _, tc := range []struct {
		method, path, length, body string
		status                     int
	}{{"HEAD", "/head", "42", "", 200}, {"GET", "/204", "", "", 204}, {"GET", "/304", "", "", 304}, {"GET", "/ok", "2", "ok", 200}} {
		fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: example\r\n\r\n", tc.method, tc.path)
		resp, err := http.ReadResponse(reader, &http.Request{Method: tc.method})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != tc.status || resp.Header.Get("Content-Length") != tc.length || string(body) != tc.body || resp.Close {
			t.Fatalf("%s: status=%d headers=%v body=%q close=%v error=%v", tc.path, resp.StatusCode, resp.Header, body, resp.Close, err)
		}
	}
}

type smallSendBufferListener struct {
	net.Listener
	closed chan struct{}
}

type observedCloseConn struct {
	net.Conn
	closed chan struct{}
}

func (c observedCloseConn) Close() error {
	err := c.Conn.Close()
	select {
	case c.closed <- struct{}{}:
	default:
	}
	return err
}

func (l smallSendBufferListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		if err = conn.(*net.TCPConn).SetWriteBuffer(1024); err != nil {
			conn.Close()
			return nil, err
		}
	}
	if err == nil {
		conn = observedCloseConn{conn, l.closed}
	}
	return conn, err
}

func TestSlowReaderRetainsAdmissionThroughBufferedFlush(t *testing.T) {
	completed := make(chan struct{}, 128)
	closed := make(chan struct{}, 1)
	p, address := configuredProxy(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", 1024)))
		completed <- struct{}{}
	}, func(c *Config) { c.RequestTimeoutMS = 80; c.GlobalLimit = 1 }, func(l net.Listener) net.Listener { return smallSendBufferListener{l, closed} })
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.(*net.TCPConn).SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	// Each small response fits net/http's handler buffer. Eventually the raw
	// client stops the final socket flush by never reading any responses.
	_, err = io.WriteString(conn, strings.Repeat("GET / HTTP/1.1\r\nHost: example\r\n\r\n", cap(completed)))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("upstream not reached")
	}
	for {
		select {
		case <-completed:
		case <-time.After(40 * time.Millisecond):
			goto blocked
		}
	}
blocked:
	if len(p.slots) != 1 {
		t.Fatal("admission released before blocked downstream flush completed", p.Snapshot())
	}
	until := time.Now().Add(time.Second)
	for len(p.slots) != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if len(p.slots) != 0 {
		t.Fatal("slow reader retained admission beyond write deadline", p.Snapshot(), len(completed))
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("server did not close connection after bounded flush", p.Snapshot(), len(completed))
	}
}

func TestChunkedRequestBodyAndTrailersForwarded(t *testing.T) {
	type incoming struct {
		body, checksum, dynamic string
		length                  int64
	}
	seen := make(chan incoming, 1)
	_, address := configuredProxy(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		seen <- incoming{string(body), r.Trailer.Get("X-Checksum"), r.Trailer.Get("X-Dynamic"), r.ContentLength}
		w.Write([]byte("ok"))
	}, nil)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	_, err = io.WriteString(conn, "POST / HTTP/1.1\r\nHost: example\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nTrailer: X-Checksum\r\n\r\n2\r\nhe\r\n3\r\nllo\r\n0\r\nX-Checksum: abc\r\nX-Dynamic: xyz\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatal(resp.Status, string(body), err)
	}
	select {
	case got := <-seen:
		if got != (incoming{"hello", "abc", "xyz", -1}) {
			t.Fatalf("chunked request or trailers changed: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive chunked request")
	}
}
