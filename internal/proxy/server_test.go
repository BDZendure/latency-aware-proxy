package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGracefulAndForcedShutdown(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "force"}[force], func(t *testing.T) {
			started := make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				if force {
					<-r.Context().Done()
				} else {
					time.Sleep(30 * time.Millisecond)
					w.Write([]byte("done"))
				}
			}))
			defer up.Close()
			c := DefaultConfig()
			c.Backends = []string{up.URL}
			c.ShutdownGraceMS = 80
			p, e := New(c)
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			ln, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			admin, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- serve(ctx, p, ln, admin) }()
			clientDone := make(chan struct{})
			go func() {
				defer close(clientDone)
				r, e := http.Get("http://" + ln.Addr().String())
				if e == nil {
					io.Copy(io.Discard, r.Body)
					r.Body.Close()
				}
			}()
			<-started
			cancel()
			select {
			case e := <-done:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown stuck")
			}
			<-clientDone
			st := p.Balancer.Snapshot()[0]
			if st.Outstanding != 0 || (force && st.Canceled != 1) || (!force && st.Canceled != 0) {
				t.Fatal(st)
			}
		})
	}
}

func TestBoundedListenerCloseUnblocksAccept(t *testing.T) {
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	limited := limitListener(ln, 1)
	defer limited.Close()
	client, e := net.Dial("tcp", ln.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	accepted, e := limited.Accept()
	if e != nil {
		t.Fatal(e)
	}
	defer accepted.Close()
	done := make(chan error, 1)
	go func() { _, e := limited.Accept(); done <- e }()
	limited.Close()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("closed listener succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Accept not interrupted")
	}
}
