package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

func Run(ctx context.Context, c Config) error {
	p, e := New(c)
	if e != nil {
		return e
	}
	defer p.Close()
	ln, e := net.Listen("tcp", c.Listen)
	if e != nil {
		return e
	}
	defer ln.Close()
	admin, e := net.Listen("tcp", c.Admin)
	if e != nil {
		return e
	}
	defer admin.Close()
	slog.Info("proxy listening", "listen", ln.Addr().String(), "admin", admin.Addr().String(), "policy", c.Policy)
	return serve(ctx, p, ln, admin)
}

func server(c Config, h http.Handler) *http.Server {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	return &http.Server{Handler: h, Protocols: protocols, ReadHeaderTimeout: ms(c.HeaderTimeoutMS), IdleTimeout: ms(c.IdleTimeoutMS), ReadTimeout: ms(c.RequestTimeoutMS), WriteTimeout: ms(c.RequestTimeoutMS) + 100*time.Millisecond, MaxHeaderBytes: c.MaxHeaderBytes}
}

func serve(ctx context.Context, p *Proxy, ln, admin net.Listener) error {
	data := server(p.Config, p)
	control := server(p.Config, p.AdminHandler())
	errs := make(chan error, 2)
	go func() { errs <- data.Serve(limitListener(ln, p.Config.ConnectionLimit)) }()
	go func() { errs <- control.Serve(limitListener(admin, 16)) }()
	p.StartHealth()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errs:
	}
	p.Drain()
	grace, cancel := context.WithTimeout(context.Background(), ms(p.Config.ShutdownGraceMS))
	defer cancel()
	if e := data.Shutdown(grace); e != nil {
		p.Close()
		_ = data.Close()
	}
	p.Close()
	_ = control.Close()
	p.handlers.Wait()
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}
