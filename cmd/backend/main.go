// Command backend serves the configurable HTTP/1 backend simulator.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"latencyproxy/internal/sim"
)

func readConfig(path string) (sim.Config, error) {
	c := sim.DefaultConfig()
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return c, err
		}
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
		if err != nil {
			return c, err
		}
		if len(data) > 1<<20 {
			return c, fmt.Errorf("config exceeds 1 MiB")
		}
		trimmed := bytes.TrimSpace(data)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return c, fmt.Errorf("config must contain one JSON object")
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&c); err != nil {
			return c, fmt.Errorf("config: %w", err)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return c, fmt.Errorf("config must contain one JSON object")
		}
	}
	return c, c.Validate()
}
func run(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("backend", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", "127.0.0.1:9001", "HTTP/1 listen address")
	config := flags.String("config", "", "JSON configuration path")
	startAt := flags.String("start-at", "", "common experiment origin (RFC3339Nano); baseline behavior before origin")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	c, err := readConfig(*config)
	if err != nil {
		return err
	}
	var origin time.Time
	if *startAt != "" {
		origin, err = time.Parse(time.RFC3339Nano, *startAt)
		if err != nil {
			return fmt.Errorf("start-at: %w", err)
		}
	}
	handler, err := sim.NewAt(c, origin)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 5 * time.Minute, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10, Protocols: protocols}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case err := <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
		}
		err := <-served
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
