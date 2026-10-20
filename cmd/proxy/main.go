package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"latencyproxy/internal/proxy"
)

func main() {
	path := flag.String("config", "configs/proxy.json", "JSON configuration")
	policy := flag.String("policy", "", "override routing policy")
	flag.Parse()
	c, e := proxy.LoadConfig(*path)
	if e == nil && *policy != "" {
		c.Policy = *policy
	}
	if e != nil {
		slog.Error("configuration failed", "error", e)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e = proxy.Run(ctx, c); e != nil {
		slog.Error("proxy stopped", "error", e)
		os.Exit(1)
	}
}
