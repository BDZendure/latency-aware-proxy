// Command loadgen emits open-loop HTTP/1 requests and a JSON accounting summary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"latencyproxy/internal/loadgen"
)

func atomicWrite(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".loadgen-summary-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	c := loadgen.DefaultConfig()
	flags := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&c.URL, "url", "", "target HTTP URL")
	startAt := flags.String("start-at", "", "common warmup origin (RFC3339Nano)")
	flags.DurationVar(&c.Duration, "duration", c.Duration, "measurement offered interval")
	flags.Float64Var(&c.Rate, "rate", c.Rate, "offered requests per second")
	flags.IntVar(&c.Workers, "workers", c.Workers, "maximum simultaneous requests")
	flags.DurationVar(&c.Timeout, "timeout", c.Timeout, "per-request timeout")
	flags.Int64Var(&c.Seed, "seed", c.Seed, "deterministic cost seed")
	flags.Float64Var(&c.HeavyFraction, "heavy-fraction", c.HeavyFraction, "fraction of heavy requests")
	flags.StringVar(&c.Output, "output", "", "atomic JSON summary output path")
	flags.StringVar(&c.Records, "records", "", "streamed per-offer CSV output path")
	flags.DurationVar(&c.MaxLag, "max-lag", c.MaxLag, "maximum dispatch lag before counting a generator miss")
	flags.DurationVar(&c.SpikeStart, "spike-start", 0, "measurement-relative arrival spike start")
	flags.DurationVar(&c.SpikeEnd, "spike-end", 0, "measurement-relative arrival spike end")
	flags.Float64Var(&c.SpikeFactor, "spike-factor", c.SpikeFactor, "arrival rate multiplier within spike window")
	flags.DurationVar(&c.Warmup, "warmup", 0, "separate warmup offered interval")
	flags.Int64Var(&c.MaxResponseBytes, "max-response-bytes", c.MaxResponseBytes, "maximum consumed response bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	if *startAt != "" {
		var err error
		c.StartAt, err = time.Parse(time.RFC3339Nano, *startAt)
		if err != nil {
			return fmt.Errorf("start-at: %w", err)
		}
	}
	if err := c.Validate(); err != nil {
		return err
	}
	summary, runErr := loadgen.Run(ctx, c)
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return errors.Join(runErr, err)
	}
	data = append(data, '\n')
	if c.Output != "" {
		if err := atomicWrite(c.Output, data); err != nil {
			return errors.Join(runErr, err)
		}
	}
	_, err = stdout.Write(data)
	return errors.Join(runErr, err)
}
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
