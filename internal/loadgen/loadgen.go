// Package loadgen runs an open-loop, bounded-concurrency HTTP/1 workload.
package loadgen

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"latencyproxy/internal/hist"
)

type Config struct {
	URL              string
	StartAt          time.Time
	Duration         time.Duration
	Rate             float64
	Workers          int
	Timeout          time.Duration
	Seed             int64
	HeavyFraction    float64
	Output           string
	Records          string
	RecordsWriter    io.Writer
	MaxLag           time.Duration
	SpikeStart       time.Duration
	SpikeEnd         time.Duration
	SpikeFactor      float64
	Warmup           time.Duration
	MaxResponseBytes int64
}

func DefaultConfig() Config {
	return Config{Duration: 10 * time.Second, Rate: 100, Workers: 128, Timeout: 2 * time.Second, Seed: 1, HeavyFraction: .1, MaxLag: 50 * time.Millisecond, SpikeFactor: 1, MaxResponseBytes: 16 << 20}
}
func (c Config) Validate() error {
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" {
		return fmt.Errorf("url must be an absolute HTTP URL without a fragment")
	}
	if math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) || c.Rate <= 0 || c.Rate > 1e7 {
		return fmt.Errorf("rate must be in (0, 10000000]")
	}
	if c.Duration <= 0 || c.Duration > 24*time.Hour || c.Warmup < 0 || c.Warmup > 24*time.Hour || c.Timeout <= 0 || c.Timeout > time.Hour {
		return fmt.Errorf("invalid duration, warmup, or timeout")
	}
	if c.Workers < 1 || c.Workers > 65536 || c.MaxLag < 0 || c.MaxLag > time.Hour {
		return fmt.Errorf("invalid workers or maximum lag")
	}
	if math.IsNaN(c.HeavyFraction) || c.HeavyFraction < 0 || c.HeavyFraction > 1 {
		return fmt.Errorf("heavy fraction must be in [0,1]")
	}
	if math.IsNaN(c.SpikeFactor) || math.IsInf(c.SpikeFactor, 0) || c.SpikeFactor < 1 || c.SpikeFactor > 1000 || c.Rate*c.SpikeFactor > 1e7 {
		return fmt.Errorf("invalid spike factor or effective rate")
	}
	if c.SpikeStart < 0 || c.SpikeEnd < 0 || c.SpikeStart > c.SpikeEnd || c.SpikeEnd > c.Duration || (c.SpikeFactor > 1 && c.SpikeStart == c.SpikeEnd) {
		return fmt.Errorf("invalid spike interval")
	}
	if c.MaxResponseBytes < 1 || c.MaxResponseBytes > 64<<20 {
		return fmt.Errorf("max response bytes must be in [1, 67108864]")
	}
	if c.Records != "" && c.RecordsWriter != nil {
		return fmt.Errorf("use records path or writer, not both")
	}
	if c.Records != "" && c.Output != "" {
		records, err := filepath.Abs(c.Records)
		if err != nil {
			return err
		}
		output, err := filepath.Abs(c.Output)
		if err != nil {
			return err
		}
		if records == output {
			return fmt.Errorf("JSON output and CSV records must use different paths")
		}
	}
	return nil
}

type Outcomes struct {
	Success        uint64 `json:"success"`
	HTTPError      uint64 `json:"http_error"`
	TransportError uint64 `json:"transport_error"`
	ClientTimeout  uint64 `json:"client_timeout"`
	ProxyTimeout   uint64 `json:"proxy_timeout"`
	ProxyRejection uint64 `json:"proxy_rejection"`
	GeneratorMiss  uint64 `json:"generator_miss"`
}

func (o Outcomes) Total() uint64 {
	return o.Success + o.HTTPError + o.TransportError + o.ClientTimeout + o.ProxyTimeout + o.ProxyRejection + o.GeneratorMiss
}
func (o *Outcomes) add(kind string) {
	switch kind {
	case "success":
		o.Success++
	case "http_error":
		o.HTTPError++
	case "transport_error":
		o.TransportError++
	case "client_timeout":
		o.ClientTimeout++
	case "proxy_timeout":
		o.ProxyTimeout++
	case "proxy_rejection":
		o.ProxyRejection++
	case "generator_miss":
		o.GeneratorMiss++
	}
}

type Quantiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}
type PhaseSummary struct {
	Offered  uint64   `json:"offered"`
	Outcomes Outcomes `json:"outcomes"`
}
type Summary struct {
	StartAt               string       `json:"start_at"`
	Offered               uint64       `json:"offered"`
	Outcomes              Outcomes     `json:"outcomes"`
	DurationSeconds       float64      `json:"duration_seconds"`
	DrainSeconds          float64      `json:"drain_seconds"`
	Throughput            float64      `json:"throughput"`
	SuccessLatencyMS      Quantiles    `json:"success_latency_ms"`
	AllLatencyMS          Quantiles    `json:"all_latency_ms"`
	ScheduledLatencyMS    Quantiles    `json:"scheduled_latency_ms"`
	SuccessLatencyCount   uint64       `json:"success_latency_count"`
	AllLatencyCount       uint64       `json:"all_latency_count"`
	ScheduledLatencyCount uint64       `json:"scheduled_latency_count"`
	Seed                  int64        `json:"seed"`
	Rate                  float64      `json:"rate"`
	Workers               int          `json:"workers"`
	Timeout               string       `json:"timeout"`
	RuntimeVersion        string       `json:"runtime_version"`
	HistogramPrecision    string       `json:"histogram_precision"`
	Warmup                PhaseSummary `json:"warmup"`
}

func quantiles(h *hist.Histogram) Quantiles {
	return Quantiles{h.Quantile(.5), h.Quantile(.95), h.Quantile(.99)}
}

// scheduledOffset inverts the integrated offered rate. Offer zero occurs at time
// zero. Spike windows are half-open and apply only to measured requests.
func scheduledOffset(index int64, c Config, measured bool) time.Duration {
	seconds := float64(index) / c.Rate
	if measured && c.SpikeFactor > 1 {
		start, end := c.SpikeStart.Seconds(), c.SpikeEnd.Seconds()
		spikeUnits := (end - start) * c.SpikeFactor
		if seconds >= start {
			if seconds < start+spikeUnits {
				seconds = start + (seconds-start)/c.SpikeFactor
			} else {
				seconds = end + (seconds - start - spikeUnits)
			}
		}
	}
	nanoseconds := math.Round(seconds * float64(time.Second))
	// Tiny positive rates can put the next offer centuries beyond the run or
	// produce +Inf. Clamp before converting: out-of-range float-to-int casts
	// can become negative durations on amd64 and keep the schedule looping.
	if nanoseconds >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(nanoseconds)
}
func cost(id, seed int64, fraction float64) string {
	x := uint64(id) ^ uint64(seed)
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	if float64(x>>11)/float64(uint64(1)<<53) < fraction {
		return "heavy"
	}
	return "light"
}

type offer struct {
	id          int64
	phase, cost string
	scheduled   time.Time
}
type result struct {
	offer
	kind                  string
	status                int
	dispatched, completed time.Time
}

// Run records every offered request exactly once. It stops scheduling on context
// cancellation, drains dispatched requests, and returns the context error. Latency
// histograms include dispatched requests only; generator misses have no latency.
// A caller-supplied RecordsWriter must return promptly and is not closed by Run.
func Run(ctx context.Context, c Config) (Summary, error) {
	summary := Summary{DurationSeconds: c.Duration.Seconds(), Seed: c.Seed, Rate: c.Rate, Workers: c.Workers, Timeout: c.Timeout.String(), RuntimeVersion: runtime.Version(), HistogramPrecision: "upper bucket boundary; <=1% relative error + 1ns"}
	if err := c.Validate(); err != nil {
		return summary, err
	}
	var file *os.File
	writer := c.RecordsWriter
	if c.Records != "" {
		var err error
		file, err = os.Create(c.Records)
		if err != nil {
			return summary, err
		}
		writer = file
	}
	var csvWriter *csv.Writer
	var buffer *bufio.Writer
	if writer != nil {
		buffer = bufio.NewWriterSize(writer, 64<<10)
		csvWriter = csv.NewWriter(buffer)
		if err := csvWriter.Write([]string{"id", "phase", "outcome", "status", "scheduled_ms", "dispatch_ms", "completion_ms", "cost"}); err != nil {
			if file != nil {
				file.Close()
			}
			return summary, err
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: c.Timeout, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: false, MaxIdleConns: c.Workers, MaxIdleConnsPerHost: c.Workers, MaxConnsPerHost: c.Workers, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: c.Timeout, MaxResponseHeaderBytes: 64 << 10, TLSHandshakeTimeout: c.Timeout}
	// An explicitly empty TLSNextProto disables automatic HTTP/2 negotiation.
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	jobs := make(chan offer)
	events := make(chan result, c.Workers)
	var workers sync.WaitGroup
	workers.Add(c.Workers)
	ready := make(chan struct{}, c.Workers)
	for i := 0; i < c.Workers; i++ {
		go func() {
			defer workers.Done()
			ready <- struct{}{}
			for job := range jobs {
				events <- execute(runCtx, client, c, job)
			}
		}()
	}
	for i := 0; i < c.Workers; i++ {
		<-ready
	}
	origin := time.Now()
	if !c.StartAt.IsZero() {
		until := time.Until(c.StartAt)
		origin = time.Now().Add(until)
		summary.StartAt = c.StartAt.UTC().Format(time.RFC3339Nano)
	} else {
		summary.StartAt = origin.UTC().Format(time.RFC3339Nano)
	}
	type collection struct {
		summary Summary
		err     error
	}
	collected := make(chan collection, 1)
	go func() {
		var success, all, scheduled hist.Histogram
		var writeErr error
		for event := range events {
			phase := &summary.Warmup
			if event.phase == "measurement" {
				summary.Offered++
				summary.Outcomes.add(event.kind)
				if !event.dispatched.IsZero() {
					all.Record(event.completed.Sub(event.dispatched))
					scheduled.Record(event.completed.Sub(event.scheduled))
					if event.kind == "success" {
						success.Record(event.completed.Sub(event.dispatched))
					}
				}
			} else {
				phase.Offered++
				phase.Outcomes.add(event.kind)
			}
			if csvWriter != nil && writeErr == nil {
				dispatch := ""
				if !event.dispatched.IsZero() {
					dispatch = milliseconds(event.dispatched.Sub(origin))
				}
				row := []string{strconv.FormatInt(event.id, 10), event.phase, event.kind, strconv.Itoa(event.status), milliseconds(event.scheduled.Sub(origin)), dispatch, milliseconds(event.completed.Sub(origin)), event.cost}
				if err := csvWriter.Write(row); err != nil {
					writeErr = err
					cancel()
				}
			}
		}
		if csvWriter != nil {
			csvWriter.Flush()
			if err := csvWriter.Error(); writeErr == nil {
				writeErr = err
			}
			if err := buffer.Flush(); writeErr == nil {
				writeErr = err
			}
		}
		if file != nil {
			if err := file.Close(); writeErr == nil {
				writeErr = err
			}
		}
		summary.SuccessLatencyMS = quantiles(&success)
		summary.AllLatencyMS = quantiles(&all)
		summary.ScheduledLatencyMS = quantiles(&scheduled)
		summary.SuccessLatencyCount = success.Count()
		summary.AllLatencyCount = all.Count()
		summary.ScheduledLatencyCount = scheduled.Count()
		if summary.Outcomes.Total() != summary.Offered || summary.Warmup.Outcomes.Total() != summary.Warmup.Offered {
			writeErr = errors.Join(writeErr, fmt.Errorf("outcome conservation failed"))
		}
		collected <- collection{summary, writeErr}
	}()
	var id int64
	schedule := func(phase string, start time.Time, duration time.Duration) bool {
		timer := time.NewTimer(0)
		defer timer.Stop()
		for index := int64(0); ; index++ {
			offset := scheduledOffset(index, c, phase == "measurement")
			if offset >= duration {
				break
			}
			due := start.Add(offset)
			if runCtx.Err() != nil {
				return false
			}
			if wait := time.Until(due); wait > 0 {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(wait)
				select {
				case <-timer.C:
				case <-runCtx.Done():
					return false
				}
			}
			job := offer{id: id, phase: phase, cost: cost(id, c.Seed, c.HeavyFraction), scheduled: due}
			id++
			if time.Since(due) > c.MaxLag {
				events <- result{offer: job, kind: "generator_miss", completed: time.Now()}
				continue
			}
			select {
			case jobs <- job:
			default:
				events <- result{offer: job, kind: "generator_miss", completed: time.Now()}
			}
		}
		// Keep the full offered interval even when the final request completes early.
		remaining := time.Until(start.Add(duration))
		if remaining > 0 {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(remaining)
			select {
			case <-timer.C:
			case <-runCtx.Done():
				return false
			}
		}
		return true
	}
	measurementStart := origin.Add(c.Warmup)
	if c.Warmup == 0 || schedule("warmup", origin, c.Warmup) {
		schedule("measurement", measurementStart, c.Duration)
	}
	scheduleEnd := time.Now()
	close(jobs)
	workers.Wait()
	drainEnd := time.Now()
	close(events)
	output := <-collected
	summary = output.summary
	// On early cancellation, report the actual measured offered interval.
	if scheduleEnd.Before(measurementStart.Add(c.Duration)) {
		summary.DurationSeconds = math.Max(0, scheduleEnd.Sub(measurementStart).Seconds())
	}
	summary.DrainSeconds = drainEnd.Sub(scheduleEnd).Seconds()
	if summary.DurationSeconds > 0 {
		summary.Throughput = float64(summary.Outcomes.Success) / summary.DurationSeconds
	}
	return summary, errors.Join(output.err, ctx.Err())
}
func milliseconds(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 6, 64)
}
func execute(ctx context.Context, client *http.Client, c Config, job offer) result {
	event := result{offer: job, dispatched: time.Now()}
	requestCtx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	target, _ := url.Parse(c.URL)
	q := target.Query()
	q.Set("id", strconv.FormatInt(job.id, 10))
	q.Set("cost", job.cost)
	target.RawQuery = q.Encode()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, target.String(), nil)
	if err != nil {
		event.kind = "transport_error"
		event.completed = time.Now()
		return event
	}
	response, err := client.Do(request)
	if err == nil {
		event.status = response.StatusCode
		n, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, c.MaxResponseBytes+1))
		closeErr := response.Body.Close()
		err = errors.Join(readErr, closeErr)
		if n > c.MaxResponseBytes {
			err = fmt.Errorf("response exceeds %d bytes", c.MaxResponseBytes)
		}
	}
	event.completed = time.Now()
	if err != nil {
		event.kind = "transport_error"
		var timeout net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
			event.kind = "client_timeout"
		}
		return event
	}
	marker := response.Header.Get("X-Latency-Proxy-Error")
	switch {
	case marker == "deadline":
		event.kind = "proxy_timeout"
	case marker != "" && response.StatusCode == 503:
		event.kind = "proxy_rejection"
	case response.StatusCode >= 400:
		event.kind = "http_error"
	default:
		event.kind = "success"
	}
	return event
}
