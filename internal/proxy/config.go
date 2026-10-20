package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
)

type Config struct {
	Listen            string   `json:"listen"`
	Admin             string   `json:"admin"`
	Backends          []string `json:"backends"`
	Policy            string   `json:"policy"`
	GlobalLimit       int      `json:"global_limit"`
	BackendLimit      int      `json:"backend_limit"`
	ConnectionLimit   int      `json:"connection_limit"`
	MaxRequestBytes   int      `json:"max_request_bytes"`
	MaxResponseBytes  int      `json:"max_response_bytes"`
	MaxHeaderBytes    int      `json:"max_header_bytes"`
	RequestTimeoutMS  int      `json:"request_timeout_ms"`
	ShutdownGraceMS   int      `json:"shutdown_grace_ms"`
	HeaderTimeoutMS   int      `json:"header_timeout_ms"`
	IdleTimeoutMS     int      `json:"idle_timeout_ms"`
	ColdMS            int      `json:"cold_ms"`
	DecayMS           int      `json:"decay_ms"`
	FailurePenaltyMS  int      `json:"failure_penalty_ms"`
	ProbeIntervalMS   int      `json:"probe_interval_ms"`
	ProbeTimeoutMS    int      `json:"probe_timeout_ms"`
	FailureThreshold  int      `json:"failure_threshold"`
	RecoveryThreshold int      `json:"recovery_threshold"`
	Seed              uint64   `json:"seed"`
}

func DefaultConfig() Config {
	return Config{Listen: "127.0.0.1:8080", Admin: "127.0.0.1:8081", Policy: "round-robin", GlobalLimit: 128, BackendLimit: 32, ConnectionLimit: 256, MaxRequestBytes: 64 << 10, MaxResponseBytes: 1 << 20, MaxHeaderBytes: 32 << 10, RequestTimeoutMS: 1000, ShutdownGraceMS: 5000, HeaderTimeoutMS: 2000, IdleTimeoutMS: 30000, ColdMS: 50, DecayMS: 10000, FailurePenaltyMS: 1000, ProbeIntervalMS: 1000, ProbeTimeoutMS: 200, FailureThreshold: 3, RecoveryThreshold: 2, Seed: 1}
}

func LoadConfig(path string) (Config, error) {
	c := DefaultConfig()
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return c, errors.New("configuration must contain one JSON object")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.Policy != "round-robin" && c.Policy != "least-outstanding" && c.Policy != "peak-ewma" {
		return fmt.Errorf("unknown policy %q", c.Policy)
	}
	if c.Listen == "" || c.Admin == "" || len(c.Backends) == 0 || len(c.Backends) > 256 {
		return errors.New("listen/admin and 1..256 backends required")
	}
	if c.GlobalLimit < 1 || c.GlobalLimit > 4096 || c.BackendLimit < 1 || c.BackendLimit > 4096 || c.ConnectionLimit < 1 || c.ConnectionLimit > 65536 {
		return errors.New("concurrency limits out of range")
	}
	if c.MaxRequestBytes < 1 || c.MaxRequestBytes > 16<<20 || c.MaxResponseBytes < 1 || c.MaxResponseBytes > 16<<20 || c.MaxHeaderBytes < 1024 || c.MaxHeaderBytes > 1<<20 {
		return errors.New("body/header limits out of range")
	}
	for _, v := range []int{c.RequestTimeoutMS, c.ShutdownGraceMS, c.HeaderTimeoutMS, c.IdleTimeoutMS, c.ColdMS, c.DecayMS, c.FailurePenaltyMS, c.ProbeIntervalMS, c.ProbeTimeoutMS} {
		if v < 1 || v > 3600000 {
			return errors.New("durations must be 1..3600000 milliseconds")
		}
	}
	if c.FailureThreshold < 1 || c.RecoveryThreshold < 1 {
		return errors.New("health thresholds must be positive")
	}
	seen := map[string]bool{}
	for _, address := range c.Backends {
		u, e := url.Parse(address)
		if e != nil || u.Scheme != "http" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
			return fmt.Errorf("backend must be a plain HTTP origin: %q", address)
		}
		key := u.Scheme + "://" + u.Host
		if seen[key] {
			return fmt.Errorf("duplicate backend %q", address)
		}
		seen[key] = true
	}
	return nil
}
