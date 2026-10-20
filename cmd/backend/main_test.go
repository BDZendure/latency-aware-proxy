package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadConfigAndRejectMalformed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"service_ms":0,"capacity":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := readConfig(path)
	if err != nil || c.Capacity != 2 || c.ResponseBytes != 128 {
		t.Fatal(c, err)
	}
	for _, raw := range []string{`null`, `{"capacity":0}`, `{"unknown":true}`, `{} {}`, `{"service_ms":NaN}`} {
		os.WriteFile(path, []byte(raw), 0600)
		if _, err := readConfig(path); err == nil {
			t.Fatal(raw)
		}
	}
}
func TestCanceledBackendShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, []string{"-listen", "127.0.0.1:0"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, []string{"-invalid"}, io.Discard); err == nil {
		t.Fatal("accepted invalid flag")
	}
}
func TestBackendStartFlagValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, []string{"-start-at", "not-a-time"}, io.Discard); err == nil {
		t.Fatal("invalid start-at accepted")
	}
	if err := run(ctx, []string{"-listen", "127.0.0.1:0", "-start-at", time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano)}, io.Discard); err != nil {
		t.Fatal(err)
	}
}
