package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"latencyproxy/internal/loadgen"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestCLIOutputAndValidation(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer s.Close()
	path := filepath.Join(t.TempDir(), "summary.json")
	var stdout bytes.Buffer
	err := run(context.Background(), []string{"-url", s.URL, "-duration", "20ms", "-rate", "100", "-output", path}, &stdout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var got loadgen.Summary
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Offered != 2 {
		t.Fatal(got)
	}
	b, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(b, stdout.Bytes()) {
		t.Fatal(string(b), err)
	}
	if err := run(context.Background(), []string{"-url", s.URL, "-rate", "0"}, io.Discard, io.Discard); err == nil {
		t.Fatal("accepted invalid rate")
	}
}
func TestArtifactConservationAndMarkedRejectionProvenance(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.Atoi(r.URL.Query().Get("id"))
		switch id % 3 {
		case 0:
			w.Header().Set("X-Latency-Proxy-Error", "overload")
			w.WriteHeader(503)
		case 1:
			w.WriteHeader(503)
		case 2:
			w.Header().Set("X-Latency-Proxy-Error", "deadline")
			w.WriteHeader(504)
		}
	}))
	defer s.Close()
	dir := t.TempDir()
	output, records := filepath.Join(dir, "summary.json"), filepath.Join(dir, "records.csv")
	start := time.Now().Add(30 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	if err := run(context.Background(), []string{"-url", s.URL, "-duration", "60ms", "-rate", "200", "-workers", "16", "-output", output, "-records", records, "-start-at", start}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var summary loadgen.Summary
	if err := json.Unmarshal(data, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Offered != 12 || summary.Outcomes.Total() != summary.Offered || summary.Outcomes.ProxyRejection != 4 || summary.Outcomes.HTTPError != 4 || summary.Outcomes.ProxyTimeout != 4 {
		t.Fatal(summary)
	}
	f, err := os.Open(records)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows)-1 != int(summary.Offered) {
		t.Fatal("record count", len(rows), summary.Offered)
	}
	counts := map[string]uint64{}
	for _, row := range rows[1:] {
		id, _ := strconv.Atoi(row[0])
		expected := []string{"proxy_rejection", "http_error", "proxy_timeout"}[id%3]
		if row[2] != expected {
			t.Fatalf("ID %d: expected %s based on server marker, got %s", id, expected, row[2])
		}
		counts[row[2]]++
	}
	if counts["proxy_rejection"] != summary.Outcomes.ProxyRejection || counts["http_error"] != summary.Outcomes.HTTPError || counts["proxy_timeout"] != summary.Outcomes.ProxyTimeout {
		t.Fatal("artifact counts differ", counts, summary)
	}
}
