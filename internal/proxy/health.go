package proxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

func (p *Proxy) StartHealth() {
	p.startHealth.Do(func() {
		for i, address := range p.Config.Backends {
			p.healthWG.Go(func() {
				ticker := time.NewTicker(ms(p.Config.ProbeIntervalMS))
				defer ticker.Stop()
				for {
					select {
					case <-p.root.Done():
						return
					case <-ticker.C:
						p.probe(i, address)
					}
				}
			})
		}
	})
}

func (p *Proxy) probe(i int, address string) {
	ctx, cancel := context.WithTimeout(p.root, ms(p.Config.ProbeTimeoutMS))
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(address, "/")+"/healthz", nil)
	resp, e := p.probeTransport.RoundTrip(req)
	success := false
	if e == nil {
		n, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4097))
		resp.Body.Close()
		success = resp.StatusCode >= 200 && resp.StatusCode < 300 && readErr == nil && n <= 4096
	}
	if p.root.Err() == nil {
		p.Balancer.Probe(i, success)
	}
}
