# Latency-aware HTTP reverse proxy

A Go systems engineering project comparing round-robin, exact least-outstanding,
and power-of-two-choices Peak EWMA routing. The goal is a reliable proxy and a
reproducible evaluation, including cases where latency-aware routing loses.

The Peak EWMA estimator is based on [Finagle at an immutable commit](docs/peak-ewma.md).
This project implements the estimator and routing explicitly; HTTP parsing,
forwarding, and connection pooling use Go's standard library.

The [evaluation](docs/evaluation.md) retains **93 Docker trials and 626,400
measured offers**, including impaired trials. Peak EWMA helped during persistent
slowdowns and was modestly worse than least-outstanding in the mixed-cost stress
test. The raw results, limits, and interpretation are included.

## Quick start

Requires Go 1.26.1 and Python 3.10 or newer. No third-party Go or Python packages.

```sh
make check
make smoke
```

`make smoke` builds three binaries, starts three backend processes and one proxy,
runs a short open-loop workload against each policy, validates every offered
request, writes results beneath `bench/runs/`, and stops its child processes.
It needs permission to bind local TCP ports. Smoke results verify the experiment
pipeline; they are too short to support performance conclusions.

For manual exploration, run one backend in each of three terminals, changing the
port to 9001, 9002, and 9003:

```sh
make build
./bin/backend -listen 127.0.0.1:9001 -config configs/backend.json
```

In another terminal:

```sh
./bin/proxy -config configs/proxy.json -policy peak-ewma
```

Then send requests or a repeatable workload:

```sh
curl 'http://127.0.0.1:8080/work?id=1&cost=light'
curl 'http://127.0.0.1:8081/metrics'
./bin/loadgen -url http://127.0.0.1:8080/work -rate 400 -duration 15s \
  -warmup 2s -seed 1 -workers 256 -output /tmp/result.json \
  -records /tmp/requests.csv
```

Send SIGINT or SIGTERM to drain the proxy. It stops admitting new requests, gives
existing requests a grace period, and then cancels remaining operations.

To see adaptation manually, start backend 9001 with
`-config configs/backend-slowdown.json`. It slows down from second 5 through
second 10, then recovers. `configs/backend-faults.json` demonstrates HTTP failures
and connection drops. The benchmark runner synchronizes these phases across
processes automatically; manual launches use each backend's own startup time.

## Routing policies

| Policy flag | Decision | Signal |
| --- | --- | --- |
| `round-robin` | Cycle through eligible backends | Non-adaptive baseline |
| `least-outstanding` | Minimum unfinished requests over the entire eligible pool, random ties | Current load |
| `peak-ewma` | Sample two distinct eligible backends and minimize `estimated_latency * (outstanding + 1)` | Load and latency history |

All policies have identical health rules, resource limits, and timeouts. Eligible
backends must be healthy and below their outstanding-request limit. Round-robin
therefore shares mandatory capacity protection, even though its ranking does not
adapt. There are no application-level retries.

Peak EWMA immediately adopts an observed latency spike, then recovers gradually.
Its source pin, exact equations, idle decay, cold-start prior, failure penalties,
and cancellation accounting are in [the algorithm contract](docs/peak-ewma.md).

## What is included

- HTTP/1.1 proxy with pooled backend connections, bounded admission and body
  buffers, deadlines, health exclusion/recovery, and graceful shutdown.
- Configurable bounded backend simulators with deterministic request costs,
  slowdown/recovery phases, status failures, and connection drops.
- Open-loop workload generator with bounded workers, common experiment origins,
  explicit generator misses, streamed per-request records, and fixed-memory
  latency histograms.
- Correctness, concurrency, HTTP integration, CLI, and benchmark-accounting tests.
- Repeated benchmark runner with per-process resource measurements, a Docker
  path with hard resource limits, and raw artifacts for independent analysis.

Read [architecture](docs/architecture.md), [benchmark instructions](docs/benchmarking.md),
and [evaluation](docs/evaluation.md) for design decisions and measured limitations.

## Scope

One proxy process, HTTP/1.1, fixed plain-HTTP backend origins, bounded complete
request/response buffering. This is a portfolio implementation, with documented
limits, rather than a production replacement for a general-purpose proxy.
Streaming responses, WebSockets, CONNECT, HTTP/2, TLS termination, discovery,
distributed coordination, and caching are outside this version.
