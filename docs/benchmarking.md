# Reproducing the evaluation

## Correctness and smoke checks

```sh
make check
make smoke
```

`make check` runs formatting verification, vet, Go tests, race tests, and Python
benchmark-accounting tests. HTTP integration tests bind localhost sockets.

## Local process measurements

```sh
make build
python3 bench/runner.py --output bench/runs/local-$(date -u +%Y%m%dT%H%M%SZ)
```

The default matrix runs five scenarios, three policies, and three seeds. Each
trial uses a 2-second warmup and 15-second measured interval, restarts every
process, and rotates policy order between repetitions. Choose a fresh output
directory; existing trial directories are never overwritten.

Repeat at a different offered-load level without changing backend work or proxy
limits:

```sh
python3 bench/runner.py --rate-scale 0.5 --output bench/runs/local-low
```

The same option applies to Docker runs. For example, `--scenarios mixed
--rate-scale 1.75` increases the mixed-cost scenario from 400 to 700 requests/s.

Local mode sets `GOMAXPROCS=2` and `GOMEMLIMIT=192MiB` for every role and every
policy. These are runtime controls: **they do not enforce an OS CPU quota or a
hard memory limit**. The processes share the host. The runner samples CPU time
and RSS with `ps`, and records kernel `wait4` lifetime CPU seconds and peak RSS
for proxy, each backend, and generator separately. CPU and peak memory include
startup, warmup, and drain. The controller and host background activity are
outside those role measurements. Use the Docker path for enforced isolation.

## Docker with hard resource limits

Start a Docker daemon, then:

```sh
make build
docker build -t latencyproxy:bench .
python3 bench/runner.py --docker --image latencyproxy:bench \
  --startup-delay 15 --cpus 2 --memory 256m \
  --output bench/runs/docker-$(date -u +%Y%m%dT%H%M%SZ)
```

Every role gets its own container with `--cpus=2`, `--memory=256m`,
`--memory-swap=256m`, and a process-count bound. The Go runtime settings remain
identical. Backend/proxy traffic uses a fresh private Docker network. Published
metrics/readiness ports bind only to host loopback. The runner records image ID,
exact commands, Docker CPU/memory samples, exit statuses, and OOM state. The
samples use the [Docker Engine statistics API](https://docs.docker.com/reference/api/engine/version/v1.52/).
CPU is cumulative through the last successful sample. Peak memory is sampled
cgroup usage, rather than an exact kernel peak RSS; sub-sample peaks and shutdown
CPU can be missed. Raw timestamps and the 250 ms target cadence are retained.
The
`GOMEMLIMIT` setting is fixed at 192 MiB; use the default 256 MiB or larger hard
memory budget. A smaller hard budget can kill the process and invalidates a run.

The runner resolves the image reference to one immutable image ID before starting
the matrix. Sampling failures do not bypass container cleanup; cleanup attempts
all roles and the network, then reports any failures. An interrupted controller
can require manual cleanup of containers/network named `latencybench-<PID>`.
Do not claim Docker results from a local run. Cross-architecture runs, Docker VM
quotas, and host contention must be stated when interpreting measurements.

## Workloads and shared settings

| Scenario | Offered load | Backend behavior |
| --- | --- | --- |
| `steady` | 700 requests/s | Three equal 8 ms backends |
| `slowdown` | 700 requests/s | Backend 0 becomes 15x slower from 25% to 60% of measurement, then recovers |
| `heterogeneous` | 700 requests/s | Baseline backend costs 4, 12, and 24 ms |
| `mixed` | 400 requests/s | 10% heavy requests cost 10x baseline, identical servers |
| `spike` | 400 requests/s; 6x from 25% to 60% | Identical backends, temporary overload |

Each backend has four service slots, 64 queue slots, seeded +/-1 ms nominal
jitter, and a 128-byte response. The proxy uses the same configuration in
`configs/proxy.json` except routing policy, seed, and run-local addresses. The
generator has 256 workers, a 2-second client timeout, and a 50 ms maximum dispatch
lag. Backend fault phases and the generator share a future `-start-at` timestamp,
converted once to each process's monotonic clock. The runner aborts if processes
are not ready before the common origin. Fault phases include warmup; spike windows
are relative to the measured interval.

Sleep-based scenarios demonstrate routing and queueing, not CPU-intensive service
performance. Increase measured duration, replicate count, and workload coverage
for stronger conclusions. The default 10-second estimator decay constant gives a
6.93-second half-life, so short recovery windows only reveal early recovery.

In the bundled trace, jitter and cost classification hash the same request ID and
seed. Heavy requests consequently correlate with the low end of nominal jitter;
with the default 8 ms baseline they take approximately 70-72 ms of simulated
service before queueing. The 10x multiplier applies to that request's jittered
baseline. This is a defined repeatable workload, rather than independent random
cost and jitter. The backend and generator expose separate seed settings for
experiments with different correlations.

## Offered-request accounting

The generator assigns one ID and terminal record to every scheduled offer. It
uses these disjoint categories:

```text
offered = success + http_error + transport_error + client_timeout
          + proxy_timeout + proxy_rejection + generator_miss
```

A backend 503 is `http_error`. A proxy-generated marked 503 is `proxy_rejection`.
A marked 504 is `proxy_timeout`. Worker exhaustion or excessive scheduler lag is
`generator_miss`, with no invented HTTP latency. All warmup offers are accounted
for separately. On cancellation the generator reports only offers already made,
returns an error, and reports the actual elapsed offered interval; the runner
rejects canceled or incomplete trials.

Latency populations are deliberately separate:

- `success_latency_ms`: dispatched-to-full-response-completion for successful
  requests, excluding warmup, errors, rejections, and generator misses.
- `all_latency_ms`: the same measurement for all dispatched terminal outcomes.
- `scheduled_latency_ms`: scheduled-to-terminal time for dispatched requests,
  including scheduler lag.
- Generator misses: counts and fractions, with timestamps in the raw CSV.

Throughput is measured successes divided by the configured offered interval.
Drain duration is separate. Histogram quantiles use upper bucket boundaries,
with at most approximately 1% relative error plus one nanosecond; empty
populations report zero with a zero count. Successful latency alone cannot
establish a win if a policy rejects more traffic.

## Artifacts and aggregation

The output contains machine/toolchain/git provenance and binary SHA-256 values.
Each trial includes backend/proxy configs, common origin, exact command lines,
offered-trace digest, summary JSON, compressed per-offer CSV, compressed sampled
metrics/resources, final proxy state, per-role lifetime resources, and logs.
The runner checks outcome conservation, expected scheduled count, unique
contiguous IDs, CSV-summary agreement, matching offered traces across policies,
and successful process exits. A failed process or OOM invalidates the trial.

`comparison.md` reports medians of per-run latency, throughput, and resources.
Offered counts and terminal outcome counters are totals across repetitions, so
isolated errors and generator misses cannot disappear behind a median of zero.
Its p99 range is
the minimum and maximum per-run p99. These are neither pooled quantiles nor
confidence intervals. Raw records permit independent recomputation and time-bucket
analysis. Do not average per-run p99 values and call the result an overall p99.

To independently audit a completed archive and derive exact CSV-based latency
statistics for scheduled-request cohorts:

```sh
python3 bench/analyze.py bench/results/2026-10-20-docker
```

This verifies the complete matrix, offered-trace digests, CSV outcomes, process
exits, recorded image identity, observed limits, and zero final outstanding work.
It writes per-second/phase cohort CSVs, an audit report, and SHA-256 manifest.
Offline analysis retains per-trial samples for exact quantiles; that memory use
is separate from the bounded serving and generator processes.

For selection overhead independently of HTTP:

```sh
go test ./internal/balancer -run '^$' -bench . -benchmem
```

That microbenchmark uses three backends and one caller. It is not a proxy
throughput test or a contention/scalability claim.
