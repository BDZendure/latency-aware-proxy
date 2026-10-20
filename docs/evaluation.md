# Evaluation

## What these measurements establish

These experiments evaluate this implementation on synthetic services in Linux
containers on a shared Apple M3 Pro laptop. They are useful evidence about
routing and queueing behavior. They are not production capacity numbers, a proof
of universal superiority, or a new load-balancing algorithm.

The initial matrix ran five scenarios, three policies, and three seeds: **45
trials**, each with a one-second warmup and 12-second measured interval. Every
role received a two-CPU quota and 256 MiB hard memory limit, with no additional
swap budget. Go used `GOMAXPROCS=2` and a 192 MiB soft memory target.
Docker Desktop supplied an 11-vCPU Linux/aarch64 VM with approximately 7.75 GiB
memory and cgroup v2. The Go toolchain and container build base were pinned.
CPU quotas do not provide dedicated physical cores or eliminate host/VM noise.

All policies used the same backends, costs, schedules, connection limits,
admission limits, deadlines, health rules, and body limits. Policy order rotated
between repetitions. An audit verified matching offered-trace hashes, unique
request IDs, terminal accounting, role exits, image identity, and zero final
outstanding reservations. The observed maxima were 32 outstanding per backend
and 98 admitted requests, within the limits of 32 and 128. Sampling cannot prove
the absence of brief excursions; atomic reservation enforces the invariant and
concurrency tests verify it.

The [original matrix](../bench/results/2026-10-20-docker/comparison.md),
[complete audit](../bench/results/2026-10-20-docker/audit.json),
[per-phase cohorts](../bench/results/2026-10-20-docker/phase-cohorts.csv), and
[provenance](../bench/results/2026-10-20-docker/provenance.json) are retained.
Each trial also contains raw compressed per-request records and metrics, exact
configuration/commands, per-role resource measurements, and logs.

## Original matrix, including impaired trials

Values below are medians of per-run successful-response p99 in milliseconds.
They are not pooled percentiles or confidence intervals.

| Scenario | Round-robin | Least outstanding | Peak EWMA |
| --- | ---: | ---: | ---: |
| Equal servers, 700 requests/s | 11.40 | 11.51 | 11.51 |
| Unequal server speeds, 700 requests/s | 202.15 | 27.36 | 27.36 |
| One server slows and recovers, 700 requests/s | 964.13 | 116.95 | 13.36 |
| Mixed costs, 400 requests/s | 74.74 | 74.00 | 74.00 |
| Traffic spike, 400 to 2,400 requests/s | 75.49 | 75.49 | 75.49 |

There were **388,800 measured offers**: 349,443 successes, 15 proxy timeouts,
38,775 proxy rejections, and 567 generator misses. HTTP errors, transport errors,
and client timeouts were zero. A further 26,100 warmup offers were accounted for
separately. The comparison table reports outcome totals across repetitions, so
isolated errors cannot disappear behind a median of zero.

Two slowdown trials had material generator interference: least-outstanding seed 2
missed 334 offers; Peak EWMA seed 2 missed 233, timed out one, and rejected 161.
The latter's successful p99 was 461.69 ms, while the other two Peak EWMA trials
were 13.36 ms. One metrics sample also failed. These trials remain in the raw
archive and aggregate counters. Missing offers change actual backend arrivals,
so their successful latencies are not clean evidence of routing quality.
The median alone hides that problem; the ranges and complete outcomes expose it.

## Replication and additional load levels

The complete slowdown comparison was repeated as a separate nine-trial block,
using the same three seeds, schedules, duration, limits, and image. It did not
replace or discard any initial trial. There were **zero generator misses** and
zero sampling failures in the replication:

| Policy | Successful p99 ms, median (range) | Successes / offered | Proxy timeouts |
| --- | ---: | ---: | ---: |
| Round-robin | 964.13 (964.13-964.13) | 25,192 / 25,200 | 8 |
| Least outstanding | 118.12 (118.12-120.49) | 25,200 / 25,200 | 0 |
| Peak EWMA | 13.63 (13.23-15.67) | 25,200 / 25,200 | 0 |

See the [replication results](../bench/results/2026-10-20-docker-slowdown-replication/comparison.md).
The improvement here is accompanied by complete successful throughput, not
achieved by rejecting additional offers.

An exploratory lower-load matrix used half the base rates, two seeds, and
eight-second measured intervals: **30 trials and 86,400 successful offers**, with
no errors, rejections, generator misses, or sampling failures. Slowdown p99 was
approximately 11.86 ms for Peak EWMA versus 117.54 ms for least-outstanding.
Conversely, the below-capacity traffic burst produced p99 values of **13.64 ms
for Peak EWMA, 12.59 ms for least-outstanding, and 11.80 ms for round-robin**.
The absolute difference is small and only two seeds were run, but it is a
measured case where Peak EWMA hurt latency. The lower-load matrix also uses a
shorter duration, so comparisons between its rows and the original matrix are
not a controlled single-factor load sweep. Comparisons within each matrix have
matched durations and limits. See [lower-load results](../bench/results/2026-10-20-docker-low/comparison.md).

The mixed-cost workload was additionally tested at **700 requests/s**, retaining
12-second measurements and three seeds. All **75,600 offers succeeded**:

| Policy | p50 ms | p95 ms | p99 ms, median (range) |
| --- | ---: | ---: | ---: |
| Round-robin | 18.19 | 92.11 | 137.13 (119.30-147.03) |
| Least outstanding | 10.53 | 73.27 | 85.06 (85.06-86.77) |
| Peak EWMA | 11.17 | 74.00 | 87.64 (86.77-87.64) |

Peak EWMA improved on round-robin but was modestly worse than exact
least-outstanding, including about 0.65 ms at p50. This is consistent with
unpredictable request cost making backend history less useful, but the experiment
does not isolate that cause. The candidate sets also differ: exact
least-outstanding examines all three backends, while Peak EWMA examines two.
An additional P2C least-outstanding ablation would be needed to isolate the
effect of latency history from candidate sampling. See [mixed-cost stress results](../bench/results/2026-10-20-docker-mixed-high/comparison.md).

Across all four blocks, **93 trials accounted for 626,400 measured offers**:
587,035 successes, 23 proxy timeouts, 38,775 rejections, and 567 generator misses.
The 47,400 warmup offers are additional and separately recorded. All initial
impaired trials remain included in these totals.

## Interpreting the policies

Peak EWMA has a useful signal that least-outstanding lacks: an idle backend can
still be known to be slow. Least-outstanding occasionally selects that backend
because it has little unfinished work. An established latency estimate can avoid
those expensive probes. The slowdown scenario shows this mechanism clearly in
the two undisturbed original Peak EWMA trials.

With stable heterogeneous servers, exact least-outstanding was already as good
as Peak EWMA at the reported p99. This is a strong baseline with a global view
of three servers, compared with Peak EWMA's two random candidates. Comparing
only against round-robin would overstate the added value of latency history.

Equal servers produced no useful Peak EWMA latency improvement. The small
differences around 11-12 ms do not justify a ranking from three laptop trials.
With mixed request costs, the request's own service time dominated the tail at
400 requests/s. A heavy request raises an entire backend's estimate even when
its subsequent light requests would be fast. That is a potential source of
misrouting as utilization increases; the estimator does not know request cost.

During the traffic spike, approximately 32.5% of offers were rejected across
policies. Successful p99 values around 75 ms must be read alongside those
rejections. Successful throughput was about 742 requests/s averaged across the
whole measured interval. Peak EWMA cannot create capacity; bounded admission
and per-backend queueing dominated this experiment. Small throughput differences
among the policies are not meaningful evidence of a win.

## Recovery and tuning tradeoffs

The default 10-second decay constant has a 6.93-second half-life. Idle estimates
decay toward zero, enabling eventual exploration, but a recovered backend can
remain unattractive for seconds. The original fault ends at 7.2 measured seconds,
leaving only 4.8 seconds of recovery observation. The cohort CSVs show early
recovery; they do not establish complete convergence. Retaining history can
avoid repeated stragglers while temporarily wasting recovered capacity.

The clean replication makes this tradeoff visible in exact CSV-based
scheduled-request cohorts. Values are medians of per-run successful p99:

| Scheduled cohort | Round-robin ms | Least outstanding ms | Peak EWMA ms |
| --- | ---: | ---: | ---: |
| Before fault, 0-3 seconds | 11.30 | 11.51 | 11.70 |
| Slowdown, 3-7.2 seconds | 978.31 | 130.29 | 14.01 |
| Early recovery, 7.2-12 seconds | 87.34 | 11.49 | 13.38 |

Peak EWMA's tail remained low during the fault, but it was slower than
least-outstanding during recovery. In the approximately 4.6-second windows
bounded by post-recovery metrics samples, the formerly slow backend received
only 6, 6, and 43 new assignments under Peak EWMA. Least-outstanding assigned
that backend 1,059-1,075 requests in corresponding windows. The latency history
was still discouraging use of restored capacity. These are selection-counter
deltas between samples, rather than exact whole-phase request shares.

A shorter decay constant explores recovered servers sooner but can send more
requests back to a persistently slow server. A longer constant remembers slow
behavior longer and can overreact to an isolated expensive request. The
configurable cold prior, error penalty, outstanding limits, and health thresholds
also affect results. No parameter sweep was used to tune Peak EWMA to these
particular traces.

The reference recurrence observes completed latency spikes. It cannot detect an
unfinished stall immediately; outstanding counts supply the earlier signal.
Health can also affect comparisons, although the initial matrix sampled no
health-state transitions. Health/failure/cancellation behavior has separate HTTP
correctness tests; these performance scenarios are not an availability study.

## Resources and limits

Resource counters cover each role separately. Docker CPU is cumulative through
the last successful sample; memory is sampled cgroup usage, not exact peak RSS.
Shutdown CPU and peaks between samples can be missed. All container exit codes
were successful and no container reported an OOM kill. The generated comparison
includes proxy resource medians; raw files include all three backends and the
generator. Sampling timestamps and failures are retained.

A separate single-caller, three-backend selection microbenchmark on darwin/arm64
measured approximately 131 ns for round-robin, 146 ns for least-outstanding, and
174 ns for Peak EWMA, including reservation and successful completion. All three
allocated one 80-byte lease per operation. This demonstrates extra estimator and
sampling work; it does not establish a significant end-to-end CPU penalty or
predict contention at a larger pool size.

Important limits of the experiment:

- Sleep-based backends isolate queueing; they do not reproduce CPU caches, GC
  pauses, storage latency, or network loss in a production service.
- Jitter and heavy-cost classification share an ID hash and seed. Heavy requests
  therefore have approximately 70-72 ms simulated service, rather than an
  independent uniform 70-90 ms distribution. All policies receive identical work.
- The proxy buffers complete bodies and measures full upstream completion.
  Streaming, large-body, and slow-reader performance require separate workloads.
- Three seeds and short intervals provide limited evidence. The p99 ranges are
  observed ranges, not statistical confidence intervals.
- The fixed small pool and global selector lock have not been shown to scale
  across large backend fleets or many proxy processes.
- A timeout after a slow upload can exhaust the shared request budget even when
  backend service is fast. The configured timeout penalty is deliberately
  conservative; GET-based benchmark requests avoid this confounder.

The implementation's value is the explicit lifecycle contract, bounded resources,
failure tests, and inspectable evidence. The policy choice depends on whether
latency history predicts future backend work in the workload being served.
