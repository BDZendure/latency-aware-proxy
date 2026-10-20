# Measured policy comparison

Latency, throughput, memory, and CPU report medians of per-run statistics.
Offered and outcome counters are totals across all repetitions; no impaired run is omitted.
The p99 range is the minimum and maximum per-run p99, not a confidence interval.
Percentiles describe successful requests; all offered outcomes are shown alongside them.

Docker memory is sampled peak cgroup usage; CPU is cumulative through the last sample.
Local memory is kernel lifetime peak RSS; CPU is kernel lifetime CPU time.

| Scenario | Policy | Runs | Offered | Success RPS | p50 ms | p95 ms | p99 ms (range) | Errors/timeouts | Rejected | Gen missed | Proxy mem MiB | Proxy CPU s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| mixed | least-outstanding | 3 | 25200 | 700.0 | 10.53 | 73.27 | 85.06 (85.06-86.77) | 0 | 0 | 0 | 21.2 | 2.98 |
| mixed | peak-ewma | 3 | 25200 | 700.0 | 11.17 | 74.00 | 87.64 (86.77-87.64) | 0 | 0 | 0 | 21.1 | 3.06 |
| mixed | round-robin | 3 | 25200 | 700.0 | 18.19 | 92.11 | 137.13 (119.30-147.03) | 0 | 0 | 0 | 28.0 | 3.06 |
