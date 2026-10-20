# Measured policy comparison

Latency, throughput, memory, and CPU report medians of per-run statistics.
Offered and outcome counters are totals across all repetitions; no impaired run is omitted.
The p99 range is the minimum and maximum per-run p99, not a confidence interval.
Percentiles describe successful requests; all offered outcomes are shown alongside them.

Docker memory is sampled peak cgroup usage; CPU is cumulative through the last sample.
Local memory is kernel lifetime peak RSS; CPU is kernel lifetime CPU time.

| Scenario | Policy | Runs | Offered | Success RPS | p50 ms | p95 ms | p99 ms (range) | Errors/timeouts | Rejected | Gen missed | Proxy mem MiB | Proxy CPU s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| slowdown | least-outstanding | 3 | 25200 | 700.0 | 9.34 | 11.17 | 118.12 (118.12-120.49) | 0 | 0 | 0 | 17.3 | 3.02 |
| slowdown | peak-ewma | 3 | 25200 | 700.0 | 9.43 | 11.63 | 13.63 (13.23-15.67) | 0 | 0 | 0 | 17.3 | 2.97 |
| slowdown | round-robin | 3 | 25200 | 699.8 | 9.34 | 11.17 | 964.13 (964.13-964.13) | 8 | 0 | 0 | 23.1 | 3.02 |
