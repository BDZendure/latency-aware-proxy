# Measured policy comparison

Latency, throughput, memory, and CPU report medians of per-run statistics.
Offered and outcome counters are totals across all repetitions; no impaired run is omitted.
The p99 range is the minimum and maximum per-run p99, not a confidence interval.
Percentiles describe successful requests; all offered outcomes are shown alongside them.

Docker memory is sampled peak cgroup usage; CPU is cumulative through the last sample.
Local memory is kernel lifetime peak RSS; CPU is kernel lifetime CPU time.

| Scenario | Policy | Runs | Offered | Success RPS | p50 ms | p95 ms | p99 ms (range) | Errors/timeouts | Rejected | Gen missed | Proxy mem MiB | Proxy CPU s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| heterogeneous | least-outstanding | 3 | 25200 | 700.0 | 6.34 | 26.03 | 27.36 (27.36-27.36) | 0 | 0 | 0 | 16.7 | 2.90 |
| heterogeneous | peak-ewma | 3 | 25200 | 700.0 | 6.34 | 25.77 | 27.36 (27.09-27.36) | 0 | 0 | 0 | 14.9 | 2.90 |
| heterogeneous | round-robin | 3 | 25200 | 700.0 | 13.10 | 200.15 | 202.15 (202.15-202.15) | 0 | 0 | 0 | 23.3 | 3.05 |
| mixed | least-outstanding | 3 | 14400 | 400.0 | 9.82 | 73.27 | 74.00 (74.00-74.00) | 0 | 0 | 0 | 15.3 | 2.25 |
| mixed | peak-ewma | 3 | 14400 | 400.0 | 9.82 | 72.54 | 74.00 (74.00-74.00) | 0 | 0 | 0 | 15.0 | 2.13 |
| mixed | round-robin | 3 | 14400 | 400.0 | 9.92 | 72.54 | 74.74 (74.74-74.74) | 0 | 0 | 0 | 15.0 | 2.18 |
| slowdown | least-outstanding | 3 | 25200 | 700.0 | 9.34 | 11.17 | 116.95 (115.79-118.12) | 0 | 0 | 334 | 17.0 | 2.96 |
| slowdown | peak-ewma | 3 | 25200 | 700.0 | 9.43 | 11.51 | 13.36 (13.36-461.69) | 1 | 161 | 233 | 17.0 | 2.97 |
| slowdown | round-robin | 3 | 25200 | 699.6 | 9.34 | 11.17 | 964.13 (954.59-964.13) | 14 | 0 | 0 | 23.7 | 2.98 |
| spike | least-outstanding | 3 | 39600 | 742.1 | 69.02 | 73.27 | 75.49 (75.49-77.00) | 0 | 12885 | 0 | 40.7 | 2.94 |
| spike | peak-ewma | 3 | 39600 | 742.4 | 69.02 | 73.27 | 75.49 (74.74-76.24) | 0 | 12862 | 0 | 38.6 | 2.98 |
| spike | round-robin | 3 | 39600 | 742.8 | 69.02 | 73.27 | 75.49 (75.49-76.24) | 0 | 12867 | 0 | 38.9 | 2.94 |
| steady | least-outstanding | 3 | 25200 | 700.0 | 9.34 | 11.17 | 11.51 (11.51-11.74) | 0 | 0 | 0 | 14.7 | 2.98 |
| steady | peak-ewma | 3 | 25200 | 700.0 | 9.34 | 11.17 | 11.51 (11.51-12.47) | 0 | 0 | 0 | 16.7 | 2.95 |
| steady | round-robin | 3 | 25200 | 700.0 | 9.34 | 10.84 | 11.40 (11.40-11.63) | 0 | 0 | 0 | 16.7 | 2.98 |
