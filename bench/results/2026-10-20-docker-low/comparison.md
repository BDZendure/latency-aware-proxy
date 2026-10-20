# Measured policy comparison

Latency, throughput, memory, and CPU report medians of per-run statistics.
Offered and outcome counters are totals across all repetitions; no impaired run is omitted.
The p99 range is the minimum and maximum per-run p99, not a confidence interval.
Percentiles describe successful requests; all offered outcomes are shown alongside them.

Docker memory is sampled peak cgroup usage; CPU is cumulative through the last sample.
Local memory is kernel lifetime peak RSS; CPU is kernel lifetime CPU time.

| Scenario | Policy | Runs | Offered | Success RPS | p50 ms | p95 ms | p99 ms (range) | Errors/timeouts | Rejected | Gen missed | Proxy mem MiB | Proxy CPU s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| heterogeneous | least-outstanding | 2 | 5600 | 350.0 | 6.69 | 26.29 | 27.36 (27.36-27.36) | 0 | 0 | 0 | 13.7 | 1.46 |
| heterogeneous | peak-ewma | 2 | 5600 | 350.0 | 6.63 | 26.03 | 27.36 (27.36-27.36) | 0 | 0 | 0 | 13.7 | 1.47 |
| heterogeneous | round-robin | 2 | 5600 | 350.0 | 13.50 | 26.55 | 27.22 (27.09-27.36) | 0 | 0 | 0 | 12.6 | 1.45 |
| mixed | least-outstanding | 2 | 3200 | 200.0 | 10.01 | 73.27 | 74.74 (74.74-74.74) | 0 | 0 | 0 | 13.1 | 1.04 |
| mixed | peak-ewma | 2 | 3200 | 200.0 | 10.06 | 73.27 | 74.74 (74.74-74.74) | 0 | 0 | 0 | 12.9 | 1.06 |
| mixed | round-robin | 2 | 3200 | 200.0 | 10.06 | 73.27 | 74.74 (74.74-74.74) | 0 | 0 | 0 | 12.5 | 1.06 |
| slowdown | least-outstanding | 2 | 5600 | 350.0 | 9.48 | 11.40 | 117.54 (116.95-118.12) | 0 | 0 | 0 | 12.6 | 1.46 |
| slowdown | peak-ewma | 2 | 5600 | 350.0 | 9.43 | 11.28 | 11.86 (11.74-11.98) | 0 | 0 | 0 | 13.0 | 1.46 |
| slowdown | round-robin | 2 | 5600 | 350.0 | 9.53 | 210.37 | 968.95 (964.13-973.77) | 0 | 0 | 0 | 20.2 | 1.49 |
| spike | least-outstanding | 2 | 8800 | 550.0 | 9.43 | 11.28 | 12.59 (12.59-12.59) | 0 | 0 | 0 | 16.8 | 1.56 |
| spike | peak-ewma | 2 | 8800 | 550.0 | 9.62 | 11.98 | 13.64 (13.36-13.91) | 0 | 0 | 0 | 17.3 | 1.57 |
| spike | round-robin | 2 | 8800 | 550.0 | 9.43 | 11.12 | 11.80 (11.74-11.86) | 0 | 0 | 0 | 17.2 | 1.54 |
| steady | least-outstanding | 2 | 5600 | 350.0 | 9.43 | 11.34 | 11.80 (11.74-11.86) | 0 | 0 | 0 | 12.7 | 1.47 |
| steady | peak-ewma | 2 | 5600 | 350.0 | 9.43 | 11.28 | 11.68 (11.63-11.74) | 0 | 0 | 0 | 12.7 | 1.46 |
| steady | round-robin | 2 | 5600 | 350.0 | 9.39 | 11.06 | 11.80 (11.74-11.86) | 0 | 0 | 0 | 13.6 | 1.46 |
