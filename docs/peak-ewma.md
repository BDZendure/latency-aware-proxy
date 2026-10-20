# Peak EWMA reference and behavioral contract

This contract is written before implementation. The implementation language is Go.

## Pinned reference

Reference: Twitter Finagle `PeakEwma.scala`, commit
`9cc08d15216497bb03a1cafda96b7266cfbbcff1`.

- [Immutable source](https://github.com/twitter/finagle/blob/9cc08d15216497bb03a1cafda96b7266cfbbcff1/finagle-core/src/main/scala/com/twitter/finagle/loadbalancer/PeakEwma.scala)
- Algorithm: `Metric.observe`, `Metric.get`, `Metric.start`, and `Metric.end`.

We adopt this estimator, with the explicit HTTP adaptations below. This is an
implementation and evaluation of an established algorithm. ingress-nginx is not
the reference, and no equivalence to its EWMA implementation is claimed.

## Exact reference behavior

Let `E` be the estimate, `s` its last update timestamp, `t` the current monotonic
time, `r` an observation, `tau > 0` the decay constant, and `n` pending work.

```text
delta = max(t - s, 0)
w = exp(-delta / tau)
E' = r                         if r > E
     E*w + r*(1-w)             otherwise
s' = t
```

Every score read first observes `r = 0`. Thus idle decay is exponential toward
zero, and reads advance `s`. The half-life is `tau * ln(2)`.

The reference starts at `E = 0`, `n = 0`. After read-time decay:

```text
score = (Long.MaxValue >> 16) + n     if E == 0 and n != 0
        E * (n + 1)                  otherwise
```

`start` increments `n` and returns a timestamp. `end` decrements `n` and observes
elapsed time. The enclosing code ends on service closure or acquisition failure;
it does not define our HTTP body, status, timeout, or cancellation semantics.

## Project estimator and selection

Use the exact observation recurrence above and the exact read-time zero
observation. Do not independently decay again before applying a nonzero
completion observation. All estimator timestamps use Go monotonic durations.

The deliberate cold-start adaptation is a positive, configurable initial estimate
of **50 ms**, rather than Finagle's zero estimate and special pending penalty.
This permits the required score formula for every eligible backend:

```text
score = estimated_latency * (outstanding_requests + 1)
```

The default `tau` is **10 seconds**, a project configuration choice. Its half-life
is approximately 6.93 seconds. Neither value is a claim about the pinned
reference's configured defaults. No positive floor is applied after initialization;
underflow to zero is allowed. A recovered backend retains its estimate and is not
reinitialized. New processes start with the same prior for every backend.

Eligible means healthy and below the per-backend outstanding limit. Uniformly
sample two distinct eligible backends without replacement and choose the lower
score. With one eligible backend, choose it. Break exact ties randomly. With no
eligible backend, reject immediately with HTTP 503. Selection and reservation
must be one synchronized operation, so concurrent selectors cannot overbook.
The initial implementation may scan the fixed pool for eligibility; it will not
claim end-to-end O(1) selection.

Round-robin rotates through eligible backends. Least outstanding scans all eligible
backends for minimum outstanding, breaking ties randomly. It is an exact minimum
baseline, not a two-choice approximation. All policies use identical eligibility,
admission limits, transports, deadlines, and health rules.

## Request accounting and observation boundaries

1. Admit a request into a bounded global in-flight set without waiting. Saturation
   returns HTTP 503. Bound accepted TCP connections separately, including idle
   connections. There is no unbounded request queue.
2. Apply a total request deadline at handler entry, including request-body input.
   Buffer the request body within its size limit before backend selection. A
   rejected or canceled request that has not reserved a backend never changes a
   backend's outstanding count.
3. Under the selector lock, choose a backend, increment outstanding exactly once,
   and record dispatch time. This covers dialing, transport connection acquisition,
   request transmission, response headers, and reading the full upstream body.
4. Finish the reservation exactly once after the upstream operation has ended and
   the response body is closed, including every error and cancellation path. A
   deferred, idempotent completion function prevents double decrements.
5. Successful upstream completion observes dispatch-to-full-body-read elapsed
   time. Do not release at response headers. Downstream response delivery retains
   the global admission slot through explicit payload flushing but does not
   retain a backend reservation. Go's final chunk/trailer framing occurs just
   after handler return; it remains bounded by the connection limit and write
   deadline. See the architecture document for this lifecycle boundary.

Responses are buffered within a strict maximum before downstream headers are
committed, so upstream failures can return a defined HTTP error. HTTP/1.1 pooling
is preserved by fully consuming and closing valid upstream bodies. Oversized or
failed bodies are closed without unbounded draining; those connections may be
discarded. Latency includes network and upstream body transfer, not just backend
service time. There are no application-level retries. Any transparent retry that
the Go transport performs must be documented and stays within one reservation
and the original deadline.

## Terminal outcomes

| Outcome | Estimate observation | Passive health evidence | Reservation |
| --- | --- | --- | --- |
| Valid upstream status below 500 and complete body | Measured elapsed | Success | Release once |
| Upstream 5xx, dial/read failure, or oversized response | `max(elapsed, 1 second)` by default | Failure | Release once |
| Proxy request deadline expires during upstream work | `max(elapsed, configured request deadline)` | Failure | Cancel transport, close body, release once |
| Client cancels, downstream write fails, or shutdown force-cancels | No observation | No evidence | Cancel upstream if active, close body, release once |
| Admission failure, invalid request, or oversized request body | No observation | No evidence | No reservation |

The configurable one-second error penalty is a project failure-handling rule, not
part of the reference recurrence. The deadline and penalty defaults are equal.
An already-observed upstream result is not erased by a later downstream failure.
Client cancellation must not be misclassified as a backend timeout because both
contexts become done. Preserve the cancellation cause and outcome precedence.

Proxy deadlines have an additional bounded 100 ms window for delivering a 504.
The one-second default is the operation budget, rather than a promise that a
timeout response is completely delivered within exactly one second.

A spike changes the estimate immediately **when observed**, not when an
unfinished request starts stalling. Outstanding counts provide the signal while
requests remain unfinished. Health exclusion prevents fast failures from becoming
attractive. These distinctions are essential to interpreting benchmark results.

## Health and recovery

Use the same configurable passive and active health rules for all policies.
Initially mark backends healthy. Three consecutive passive backend failures
exclude a backend; a successful upstream request resets that passive streak.
Run a bounded HTTP health probe once per second per backend with a 200 ms
deadline. Independently, three consecutive failed probes exclude a backend;
a successful probe resets the probe failure streak. A failed probe resets the
probe success streak. Two consecutive successful probes restore an excluded
backend and clear all streaks. Successful user requests never restore an excluded
backend. Results are ordered by completion under the selector lock; health
transitions never cancel existing reservations.
Probes do not reserve user-request slots or update latency estimates. Their
connections, response bodies, and concurrency are separately bounded. Readiness
requires at least one healthy backend; liveness reports process health.

## Required correctness evidence

- Fake-clock tests reproduce the recurrence, equality boundary, immediate peaks,
  lower observations, zero elapsed time, repeated reads, cold prior, idle decay,
  and score multiplier.
- Seeded selection tests cover distinct sampling, ties, one/no eligible backend,
  exact least-outstanding behavior, and round-robin rotation.
- Concurrent tests prove backend and global limits are never exceeded and every
  reservation returns to zero after success, failure, timeout, client cancellation,
  partial upstream body, oversized body, and forced shutdown.
- HTTP integration tests cover hop-by-hop header handling, request forwarding,
  pooling, chunked bodies, malformed/truncated responses, slow clients, health
  exclusion/recovery, and graceful drain.
- Run the Go race detector. Do not infer a latency improvement from correctness
  tests; benchmark it with complete offered-load accounting.
