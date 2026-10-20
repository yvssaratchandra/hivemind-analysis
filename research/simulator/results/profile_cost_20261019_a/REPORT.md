# Runtime profiling before baseline establishment

The 40-run sweep `controlled_baseline_20261019_a` and separate preflight were
interrupted following the request to profile before committing to the sweep.
Their partial evidence is retained; neither establishes a baseline.

## Probes

Same controlled configuration, seed 1, with shorter observation/horizon caps:

| Probe | Nodes | Simulated cap | Profiler total seconds | Status |
|---|---:|---:|---:|---|
| n100 | 100 | 35 s | 5.052 | Censored by short cap |
| n1000 | 1000 | 35 s | 52.045 | Censored by short cap |
| n100_later | 100 | 140 s | 56.142 | Interrupted by 45 s wall alarm; includes subsequent cleanup/replay |

These are instrumented single-seed diagnostics, not runtime benchmarks or
baseline outcome estimates. cProfile adds overhead; early probes do not capture
later behavior. Source snapshots, resolved configs, traces, replay, raw .prof
files and readable *_profile.txt files are retained.

For n1000, cumulative time in _reallocate was 21.250 s; Observer.emit 19.271 s;
_value conversion 13.595 s; _max_min_rates 9.104 s; post-run analyze 9.187 s.
These overlap and must not be added. zlib compression self time was 0.998 s.
Event handling includes accounting and serialization, not merely disk logging.

## Progress failure

The later 100-node probe stopped at simulated time 130.8432447158627 s.
Its last 10,000 raw events have exactly that timestamp: 9,999
network_reallocated events and the final interruption marker. In total it emitted
506,119 network_reallocated events but only 22,389 transfer_started events.
The interrupted production seed-1 trace also ended at this exact timestamp.
This demonstrates repeated event processing without simulated-time progress;
it is not useful communication simulation cost. The precise cause still needs
a regression test. A candidate is floating-point rounding of a positive residual
transfer duration to the current timestamp in Network._schedule_completion,
followed by rescheduling without clearing the remaining bytes.

## Decision

1. Diagnose and fix the no-progress loop first; add a focused regression test.
2. Reprofile representative runs through the full observation window, without
   concurrent workloads when measuring wall-clock speed. Do not extrapolate a
   two-hour sweep estimate from the stalled execution.
3. Prefer semantics-preserving event-conversion, allocation and logging changes
   before changing network abstraction. Preserve reproducible evidence and credit replay.
4. Consider whole-transfer or stage-level communication only after matched-seed
   comparisons of honest and delayed/withheld-update attacks. Preserve grouping,
   group concentration, timeout decisions, contention, work-credit eligibility,
   throughput and attack slowdown; inspect tails as well as averages.
5. Keep randomized initial readiness and multiple seeds. Establish the baseline
   only after the implementation and abstraction are settled.

No runtime model simplification was made by this profiling task.
