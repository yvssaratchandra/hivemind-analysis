# Dominant-compute native results

Private native loopback; controlled gradient leader; state sharing enabled.

| Seed | Condition | D/C crossing (s) | Confirmed (s) | Final D/C worst loss | Outcome | Cleanup |
|---|---|---:|---:|---:|---|---|
| 301 | baseline | 40.00 | 49.00 | 1.37284e-08 | completed | joined |
| 301 | exclusion | 57.00 | 66.00 | 1.20595e-06 | completed | deadline_exceeded |
| 301 | absent | 121.00 | 130.00 | 0.00177747 | completed | joined |

Crossing requires D and C each below 10% of initial loss for ten consecutive one-second evaluations. Censored pairs are retained and excluded from arithmetic differences; they are not assigned a crossing time.

Per-node submitted-sample throughput divides logged batches by the measured horizon. Up to 32 computed boundary examples per node can be unlogged if the horizon arrives during pacing. Accepted-sample counts mean accepted into a local gradient accumulator; they do not by themselves establish incorporation into final parameters.

Cleanup deadline overruns are retained separately from training completion. The controller terminates and audits owned processes; a completed training run is not evidence of clean native shutdown.

A roster omission applies to that reducer view and does not imply that all of A’s work is lost. Native state averaging and recovery remain enabled. Numerical replay is supplementary; failures are included in the report without invalidating analyzer-validated native measurements.

These timings do not establish WAN performance, uncontrolled leader exposure, or a deployment success rate.
