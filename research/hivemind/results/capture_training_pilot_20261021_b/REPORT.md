# Native capture and exclusion training pilot

One seed; private loopback; 45 DHT nodes per condition; A has 70% of configured compute. State downloads remain native. Censored crossings are not assigned the horizon.

| Condition | Evidence valid | D/C crossing (s) | Confirmed (s) | D/C final worst loss |
|---|---|---:|---:|---:|
| baseline | True | 62.00 | 71.00004205499135 | 0.000508365 |
| capture_control | True | 153.00 | 162.00008327700198 | 0.0466232 |
| gradient_exclusion | True | censored | None | 0.17096 |
| dual_exclusion | True | censored | None | 0.247617 |

| Condition | Channel | Native groups | X-led with A | D/C omission announcements | Completed omitted D/C outputs |
|---|---|---:|---:|---:|---:|
| baseline | gradient | 40 | 0 | 0 | 0 |
| baseline | state | 39 | 1 | 0 | 0 |
| capture_control | gradient | 18 | 18 | 0 | 0 |
| capture_control | state | 14 | 14 | 0 | 0 |
| gradient_exclusion | gradient | 10 | 10 | 12 | 10 |
| gradient_exclusion | state | 6 | 6 | 0 | 0 |
| dual_exclusion | gradient | 6 | 6 | 9 | 9 |
| dual_exclusion | state | 3 | 3 | 4 | 4 |

Actual Optimizer gradient/state prefixes are used. X must win native leadership; honest advertisements are not suppressed. Missing groups and escapes remain in the exposure denominator. Averages with incomplete streams retain native fallback. This pilot does not validate simulation timing or establish population success rates.
