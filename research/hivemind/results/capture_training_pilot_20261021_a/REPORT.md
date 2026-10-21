# Native capture and exclusion training pilot

One seed; private loopback; 45 DHT nodes per condition; A has 70% of configured compute. State downloads remain native. Censored crossings are not assigned the horizon.

| Condition | Evidence valid | D/C crossing (s) | Confirmed (s) | D/C final worst loss |
|---|---|---:|---:|---:|
| baseline | True | 50.00 | 59.000035553006455 | 5.69348e-05 |
| capture_control | False | 140.00 | 149.00140524999006 | 0.0299274 |

| Condition | Channel | Native groups | X-led with A | D/C omission announcements | Completed omitted D/C outputs |
|---|---|---:|---:|---:|---:|
| baseline | gradient | 47 | 0 | 0 | 0 |
| baseline | state | 47 | 6 | 0 | 0 |
| capture_control | gradient | 20 | 20 | 0 | 0 |
| capture_control | state | 17 | 17 | 0 | 0 |

Actual Optimizer gradient/state prefixes are used. X must win native leadership; honest advertisements are not suppressed. Missing groups and escapes remain in the exposure denominator. Averages with incomplete streams retain native fallback. This pilot does not validate simulation timing or establish population success rates.
