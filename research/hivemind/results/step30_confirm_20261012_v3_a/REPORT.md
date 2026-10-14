# Step 30: stock 2s versus `(1,2]` renewal

Phase: `confirm`; suite complete: `True`; integrity pass: `True`.

Eight peers ran on private loopback. X recruited followers, accepted them, held for 0.5s, and disbanded. Each remote find, store, join request, and response leg received an independently seeded uniform 100–200ms delay. The experiment records grouping and control-plane harm; metadata-only completion does not test tensor averaging or training quality.

Every raw `step_start` is in the attempt denominator. A missing honest group receives a 12s capped score; successful waits are also shown separately. Submission-window groups are counted by assembly time; drain assemblies are listed separately. Group rate is paired with honest-led mean roster size and actual honest local assemblies per minute, since the number of groups can change when roster sizes change.

| Case | Honest attempts | Groups | Groups/min | Honest-led mean roster | Honest local assemblies/min | Honest failure | Mean successful wait (s) | Mean capped wait (s) | X ACCEPTED joins | X DISBANDED joins | X multi-publication searches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| stock_2s / 3001 | 362 | 111 | 22.200 | 3.234 | 71.800 | 0.003 | 3.301 | 3.325 | 299 | 298 | 21 |
| renew_1_2 / 3001 | 343 | 104 | 20.800 | 3.288 | 68.400 | 0.003 | 3.574 | 3.599 | 385 | 384 | 21 |
| renew_1_2 / 3002 | 338 | 104 | 20.800 | 3.240 | 67.400 | 0.003 | 3.808 | 3.833 | 387 | 383 | 21 |
| stock_2s / 3002 | 354 | 105 | 21.000 | 3.324 | 69.600 | 0.006 | 3.495 | 3.543 | 296 | 295 | 21 |
| stock_2s / 3003 | 360 | 100 | 20.000 | 3.520 | 70.400 | 0.006 | 3.338 | 3.386 | 290 | 290 | 21 |
| renew_1_2 / 3003 | 341 | 105 | 21.000 | 3.238 | 67.600 | 0.000 | 3.700 | 3.700 | 368 | 367 | 21 |

## Paired effects

Positive wait/failure differences and a negative group-rate difference are harmful to honest peers. A material signal requires the predeclared threshold in at least two paired seeds, the third seed in the same direction, and the equally weighted mean effect to meet its threshold.

- `honest_mean_capped_wait_seconds`: seed effects [0.2740968517388813, 0.2899432464772502, 0.3142143745760677]; threshold 0.5; material signal `False`.
- `honest_group_failure_fraction`: seed effects [0.0001530209558172125, -0.002691137632467489, -0.005555555555555556]; threshold 0.05; material signal `False`.
- `honest_group_rate_reduction_fraction`: seed effects [0.06306306306306297, 0.00952380952380949, -0.050000000000000044]; threshold 0.15; material signal `False`.
- Coverage: `True`; conclusion: no material renewal advantage observed under the tested configuration.
- Persistence: `{"honest_group_failure_fraction": {"persistent": false, "seed_later_window_same_direction_counts": {"3001": 1, "3002": 1, "3003": 0}, "seeds_persistent": 0}, "honest_group_rate_per_minute": {"persistent": false, "seed_later_window_same_direction_counts": {"3001": 3, "3002": 2, "3003": 1}, "seeds_persistent": 1}, "honest_mean_capped_wait_seconds": {"persistent": true, "seed_later_window_same_direction_counts": {"3001": 4, "3002": 3, "3003": 4}, "seeds_persistent": 3}}`.

Descriptive local eight-peer loopback result with independently seeded 100–200ms control-leg jitter. No tensor averaging, training quality, WAN, or public-swarm impact claim is made.
