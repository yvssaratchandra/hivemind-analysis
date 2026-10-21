# Announcement-learning simulation results

This report includes only completed runs whose suite inventory and independent trace verification passed. It summarizes fixed-round simulation outcomes and makes no timing, deployment, or population-generalization claims. A negative paired difference means the arm's loss is below that seed's baseline loss. Null metrics are shown as nonfinite or unavailable; divergence remains visible even when some final metrics are finite.

The model is local to each participant; regrouping does not synchronize model parameters. Recovery measurements describe the scheduled honest rounds after the 100-round attack phase.

## Verified suite inputs

| Suite | Seeds | Runs | Verified divergences | Manifest SHA256 | Inventory SHA256 |
|---|---:|---:|---:|---|---|
| pilot | 201 | 18 | 0 | `0f3dc3c10186637965771da14c81be61e72dc6e472d746649c1ba9bf42ae1321` | `0cac63db2ed031eb54e28d7d5177a01a8acfcb6651986740e668bd74b1ac2a7c` |
| confirmation | 202, 203, 204, 205, 206 | 90 | 0 | `77eef53c04340bceaf8174178bd1e52dd5880028b55a34841919e6cf09922803` | `afddc0aa27418d0b4cb288a4f4f635cbad9e639324412eafd93ce87698c4814d` |

## Pilot

### iid

| Arm | Finite / runs | Final honest mean loss | Paired Δ vs baseline | Honest worst loss | Converged / censored | B / D / A / C final loss | Recovery Δ loss (100→200) |
|---|---:|---:|---:|---:|---:|---|---:|
| `baseline` | 1 / 1 (diverged 0) | 1.05586e-13 | 0 | 1.0591e-13 | 1 / 0 | B=1.05173e-13; D=1.05677e-13; A=1.0591e-13; C=1.05173e-13 | n/a |
| `random_omission_25` | 1 / 1 (diverged 0) | 1.25608e-05 | 1.25608e-05 | 3.28192e-05 | 1 / 0 | B=2.13084e-05; D=3.28192e-05; A=4.24324e-07; C=4.43895e-06 | n/a |
| `random_omission_100` | 1 / 1 (diverged 0) | 1.68239e-06 | 1.68239e-06 | 2.30857e-06 | 1 / 0 | B=1.45627e-05; D=2.30857e-06; A=1.43767e-06; C=1.30094e-06 | n/a |
| `membership_25` | 1 / 1 (diverged 0) | 5.50085e-07 | 5.50085e-07 | 1.54653e-06 | 1 / 0 | B=5.18654e-08; D=5.18654e-08; A=1.54653e-06; C=5.18656e-08 | n/a |
| `membership_100` | 1 / 1 (diverged 0) | 1.42975e-11 | 1.41919e-11 | 4.26897e-11 | 1 / 0 | B=1.01696e-13; D=1.01589e-13; A=4.26897e-11; C=1.01283e-13 | n/a |
| `ordering_25` | 1 / 1 (diverged 0) | 6.95075e-06 | 6.95075e-06 | 1.84413e-05 | 1 / 0 | B=1.20546e-06; D=1.20545e-06; A=1.84413e-05; C=1.20546e-06 | n/a |
| `ordering_100` | 1 / 1 (diverged 0) | 0.0951465 | 0.0951465 | 0.23669 | 0 / 1 | B=0.0243745; D=0.0243745; A=0.23669; C=0.0243745 | n/a |
| `membership_recovery` | 1 / 1 (diverged 0) | 3.38956e-07 | 3.38956e-07 | 9.57963e-07 | 1 / 0 | B=2.9452e-08; D=2.94519e-08; A=9.57963e-07; C=2.9452e-08 | 1.57989e-07 |
| `ordering_recovery` | 1 / 1 (diverged 0) | 0.0272535 | 0.0272535 | 0.0688777 | 1 / 0 | B=0.00644142; D=0.00644142; A=0.0688777; C=0.00644142 | 0.0678913 |

### distinctive

| Arm | Finite / runs | Final honest mean loss | Paired Δ vs baseline | Honest worst loss | Converged / censored | B / D / A / C final loss | Recovery Δ loss (100→200) |
|---|---:|---:|---:|---:|---:|---|---:|
| `baseline` | 1 / 1 (diverged 0) | 1.51566e-05 | 0 | 1.51567e-05 | 1 / 0 | B=1.51566e-05; D=1.51566e-05; A=1.51567e-05; C=1.51566e-05 | n/a |
| `random_omission_25` | 1 / 1 (diverged 0) | 0.00483759 | 0.00482243 | 0.00733144 | 1 / 0 | B=0.00187473; D=0.00733144; A=1.24903e-05; C=0.00716883 | n/a |
| `random_omission_100` | 1 / 1 (diverged 0) | 0.0627462 | 0.062731 | 0.100745 | 0 / 1 | B=0.0577905; D=0.100745; A=7.60696e-07; C=0.0874927 | n/a |
| `membership_25` | 1 / 1 (diverged 0) | 0.0270148 | 0.0269996 | 0.0405202 | 1 / 0 | B=0.0405202; D=0.0405202; A=3.89877e-06; C=0.0405202 | n/a |
| `membership_100` | 1 / 1 (diverged 0) | 0.0926582 | 0.0926431 | 0.138987 | 0 / 1 | B=0.138987; D=0.138987; A=4.26897e-11; C=0.138987 | n/a |
| `ordering_25` | 1 / 1 (diverged 0) | 5.764e-05 | 4.24834e-05 | 9.73609e-05 | 1 / 0 | B=3.77795e-05; D=3.77795e-05; A=9.73609e-05; C=3.77795e-05 | n/a |
| `ordering_100` | 1 / 1 (diverged 0) | 0.103133 | 0.103117 | 0.247244 | 0 / 1 | B=0.0310766; D=0.0310766; A=0.247244; C=0.0310766 | n/a |
| `membership_recovery` | 1 / 1 (diverged 0) | 0.0925858 | 0.0925706 | 0.138878 | 0 / 1 | B=0.138878; D=0.138878; A=7.87808e-07; C=0.138878 | 7.38418e-05 |
| `ordering_recovery` | 1 / 1 (diverged 0) | 0.0313223 | 0.0313071 | 0.0755412 | 1 / 0 | B=0.00921282; D=0.00921282; A=0.0755412; C=0.00921282 | 0.0738232 |


## Confirmation

### iid

| Arm | Finite / runs | Final honest mean loss | Paired Δ vs baseline | Honest worst loss | Converged / censored | B / D / A / C final loss | Recovery Δ loss (100→200) |
|---|---:|---:|---:|---:|---:|---|---:|
| `baseline` | 5 / 5 (diverged 0) | 1.02755e-13 | 0 | 1.02965e-13 | 5 / 0 | B=1.02841e-13; D=1.02736e-13; A=1.02875e-13; C=1.02655e-13 | n/a |
| `random_omission_25` | 5 / 5 (diverged 0) | 1.11196e-05 | 1.11196e-05 | 2.94778e-05 | 5 / 0 | B=2.42108e-05; D=2.36786e-05; A=3.97868e-06; C=5.70158e-06 | n/a |
| `random_omission_100` | 5 / 5 (diverged 0) | 1.56032e-06 | 1.56032e-06 | 2.24005e-06 | 5 / 0 | B=1.46209e-05; D=1.28387e-06; A=1.84122e-06; C=1.55589e-06 | n/a |
| `membership_25` | 5 / 5 (diverged 0) | 4.1894e-07 | 4.1894e-07 | 1.18573e-06 | 5 / 0 | B=3.5545e-08; D=3.55453e-08; A=1.18573e-06; C=3.55451e-08 | n/a |
| `membership_100` | 5 / 5 (diverged 0) | 9.85759e-12 | 9.75483e-12 | 2.9355e-11 | 5 / 0 | B=1.08854e-13; D=1.08792e-13; A=2.9355e-11; C=1.08939e-13 | n/a |
| `ordering_25` | 5 / 5 (diverged 0) | 5.34639e-06 | 5.34639e-06 | 1.48141e-05 | 5 / 0 | B=6.12544e-07; D=6.12543e-07; A=1.48141e-05; C=6.12543e-07 | n/a |
| `ordering_100` | 5 / 5 (diverged 0) | 0.140689 | 0.140689 | 0.337844 | 0 / 5 | B=0.042112; D=0.042112; A=0.337844; C=0.042112 | n/a |
| `membership_recovery` | 5 / 5 (diverged 0) | 2.50493e-07 | 2.50493e-07 | 7.04208e-07 | 5 / 0 | B=2.36358e-08; D=2.36358e-08; A=7.04208e-07; C=2.36359e-08 | 1.12079e-07 |
| `ordering_recovery` | 5 / 5 (diverged 0) | 0.0349627 | 0.0349627 | 0.086065 | 5 / 0 | B=0.00941152; D=0.00941152; A=0.086065; C=0.00941152 | 0.105725 |

### distinctive

| Arm | Finite / runs | Final honest mean loss | Paired Δ vs baseline | Honest worst loss | Converged / censored | B / D / A / C final loss | Recovery Δ loss (100→200) |
|---|---:|---:|---:|---:|---:|---|---:|
| `baseline` | 5 / 5 (diverged 0) | 2.02743e-05 | 0 | 2.02744e-05 | 5 / 0 | B=2.02744e-05; D=2.02744e-05; A=2.02743e-05; C=2.02744e-05 | n/a |
| `random_omission_25` | 5 / 5 (diverged 0) | 0.00825836 | 0.00823809 | 0.0139928 | 5 / 0 | B=0.00723865; D=0.0128239; A=3.17671e-05; C=0.0119194 | n/a |
| `random_omission_100` | 5 / 5 (diverged 0) | 0.0534319 | 0.0534116 | 0.0872818 | 3 / 2 | B=0.0439807; D=0.0730121; A=1.77458e-06; C=0.0872818 | n/a |
| `membership_25` | 5 / 5 (diverged 0) | 0.0229788 | 0.0229585 | 0.0344664 | 5 / 0 | B=0.0344664; D=0.0344664; A=3.66884e-06; C=0.0344664 | n/a |
| `membership_100` | 5 / 5 (diverged 0) | 0.0853963 | 0.085376 | 0.128094 | 0 / 5 | B=0.128094; D=0.128094; A=2.9355e-11; C=0.128094 | n/a |
| `ordering_25` | 5 / 5 (diverged 0) | 4.29208e-05 | 2.26464e-05 | 6.30319e-05 | 5 / 0 | B=3.28652e-05; D=3.28652e-05; A=6.30319e-05; C=3.28652e-05 | n/a |
| `ordering_100` | 5 / 5 (diverged 0) | 0.150354 | 0.150334 | 0.35191 | 0 / 5 | B=0.0495761; D=0.0495761; A=0.35191; C=0.0495761 | n/a |
| `membership_recovery` | 5 / 5 (diverged 0) | 0.0853114 | 0.0852911 | 0.127967 | 0 / 5 | B=0.127967; D=0.127967; A=5.472e-07; C=0.127967 | 8.53587e-05 |
| `ordering_recovery` | 5 / 5 (diverged 0) | 0.0389739 | 0.0389536 | 0.0930719 | 4 / 1 | B=0.0119249; D=0.0119249; A=0.0930719; C=0.0119249 | 0.110917 |

## Reading the metrics

The final honest mean is computed within each run across honest nodes A, D, and C; the table then reports the arithmetic mean of those run-level means across the listed seeds. Per-node final losses are separately averaged across seeds. Paired differences use the same seed and data distribution, with `arm − baseline` as the sign convention. Honest worst loss is the final maximum across A, D, and C. Convergence uses the runner's fixed threshold (10% of initial honest mean loss) and ten consecutive rounds; censored runs did not meet it within the recorded horizon or diverged first. Recovery loss change is measured from round 100 to round 200; a positive reduction means loss fell after the attack stopped. If a verified-divergence run did not reach either round, the recovery value is unavailable.

Per-node final and trajectory metrics, seed-paired rows, nonfinite markers, and recovery details are available in `report.json`, `case_metrics.csv`, and `paired_losses.csv`.
