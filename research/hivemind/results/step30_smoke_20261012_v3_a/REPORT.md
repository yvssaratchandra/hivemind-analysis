# Step 30: stock 2s versus `(1,2]` renewal

Phase: `smoke`; suite complete: `True`; integrity pass: `True`.

Eight peers ran on private loopback. X recruited followers, accepted them, held for 0.5s, and disbanded. Each remote find, store, join request, and response leg received an independently seeded uniform 100–200ms delay. The experiment records grouping and control-plane harm; metadata-only completion does not test tensor averaging or training quality.

Every raw `step_start` is in the attempt denominator. A missing honest group receives a 12s capped score; successful waits are also shown separately. Submission-window groups are counted by assembly time; drain assemblies are listed separately. Group rate is paired with honest-led mean roster size and actual honest local assemblies per minute, since the number of groups can change when roster sizes change.

| Case | Honest attempts | Groups | Groups/min | Honest-led mean roster | Honest local assemblies/min | Honest failure | Mean successful wait (s) | Mean capped wait (s) | X ACCEPTED joins | X DISBANDED joins | X multi-publication searches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| stock_2s / 3000 | 25 | 7 | 21.000 | 3.143 | 66.000 | 0.040 | 3.879 | 4.203 | 31 | 31 | 2 |
| renew_1_2 / 3000 | 24 | 7 | 21.000 | 3.000 | 63.000 | 0.042 | 3.875 | 4.214 | 35 | 35 | 2 |

Descriptive local eight-peer loopback result with independently seeded 100–200ms control-leg jitter. No tensor averaging, training quality, WAN, or public-swarm impact claim is made.
