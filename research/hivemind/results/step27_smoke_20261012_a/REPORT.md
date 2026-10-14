# Step 27 control-plane screen

Phase: **smoke**. Trials: **2/2**. Integrity: **True**.

This is a bounded local screening experiment with 0.15-second one-way control RPC delay. The capped latency uses native `step_start` through first relevant native assembly; missing assembly is assigned 12 seconds. Honest group memberships count deduplicated native groups assembled during each submission window.

Per-seed flags are preliminary descriptive checks. They are not significance tests, deployment success rates, or deployment claims. The report separates disband emission, the server-side response event after its delay, and the native client's reply record; server-side response evidence alone does not establish client receipt.

## Trial metrics

| Case | Seed | X led / submitted | X led by 2s | Honest capped latency | Honest memberships/minute | Groups | Retries | Holds | Integrity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| unreliable_renewed | 2700 | 0/1 | 0/1 | 4.418942799468156 | 8.571428571428573 | 4 | 17 | 17 | True |
| reliable_renewed | 2700 | 2/2 | 2/2 | 2.0657873722493605 | 12.000000000000002 | 5 | 0 | 0 | True |

## Paired-seed screening flags

Suite flags: `null`. Status: `null`.

| Seed | Δ X-led-by-2s rate, renewed vs once | Δ X-led-by-2s rate, renewed vs native | Renewal-alone Δ honest capped latency | Harm difference-in-differences | Flags |
|---:|---:|---:|---:|---:|---|

## Integrity and coverage

Smoke gate: **True**.

Delayed correlated find/store/join RPC counts: `{"find": 1107, "join": 80, "store": 277}`.

All detailed rows are event-referenced in the CSV exports. `selection_observed_x_expiration` comes from the selection queue snapshot; `latest_published_x_expiration` comes from X's latest active publication event. Queue alternatives are observed in the local native queue and do not establish global DHT availability.
