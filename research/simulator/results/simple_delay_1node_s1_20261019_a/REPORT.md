# One-node delay comparison

Seed 1; 100 nodes; N0000 delays every aggregation input and result message by 1 s, including local reducer messages. Chunk payloads and group-selection policy are unchanged. This is a per-message send delay, not a single 1 s pause per round. Both runs use identical initial offsets and resources. No numerical gradients are computed.

| Metric | Honest seed 1 | Delay seed 1 | Difference |
|---|---:|---:|---:|
| Time to 500 credits (simulated s) | 120.84 | 120.94 | 0.10 |
| All credits by 180 s | 793.00 | 771.00 | -22.00 |
| Honest-node credits by 180 s (same 99 nodes) | 785.00 | 764.00 | -21.00 |
| N0000 credits by 180 s | 8.00 | 7.00 | -1.00 |
| Groups formed by 180 s | 118.00 | 112.00 | -6.00 |
| Nodes without credit by 180 s | 0.00 | 0.00 | 0.00 |
| Failed work attempts by 180 s | 0.00 | 0.00 | 0.00 |
| Median ready-to-credit wait (s) | 9.85 | 9.85 | -0.00 |

Logged policy actions before 180 s: 261. Distinct honest peers sharing a roster with N0000: 21. Roster exposure is not a causal estimate for each peer. One seed demonstrates a scenario; it does not establish a general attack effect. See comparison.json and policy_actions.json.

Mechanism check: the first remote policy action at 19.085461635 s was followed
by the first matching transfer at 20.085461635 s (1.0 s later). There were
128 input-transfer and 133 result-transfer actions before 180 s. Twenty-one
honest nodes each received one fewer credit than in the control; none gained
credit. No partial aggregation occurred. See mechanism_check.json.

[Open the paired dashboard](COMPARISON_DASHBOARD.html).
