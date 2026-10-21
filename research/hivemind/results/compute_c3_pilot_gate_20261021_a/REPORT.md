# Pilot numerical simulation gate

**Gate failed; no compute-share sweep.** All three native pilot cases passed their raw evidence checks. Numerical replay passes exclusion and absent, but fails one baseline group.

B registered only two of three C-reducer response parts and preserved its own input at coordinates `[32:36]`, with four of 48 elements marked failed. The reliable-delivery numerical model instead predicts the full weighted average. The raw fallback occurred about 60 ms after B entered aggregation; the cause of early stream termination is unknown and it is not the attack-induced two-second wait.

The native convergence measurements remain valid. The predeclared simulation gate requires matching numerical behavior, so timing validation and the rate-share sweep are not enabled. See `report.json` for exact values, group ID, source inventory identity, and replay failures.
