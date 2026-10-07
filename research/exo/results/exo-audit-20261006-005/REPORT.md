# Initial local Exo audit — 2026-10-06

**Completed:** [summary](summary.json). Two honest Exo coordinators and one controlled Router publisher ran on a private IPv6 Docker bridge, with no published ports, models, downloads, or inference workers. Each arm used fresh state and a separate namespace. Source: `21a54c5ea0230a3bec1e1a786d200126c7e34ec6`; seed: `1901`. This is one local trial per arm.

| Arm | Sends | Observed global indices | Result |
| --- | ---: | --- | --- |
| [Baseline](baseline/result.json) | 0 | None | No-send control; publisher resource entry stayed absent |
| [Self](self/result.json) | 1 | `0` | Publisher's synthetic RAM entry appeared at both honest peers |
| [Forged identity](forged/result.json) | 1 | `0` | Publisher claimed honest-b's ID; that entry appeared at both peers |
| [Stale session](stale-session/result.json) | 1 | None | Clock `−1` instead of current `0`; marker absent at both peers |
| [Exact replay](replay/result.json) | 2 | `0` | One distinct index observed after two identical sends |

The accepted entries contained exactly **136,164,352 available bytes** and were absent before publication. In the forged arm, honest-b was also the elected master; the publisher had a distinct transport identity. Its matching global event had the master's origin and session, and both honest API snapshots corroborated the state change. Peer identities and committed sessions stayed unchanged across every arm.

This establishes local acceptance of a resource advertisement claiming another peer's identity. **It does not validate a security vulnerability or inference impact.** Workers were disabled, so there was no existing worker advertisement to overwrite. The session control used a constructed clock mismatch, rather than a message captured from a previously active session; no acknowledgement during the bounded window is an absence observation. Replay preserved the local origin, index, event ID, timestamp, and serialized envelope; replay using a fresh local index remains untested.

Source inspection provides separate explanations: the [master](../../../../exo/src/exo/master/main.py) drops mismatched sessions and assigns global indices; the [ordered buffer](../../../../exo/src/exo/utils/event_buffer.py) drops already-consumed local indices. These are code inferences alongside the measured observations.

| Container role | Largest captured cgroup peak across five arms |
| --- | ---: |
| Honest-a | 142.6 MiB |
| Honest-b | 143.8 MiB |
| Publisher | 96.4 MiB |

[Exact resource figures](resource-summary.json) cover each container's lifetime through capture, including startup and observer processes. All 15 captures recorded zero OOM events. These figures are not minimum requirements, build peaks, host peaks, or inference budgets. The chosen cap stayed 2 GiB per container. Preflight reported 18.8 GiB available host RAM; clearing RAM was unnecessary. The [image](image-size.json) reports 4.59 GiB logical size; build RAM peak was not measured.

[Cleanup verification](cleanup-verification.json) found no containers or networks remaining from attempts `004` and `005`; state volumes and evidence were retained. Attempt [004](../exo-audit-20261006-004/summary.json) stopped before publication because ANSI-colored startup IDs broke our observer; parsing was corrected and checked against its saved logs. The [image build](../exo-audit-build-20261006-004/build-result.json) succeeded, and all 37 audit helper tests passed.

Raw evidence in each arm: `probe.jsonl`, `control.json`, `before.json`, `after.json`, consensus records, peer logs, `commands.jsonl`, and cgroup files. [Offline verification](evidence-validation.json) reproduced all five outcomes from the saved records, snapshots, and logs, and checked lab hashes against the retained copies. [Manifest](manifest.json) retains settings, source hashes, and [lab source copies](lab-source/research/exo/audit/run.py). See the [audit instructions](../../audit/README.md); use a fresh label for any rerun.
