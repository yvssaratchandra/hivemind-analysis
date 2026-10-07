# Exo research

Keep Exo notes, local experiments, and results in this directory. The upstream source belongs in a separate checkout at `/workspace/exo`, alongside `/workspace/hivemind`.

Upstream: [exo-explore/exo](https://github.com/exo-explore/exo).

The source is now available at [`/workspace/exo`](../../exo/). The initial research checkout is `21a54c5e` (verified 2026-10-06).

[Two local peers with Docker](two-peer/README.md) provides a model-free discovery and election setup. The shared image and honest-peer discovery/election were exercised in the three-peer audit.

[Initial three-peer audit](audit/README.md) adds two honest coordinators and one bounded publisher, with baseline, identity-claim, stale-session, and replay controls. The image built and all five arms completed; [measured results and raw evidence](results/exo-audit-20261006-005/REPORT.md) show acceptance of a claimed honest-peer identity, no observed session-mismatch marker, and one observed index for exact replay. Inference impact remains untested. All 37 helper tests pass.

[System requirements](SYSTEM_REQUIREMENTS.md) covers basic model-free tests, prerequisites, and initial RAM budgets for one or multiple peers; small-model inference is optional.

[Exo overview presentation](presentations/261006_exo_overview/README.md) explains the problem, architecture, control plane, and research questions, with speaker notes and an evidence appendix.

The parent repository ignores `/exo/` so its source and Git history remain independent. The existing [project survey](../hivemind/DHT_PROJECT_SURVEY.md) and [security shortlist](../hivemind/CONTROL_PLANE_TARGET_SURVEY.md) provide the starting context.
