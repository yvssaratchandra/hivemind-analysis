# Working agreement for this Hivemind research fork

## Research context

- Keep research changes under `research/<project>/`: Hivemind work belongs in `research/hivemind/` and Exo work in `research/exo/`. Do not modify `hivemind/` for an experiment unless the user explicitly changes that constraint.
- Use `research/hivemind/HANDOFF.md` for Hivemind status and `research/hivemind/CONTROL_DATA_AUDIT.md` for its experiment queue and relevant worksheet. Read only the sections needed for the task.
- Check the script and raw result files before making an empirical claim. Distinguish local measurements, WAN-delay emulation, code inference, and untested hypotheses. Do not generalize a small observed count into a deployment success rate.
- Verify the user's technical claims against source code, documentation, and relevant experiments before relying on them. Correct misunderstandings explicitly and distinguish verified behavior from assumptions.
- Give each new experiment a distinct output label. Preserve controls, seeds, configuration, and raw traces so a result can be checked and repeated.
- Before interacting with a public swarm, establish the operator, exact protocol path and key, and agreed scope. Local experiments need no such step.

## Implementation work

- Use Plan mode with Sol High for substantial planning; the project defaults to Sol Medium for execution and Luna Max for subagents, with Fast mode preferred throughout. Choose another available model when the task or client requires it.
- For a substantial, multi-step change, make a concise task-specific plan with a small `Implementation status` table. Do not create phase files or a status table for a simple edit or a discussion.
- Every implementation plan must include the general approach, architecture and data flow, and a concrete coding plan. Specify component responsibilities, interfaces, implementation order, tests, acceptance criteria, and review gates clearly enough that a less capable coding agent can implement it without making research or architecture decisions. Scale the detail to the task.
- Delegate at most three independent, bounded tasks when parallel work should save time. Assign exclusive file ownership; keep dependent edits and integration with the main agent.
- A worker reviews its diff, runs focused checks, and reports changed files, results, and remaining risks. The main agent inspects the diff and integration before accepting the work or marking a task complete.
- Run focused checks appropriate to the change. Diagnose failures before retrying, and rerun previously passing checks when a later change could affect them. Use a broader suite when there is a concrete regression risk.
- Avoid speculative optimization and process documents that do not help complete the current task. Measure performance claims when practical.
- After each meaningful task, review the changes, run appropriate checks, split the work into logical commits, and push to the configured remote unless the user says otherwise. Preserve unrelated work and keep bulky local artifacts out of Git with an inspectable inventory. Report commit IDs, checks, and any push failure.

## Presentations

- Keep slides concise as cues for a live explanation; put fuller explanations in speaker notes. Prefer clear diagrams when they clarify a mechanism, and preserve layouts the user has approved.
- Separate measured results from theoretical conditions and untested combinations. Keep exact experiment tables inspectable in an appendix when making a presentation.
