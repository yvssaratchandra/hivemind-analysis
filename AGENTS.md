# Working agreement for this Hivemind research fork

## Research context

- Keep research changes under `research/`. Do not modify `hivemind/` for an experiment unless the user explicitly changes that constraint.
- Use `research/HANDOFF.md` for current status and `research/CONTROL_DATA_AUDIT.md` for the experiment queue and relevant worksheet. Read only the sections needed for the task.
- Check the script and raw result files before making an empirical claim. Distinguish local measurements, WAN-delay emulation, code inference, and untested hypotheses. Do not generalize a small observed count into a deployment success rate.
- Give each new experiment a distinct output label. Preserve controls, seeds, configuration, and raw traces so a result can be checked and repeated.
- Before interacting with a public swarm, establish the operator, exact protocol path and key, and agreed scope. Local experiments need no such step.

## Implementation work

- Use Plan mode with Sol High for substantial planning; the project defaults to Sol Medium for execution and Luna Max for subagents, with Fast mode preferred throughout. Choose another available model when the task or client requires it.
- For a substantial, multi-step change, make a concise task-specific plan with a small `Implementation status` table. Do not create phase files or a status table for a simple edit or a discussion.
- Delegate at most three independent, bounded tasks when parallel work should save time. Assign exclusive file ownership; keep dependent edits and integration with the main agent.
- A worker reviews its diff, runs focused checks, and reports changed files, results, and remaining risks. The main agent inspects the diff and integration before accepting the work or marking a task complete.
- Run focused checks appropriate to the change. Diagnose failures before retrying, and rerun previously passing checks when a later change could affect them. Use a broader suite when there is a concrete regression risk.
- Avoid speculative optimization and process documents that do not help complete the current task. Measure performance claims when practical.

## Presentations

- Keep slides concise as cues for a live explanation; put fuller explanations in speaker notes. Prefer clear diagrams when they clarify a mechanism, and preserve layouts the user has approved.
- Separate measured results from theoretical conditions and untested combinations. Keep exact experiment tables inspectable in an appendix when making a presentation.
