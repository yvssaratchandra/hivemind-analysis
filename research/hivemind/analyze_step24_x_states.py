"""Offline reconstruction of X's roles, searches, and advertisements in Step 24.

Reads a complete grouping suite; never launches peers or rewrites its input.
Only the Python standard library and the existing research analyzers are used.
"""

from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import sys

from analyze_step24_lifetime import analyse_suite, _event_stream, read_json, require, finite_number
from export_step24_elections import export_suite


STATES = ("searching", "leading", "following", "between_rounds", "initial_startup", "transition")
TRACKED = {"step_start", "step_result", "search_start", "search_end", "assembled",
           "native_client_reply", "native_client_exception", "allreduce_start",
           "allreduce_end", "ad_started"}


def event_ref(event):
    return f"{event['pid']}:{event['event_number']}"


def _split_intervals(intervals, cutoff):
    """Keep complete elapsed time, splitting the reporting window and training readiness."""
    output = []
    for interval in intervals:
        boundaries = [interval["start"], interval["end"]]
        ready = interval["training_ready"]
        if interval["start"] < cutoff < interval["end"]:
            boundaries.append(cutoff)
        if (interval["state"] == "between_rounds" and ready is not None and
                interval["start"] < ready < interval["end"]):
            boundaries.append(ready)
        boundaries.sort()
        for left, right in zip(boundaries, boundaries[1:]):
            row = dict(interval, start=left, end=right, duration=right-left,
                       period="submission" if left < cutoff else "drain")
            row["between_substate"] = None
            if row["state"] == "between_rounds":
                row["between_substate"] = "training_delay" if left < ready else "ready_wait"
            output.append(row)
    return output


def reconstruct_trial(config, events, elections):
    """Reconstruct validated events; event boundaries are observed, not inferred native stages.

    Search episodes retain their identity through follower waits and assembly cleanup.
    The Searching *state* ends as soon as X commits to following or assembles a group.
    Advertisement changes cannot end an established group role.
    """
    case, seed = config["case"]["name"], config["seed"]
    trial_id = f"{case}_s{seed}"
    x = config["peer_ids"]["X"]
    start = finite_number(config["started_monotonic"], "trial start")
    duration = finite_number(config["duration"], "trial duration")
    require(duration > 0, "nonpositive trial duration")
    cutoff = start + duration
    events = sorted(events, key=lambda e: (e["monotonic"], e["pid"], e["event_number"]))
    full_end = max([cutoff] + [e["monotonic"] for e in events if e["kind"] == "step_result"])
    assemblies = {e["detail"]["group_id"]: e for e in events if e["kind"] == "assembled"}
    elections = [r for r in elections if r["case"] == case and r["seed"] == seed]
    groups = {r["group_id"]: r for r in elections}
    require(len(groups) == len(elections), "duplicate exported group ID")
    require(set(groups) == set(assemblies), "election export and raw assemblies differ")
    ready_by_step = {}
    for e in events:
        if e["peer"] == x and e["kind"] == "training_scheduled":
            sid = e["detail"]["step_id"]
            require(sid not in ready_by_step, f"duplicate training schedule {sid}")
            ready_by_step[sid] = finite_number(e["detail"]["ready"], "training readiness")

    context = dict(state="initial_startup", step_id=None, search_id=None,
                   group_id=None, group_roster=None, group_expiration_at_assembly=None,
                   group_leader_peer_id=None, ad_active=False, declared_expiration=None,
                   historical_expiration=None, ad_group_key=None, declaration_event_ref=None,
                   training_ready=None, state_event_ref=None)
    intervals, searches = [], []
    cursor = start
    episode = None
    active_step = None
    ordinal = Counter()
    completed_steps = set()
    seen_steps = set()
    attached_groups = set()
    running_group = None

    def flush(t):
        nonlocal cursor
        t = min(t, full_end)
        require(t >= cursor, "state clock moved backwards")
        if t > cursor:
            intervals.append(dict(context, case=case, seed=seed, start=cursor, end=t,
                                  duration=t-cursor))
        cursor = t

    def set_state(state, event):
        context["state"] = state
        context["state_event_ref"] = event_ref(event)

    def clear_group():
        context.update(group_id=None, group_roster=None, group_expiration_at_assembly=None,
                       group_leader_peer_id=None)

    def attach_group(gid):
        require(gid in groups, f"unlogged group {gid}")
        row = groups[gid]
        require(x in row["roster"], f"X absent from its group {gid}")
        context.update(group_id=gid, group_roster=row["roster"],
                       group_expiration_at_assembly=row["leader_expiration"],
                       group_leader_peer_id=row["leader_peer_id"])
        attached_groups.add(gid)

    for event in events:
        if event["peer"] != x or event["kind"] not in TRACKED:
            continue
        t, kind, detail = event["monotonic"], event["kind"], event["detail"]
        require(t >= start, "X lifecycle event precedes trial")
        flush(t)
        sid = detail.get("step_id")
        if kind == "step_start":
            require(active_step is None and episode is None and running_group is None,
                    "step starts before previous step finishes")
            require(sid and sid not in seen_steps, "missing or duplicate step start")
            seen_steps.add(sid)
            active_step = sid
            clear_group()
            context.update(step_id=sid, search_id=None, training_ready=None)
            set_state("transition", event)
        elif kind == "search_start":
            require(active_step == sid and episode is None, "overlapping or unattached search start")
            require(running_group is None, "retry overlaps a running group")
            ordinal[sid] += 1
            search_id = f"{trial_id}:{sid}:search_{ordinal[sid]}"
            episode = dict(case=case, seed=seed, search_id=search_id, step_id=sid,
                           ordinal=ordinal[sid], start=t, start_event_ref=event_ref(event),
                           started_in_submission=t < cutoff, group_key=detail.get("group_key"),
                           led_group_ids=[], followed_group_ids=[])
            clear_group()
            context.update(search_id=search_id, training_ready=None)
            set_state("searching", event)
        elif kind == "assembled":
            gid = detail["group_id"]
            if gid in attached_groups:
                require(context["group_id"] == gid, "conflicting duplicate X assembly")
                continue
            require(episode is not None and sid == active_step and context["state"] == "searching",
                    "X assembly outside its searching state")
            require(groups[gid]["leader_peer_id"] == x, "X assembly leader mismatch")
            episode["led_group_ids"].append(gid)
            attach_group(gid)
            set_state("leading", event)
        elif kind == "native_client_reply":
            require(episode is not None and active_step == sid, "client reply outside search")
            code = detail["code"]
            if code == "ACCEPTED":
                require(context["state"] == "searching", "accepted while committed to a group")
                clear_group()
                context["group_leader_peer_id"] = event["other"]
                set_state("following", event)
            elif code == "BEGIN_ALLREDUCE":
                require(context["state"] == "following", "follower group without acceptance")
                gid = detail["group_id"]
                require(groups[gid]["leader_peer_id"] == event["other"] != x,
                        "follower leader identity mismatch")
                episode["followed_group_ids"].append(gid)
                attach_group(gid)
            elif detail.get("phase") == "waiting_for_group":
                require(context["state"] == "following" and context["group_id"] is None,
                        "unexpected response after group formation")
                clear_group()
                set_state("searching", event)
        elif kind == "native_client_exception":
            if context["state"] == "following" and context["group_id"] is None:
                require(episode is not None, "follower exception outside search")
                clear_group()
                set_state("searching", event)
        elif kind == "search_end":
            require(episode is not None and sid == active_step == episode["step_id"],
                    "unpaired or mismatched search end")
            require(not (episode["led_group_ids"] and episode["followed_group_ids"]),
                    "search simultaneously led and followed")
            outcome = ("led" if episode["led_group_ids"] else
                       "followed" if episode["followed_group_ids"] else "no_group")
            episode.update(end=t, duration=t-episode["start"], end_event_ref=event_ref(event),
                           outcome=outcome, completed_in_drain=t >= cutoff)
            searches.append(episode)
            episode = None
            if context["state"] == "searching" or context["group_id"] is None:
                set_state("transition", event)
        elif kind == "allreduce_start":
            require(active_step == sid and context["state"] in ("leading", "following"),
                    "group execution without formed role")
            require(detail["group_id"] == context["group_id"] and running_group is None,
                    "overlapping or mismatched group execution")
            running_group = detail["group_id"]
        elif kind == "allreduce_end":
            require(detail["group_id"] == running_group and sid == active_step,
                    "unpaired group execution end")
            running_group = None
        elif kind == "step_result":
            require(active_step == sid and episode is None and running_group is None,
                    "step result precedes lifecycle closure")
            require(sid in ready_by_step, "step result lacks training schedule")
            completed_steps.add(sid)
            active_step = None
            clear_group()
            context.update(search_id=None, training_ready=ready_by_step[sid])
            set_state("between_rounds", event)
        elif kind == "ad_started":
            active = detail.get("active")
            require(isinstance(active, bool), "invalid X advertisement state")
            expiration = finite_number(detail.get("expiration"), "X ad expiration")
            context.update(ad_active=active, declared_expiration=expiration if active else None,
                           historical_expiration=expiration, ad_group_key=event["other"],
                           declaration_event_ref=event_ref(event))

    require(episode is None and active_step is None and running_group is None, "unclosed X lifecycle")
    require(completed_steps == seen_steps and seen_steps, "missing X completion")
    require(set(ready_by_step) == completed_steps, "orphaned training schedule")
    flush(full_end)
    timeline = _split_intervals(intervals, cutoff)
    for row in timeline:
        row.update(start_relative=row["start"]-start, end_relative=row["end"]-start)
    require(timeline and timeline[0]["start"] == start and timeline[-1]["end"] == full_end,
            "incomplete state timeline")
    require(all(math.isclose(a["end"], b["start"], abs_tol=1e-9, rel_tol=0)
                for a, b in zip(timeline, timeline[1:])), "gapped state timeline")
    durations = {period: {state: 0.0 for state in STATES} for period in ("submission", "drain")}
    between = {period: {state: 0.0 for state in ("training_delay", "ready_wait")}
               for period in durations}
    for row in timeline:
        durations[row["period"]][row["state"]] += row["duration"]
        if row["between_substate"]:
            between[row["period"]][row["between_substate"]] += row["duration"]
    require(math.isclose(sum(durations["submission"].values()), duration, abs_tol=1e-6, rel_tol=0),
            "submission state durations do not cover the window")
    require(math.isclose(sum(durations["drain"].values()), full_end-cutoff, abs_tol=1e-6, rel_tol=0),
            "drain durations do not cover completion time")

    starts = [r["start"] for r in timeline]
    other_groups = []
    for gid, group in groups.items():
        if group["leader_peer_id"] == x:
            continue
        event = assemblies[gid]
        t = event["monotonic"]
        index = bisect_right(starts, t) - 1
        require(index >= 0 and t <= full_end, "assembly outside observation window")
        state = timeline[index]
        # Durations use half-open intervals. At the final endpoint, use the last observed state.
        other_groups.append(dict(group, assembly_monotonic=t, x_state=state["state"],
                                 x_search_id=state["search_id"], x_step_id=state["step_id"],
                                 x_formed_group_id=state["group_id"],
                                 period="submission" if t < cutoff else "drain",
                                 x_state_event_ref=state["state_event_ref"],
                                 x_is_member_of_other_group=x in group["roster"],
                                 x_ad_unexpired=bool(group["x_ad_active"] and
                                                    group["x_remaining_seconds"] > 0),
                                 potential_deadline_competition=bool(
                                     state["state"] == "searching" and group["x_ad_active"] and
                                     x not in group["roster"] and
                                     group["x_remaining_seconds"] > 0 and
                                     group["leader_remaining_seconds"] > 0 and
                                     group["leader_x_same_group_key"] is True)))
    outcomes = Counter(r["outcome"] for r in searches)
    own_ids = {gid for r in searches for gid in r["led_group_ids"]}
    require(own_ids == {gid for gid, r in groups.items() if r["leader_peer_id"] == x},
            "X-led search attribution does not reconcile")
    summary = dict(case=case, seed=seed, submission_seconds=duration, drain_seconds=full_end-cutoff,
                   searches_started=len(searches), searches_led=outcomes["led"],
                   searches_followed=outcomes["followed"], searches_no_group=outcomes["no_group"],
                   led_search_fraction=outcomes["led"]/len(searches) if searches else None,
                   steps_started=len(seen_steps), retry_searches=len(searches)-len(seen_steps),
                   x_led_groups=len(own_ids), all_groups=len(groups), other_groups=len(other_groups),
                   submission_state_seconds=durations["submission"], drain_state_seconds=durations["drain"],
                   submission_between_seconds=between["submission"], drain_between_seconds=between["drain"],
                   other_groups_by_state={period: dict(Counter(r["x_state"] for r in other_groups
                                                               if r["period"] == period))
                                          for period in durations},
                   candidate_other_groups={period: sum(r["potential_deadline_competition"]
                                                       for r in other_groups if r["period"] == period)
                                           for period in durations})
    for period in ("submission", "drain"):
        selected = [r for r in searches if r["started_in_submission"] == (period == "submission")]
        counts = Counter(r["outcome"] for r in selected)
        summary[f"{period}_searches_started"] = len(selected)
        for outcome in ("led", "followed", "no_group"):
            summary[f"{period}_searches_{outcome}"] = counts[outcome]
        summary[f"{period}_led_search_fraction"] = counts["led"]/len(selected) if selected else None
    return dict(timeline=timeline, searches=searches, other_groups=other_groups, summary=summary)


def aggregate_trials(trials):
    grouped = defaultdict(list)
    for trial in trials:
        grouped[trial["summary"]["case"]].append(trial["summary"])
    rows = []
    for case, summaries in sorted(grouped.items()):
        additive = ("submission_seconds", "drain_seconds", "searches_started", "searches_led",
                    "searches_followed", "searches_no_group", "steps_started", "retry_searches",
                    "x_led_groups", "all_groups", "other_groups")
        row = dict(case=case, trials=len(summaries), seeds=[r["seed"] for r in summaries])
        row.update({key: sum(r[key] for r in summaries) for key in additive})
        row["led_search_fraction"] = row["searches_led"] / row["searches_started"]
        for period in ("submission", "drain"):
            for outcome in ("started", "led", "followed", "no_group"):
                key = f"{period}_searches_{outcome}"
                row[key] = sum(r[key] for r in summaries)
            count = row[f"{period}_searches_started"]
            row[f"{period}_led_search_fraction"] = row[f"{period}_searches_led"]/count if count else None
            row[f"{period}_state_seconds"] = {state: sum(r[f"{period}_state_seconds"][state]
                                                         for r in summaries) for state in STATES}
            row[f"{period}_between_seconds"] = {state: sum(r[f"{period}_between_seconds"][state]
                                                           for r in summaries)
                                                 for state in ("training_delay", "ready_wait")}
            row[f"{period}_state_fraction"] = {
                state: seconds/row[f"{period}_seconds"] if row[f"{period}_seconds"] else 0.0
                for state, seconds in row[f"{period}_state_seconds"].items()}
            row.setdefault("other_groups_by_state", {})[period] = {
                state: sum(r["other_groups_by_state"][period].get(state, 0) for r in summaries)
                for state in STATES}
            row.setdefault("candidate_other_groups", {})[period] = sum(
                r["candidate_other_groups"][period] for r in summaries)
        rows.append(row)
    return rows


def _write_csv(path, rows):
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v
                             for k, v in row.items()})


def _summary_csv_rows(summaries):
    """Expose durations as ordinary columns while keeping structured JSON in analysis.json."""
    output = []
    for summary in summaries:
        row = {k: v for k, v in summary.items() if not isinstance(v, dict)}
        for period in ("submission", "drain"):
            for state, seconds in summary[f"{period}_state_seconds"].items():
                row[f"{period}_{state}_seconds"] = seconds
                total = summary[f"{period}_seconds"]
                row[f"{period}_{state}_percent"] = 100*seconds/total if total else 0.0
                row[f"{period}_other_groups_{state}"] = summary["other_groups_by_state"][period].get(state, 0)
            for substate, seconds in summary[f"{period}_between_seconds"].items():
                row[f"{period}_between_{substate}_seconds"] = seconds
            row[f"{period}_candidate_other_groups"] = summary["candidate_other_groups"][period]
        output.append(row)
    return output


def render_report(phase, summaries, totals):
    lines = ["# Step 24: X's state and search outcomes", "",
             "Offline reconstruction of existing private local traces. No new network experiment.", "",
             "## Implementation status", "", "| Work | Status |", "|---|---|",
             "| Trace validation and state reconstruction | Complete |",
             "| Search metrics and deadline comparisons | Complete |",
             "| Count, duration, and provenance reconciliation | Complete |", "",
             f"Input phase: **{phase}**. {totals['trial_count']} trials; "
             f"{totals['searches_started']} X searches: {totals['searches_led']} led, "
             f"{totals['searches_followed']} followed, {totals['searches_no_group']} without a group.", "",
             "## Searches", "", "| Policy | Searches | Led | Followed | No group | Led/search |",
             "|---|---:|---:|---:|---:|---:|"]
    for r in summaries:
        fraction = r['submission_led_search_fraction']
        rendered_fraction = f"{fraction:.1%}" if fraction is not None else "n/a"
        lines.append(f"| {r['case']} | {r['submission_searches_started']} | {r['submission_searches_led']} | "
                     f"{r['submission_searches_followed']} | {r['submission_searches_no_group']} | {rendered_fraction} |")
    lines += ["", "A search producing an X-led assembly is credited even if that group later fails.",
              "The table counts searches starting within submission windows, including their later outcomes.",
              "Retries have distinct search IDs within the same step; any starting during drain are separate.",
              "These finite local fractions",
              "are not deployment success rates. Existing group counts and tensor diagnostics are unchanged.", "",
              "## Time within the submission window", "",
              "Each policy uses equal submission windows. Percentages pool seconds across seeds.",
              "The extra completion time is reported separately in the CSV/JSON outputs.", "",
              "| Policy | Searching | Leading | Following | Between rounds | Initial/transition |",
              "|---|---:|---:|---:|---:|---:|"]
    for r in summaries:
        f = r["submission_state_fraction"]
        lines.append(f"| {r['case']} | {f['searching']:.1%} | {f['leading']:.1%} | "
                     f"{f['following']:.1%} | {f['between_rounds']:.1%} | "
                     f"{f['initial_startup']+f['transition']:.1%} |")
    lines += ["", "## Other-led assemblies during submission", "",
              "| Policy | X searching | X leading | X following | X between rounds | Initial/transition |",
              "|---|---:|---:|---:|---:|---:|"]
    for r in summaries:
        c = r["other_groups_by_state"]["submission"]
        lines.append(f"| {r['case']} | {c['searching']} | {c['leading']} | {c['following']} | "
                     f"{c['between_rounds']} | {c['initial_startup']+c['transition']} |")
    lines += ["", "## Definitions and limits", "",
              "Searching starts at a logged search entry and stops at acceptance by another leader,",
              "X's assembly, or search exit. Following includes waiting after ACCEPTED and participating",
              "in the formed group. Leading starts at X's assembly and lasts through observed step",
              "completion or entry into a retry. Advertisement withdrawal and background search exit",
              "do not end Leading. Between rounds starts at observed completion and ends at the next",
              "step submission; it is split into time before training readiness and subsequent waiting.", "",
              "Initial launch delay and intervals between recorded lifecycle transitions are explicit",
              "residual states. Timings use the common local monotonic clock and logged boundaries;",
              "step completion is observed by harness polling. No sub-event native stage precision is claimed.", "",
              "The primary time window is [trial start, submission cutoff). Drain ends at the last",
              "observed step result across all peers, excluding process shutdown. Searching outcomes",
              "include later assembly/closure. Other-led assemblies are labeled submission or drain.", "",
              "Advertisement activity describes local publisher declaration state, independently of role.",
              "Inactive X has no current declared expiration; historical expiration is retained separately.",
              "Signed gap = X expiration minus leader expiration: positive means the leader is earlier.",
              "Deadline comparisons and latest logged follower views are descriptive; the exact queue",
              "dequeue and causal reason for every leader choice were not instrumented.", "",
              "A potential deadline comparison requires X to be Searching and outside the winner's roster,",
              "both declarations unexpired",
              "at assembly, and matching advertised group keys. It does not establish visibility to every",
              "follower, admission, sufficient followers, or an X loss. Multiple groups can coexist.", "",
              "## Artifacts", "",
              "- `timeline.csv`: intervals, roles, independent ad state, source references and period.",
              "- `searches.csv`: search ordinals, outcomes and exact group attribution.",
              "- `other_groups.csv`: every other-led assembly annotated with X's state and both expirations.",
              "- `other_groups_while_searching.csv`: the Searching subset, including drain labels.",
              "- `trial_summary.csv`, `policy_summary.csv`, `analysis.json`: counts and time totals.",
              "- `provenance.json`, `harness/`: hashed inputs and snapshotted extraction code.", "",
              "Repeat with a fresh label:", "", "```sh",
              "python research/hivemind/analyze_step24_x_states.py <complete-grouping-suite> --label <fresh-label>",
              "```", ""]
    return "\n".join(lines)


def analyze_to_output(suite_dir, output_dir):
    suite_dir, output_dir = Path(suite_dir).resolve(), Path(output_dir).resolve()
    require(not output_dir.exists(), f"output already exists: {output_dir}")
    require(output_dir != suite_dir and suite_dir not in output_dir.parents, "output must be outside input suite")
    checked = analyse_suite(suite_dir)
    require(checked["outcome"] == "grouping" and checked["complete"] and checked["control_plane_pass"],
            "input must be a complete passing grouping suite")
    elections = export_suite(suite_dir)
    trials = []
    for trial in checked["trials"]:
        path = Path(trial["source"])
        rebuilt = reconstruct_trial(read_json(path/"config.json"), _event_stream(path), elections)
        summary = rebuilt["summary"]
        require(summary["x_led_groups"] == trial["x_assembled_leads"] and
                summary["all_groups"] == trial["total_assembled_groups"], "original grouping counts changed")
        trials.append(rebuilt)
    policies = aggregate_trials(trials)
    keys = ("searches_started", "searches_led", "searches_followed", "searches_no_group",
            "steps_started", "retry_searches", "all_groups", "x_led_groups", "other_groups")
    totals = {key: sum(t["summary"][key] for t in trials) for key in keys}
    totals["trial_count"] = len(trials)
    require(totals["searches_started"] == sum(totals[k] for k in
                                             ("searches_led", "searches_followed", "searches_no_group")),
            "search outcomes fail reconciliation")
    require(totals["all_groups"] == len(elections) and
            totals["x_led_groups"] + totals["other_groups"] == totals["all_groups"],
            "group totals fail reconciliation")
    result = dict(phase=checked["phase"], input_suite=str(suite_dir), totals=totals,
                  trials=[t["summary"] for t in trials], policies=policies,
                  interpretation="Observed operational roles are separate from local ad state; deadline gaps do not prove losses.")
    # Validate everything before creating outputs. Repeated labels never overwrite evidence.
    output_dir.mkdir(parents=True)
    for key, filename in (("timeline", "timeline.csv"), ("searches", "searches.csv"),
                          ("other_groups", "other_groups.csv")):
        _write_csv(output_dir/filename, [r for t in trials for r in t[key]])
    _write_csv(output_dir/"other_groups_while_searching.csv",
               [r for t in trials for r in t["other_groups"] if r["x_state"] == "searching"])
    _write_csv(output_dir/"trial_summary.csv", _summary_csv_rows(result["trials"]))
    _write_csv(output_dir/"policy_summary.csv", _summary_csv_rows(policies))
    (output_dir/"analysis.json").write_text(json.dumps(result, indent=2, allow_nan=False)+"\n")
    (output_dir/"report.md").write_text(render_report(checked["phase"], policies, totals))
    snapshot = output_dir/"harness"
    snapshot.mkdir()
    hashes = {}
    for name in (Path(__file__).name, "analyze_step24_lifetime.py", "export_step24_elections.py",
                 "test_step24_x_states.py"):
        data = (Path(__file__).parent/name).read_bytes()
        (snapshot/name).write_bytes(data)
        hashes[name] = hashlib.sha256(data).hexdigest()
    input_hashes = {str(p.relative_to(suite_dir)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in sorted(suite_dir.rglob("*")) if p.is_file() and
                    p.name in ("events.jsonl", "config.json", "summary.json", "suite.json", "provenance.json")}
    (output_dir/"provenance.json").write_text(json.dumps(dict(input_suite=str(suite_dir),
                    input_sha256=input_hashes, extraction_sha256=hashes, python=sys.version,
                    argv=sys.argv, primary_window="submission", drain_endpoint="last step_result across peers"),
                    indent=2)+"\n")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite_dir", type=Path)
    parser.add_argument("--label", required=True)
    args = parser.parse_args(argv)
    require(re.fullmatch(r"[A-Za-z0-9_-]+", args.label), "invalid output label")
    output = Path(__file__).resolve().parent/"results"/f"step24_x_states_{args.label}"
    result = analyze_to_output(args.suite_dir, output)
    print(json.dumps(result["totals"], indent=2))
    print(f"Saved {output}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, KeyError) as exc:
        print(f"state analysis failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
