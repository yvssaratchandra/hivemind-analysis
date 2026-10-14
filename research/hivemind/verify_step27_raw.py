"""Independently count Step 27 groups, attempts and retry witnesses from raw JSON.

Does not import the experiment analyzer or infer success from tensor results.
All membership outcomes are linked using the assembled member's exact step_id.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def ref(event):
    return f"{event['pid']}:{event['event_number']}"


def number_equal(a, b):
    if a is None or b in (None, ""):
        return a is None and b in (None, "")
    try:
        # Epoch timestamps are large: relative tolerance would hide tenths of
        # a second. JSON/CSV round-trip floats exactly, so use absolute tolerance.
        return math.isclose(float(a), float(b), rel_tol=0., abs_tol=1e-9)
    except (TypeError, ValueError):
        return False


def raw_trial(path):
    config = read_json(path / "config.json")
    events = [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines() if line.strip()]
    errors, by_ref, starts, results, groups = [], {}, {}, {}, {}
    names = {peer: name for name, peer in config["peer_ids"].items()}
    if len(names) != 8 or set(names.values()) != {"X", *(f"H{i}" for i in range(7))}:
        errors.append("expected eight distinct participants")
    closed = [e for e in events if e["kind"] == "trace_closed"]
    if (len(closed) != 1 or not events or events[-1] != closed[0] or
            closed[0].get("peer") != "" or closed[0].get("other") != "" or
            set(closed[0]["detail"].get("participants", [])) != set(names.values()) or
            closed[0]["detail"].get("pending") != [] or closed[0]["detail"].get("error") is not None):
        errors.append("invalid final nonparticipant closure marker")
    for event in events:
        if ref(event) in by_ref:
            errors.append("duplicate raw event reference")
        by_ref[ref(event)] = event
        detail, peer = event["detail"], event["peer"]
        if event["kind"] in ("step_start", "step_result"):
            key = peer, detail.get("step_id")
            target = starts if event["kind"] == "step_start" else results
            if key in target or peer not in names or not key[1]:
                errors.append(f"invalid or duplicate {event['kind']}: {key}")
            target[key] = event
        elif event["kind"] == "assembled":
            gid = detail.get("group_id")
            signature = (peer, detail.get("step_id"), detail.get("roster"), detail.get("members"))
            if not gid:
                errors.append("assembly missing group ID")
                continue
            if gid in groups:
                prior = groups[gid]
                pd = prior["detail"]
                if signature != (prior["peer"], pd.get("step_id"), pd.get("roster"), pd.get("members")):
                    errors.append(f"conflicting repeated assembly: {gid}")
            else:
                groups[gid] = event
    if starts.keys() != results.keys():
        errors.append("raw step_start/step_result keys differ")
    memberships, leads = defaultdict(list), defaultdict(list)
    for gid, event in groups.items():
        d = event["detail"]
        roster, members = d.get("roster", []), d.get("members", {})
        if (not roster or len(set(roster)) != len(roster) or set(roster) != set(members) or
                event["peer"] not in roster or any(peer not in names for peer in roster)):
            errors.append(f"invalid roster/member map: {gid}")
            continue
        if names.get(event["peer"]) == "X" and config["case"].get("response") == "unreliable":
            errors.append(f"unreliable X unexpectedly assembled: {gid}")
        leader_key = event["peer"], d.get("step_id")
        if leader_key not in starts or members[event["peer"]].get("step_id") != leader_key[1]:
            errors.append(f"leader attempt mismatch: {gid}")
        for peer, member in members.items():
            key = peer, member.get("step_id")
            if key not in starts or member != starts[key]["detail"]:
                # step_start can include instrumentation fields in addition to
                # the exact gather payload; compare the identity/value fields.
                source = starts.get(key, {}).get("detail", {})
                if key not in starts or any(member.get(k) != source.get(k) for k in ("name", "sequence", "step_id", "value")):
                    errors.append(f"member attempt mismatch: {gid}/{peer}")
                    continue
            if event["monotonic"] < starts[key]["monotonic"]:
                errors.append(f"assembly precedes member attempt: {gid}/{peer}")
            memberships[key].append(event)
        leads[leader_key].append(event)
    attempts = {}
    for key, event in starts.items():
        first = min(memberships[key], key=lambda e: e["monotonic"], default=None)
        lead = min(leads[key], key=lambda e: e["monotonic"], default=None)
        delta = first["monotonic"] - event["monotonic"] if first else None
        lead_delta = lead["monotonic"] - event["monotonic"] if lead else None
        attempts[key] = dict(peer_id=key[0], step_id=key[1], peer_name=names[key[0]],
                             capped_latency_seconds=min(max(delta, 0), 12) if delta is not None else 12.,
                             first_assembly_group_id=first["detail"]["group_id"] if first else None,
                             lead_latency_seconds=lead_delta,
                             first_lead_group_id=lead["detail"]["group_id"] if lead else None,
                             led_success=lead is not None, membership_success=first is not None)
    x = [r for r in attempts.values() if r["peer_name"] == "X"]
    honest = [r for r in attempts.values() if r["peer_name"] != "X"]
    start, end = config["started_monotonic"], config["started_monotonic"] + config["duration"]
    inside = [e for e in groups.values() if start <= e["monotonic"] < end]
    honest_memberships = sum(sum(names.get(p) != "X" for p in e["detail"]["roster"]) for e in inside)
    retries = raw_retries(events, config)
    metrics = dict(total_groups=len(groups), x_led_groups=sum(names[e["peer"]] == "X" for e in groups.values()),
                   x_submitted_attempts=len(x), x_led_attempts=sum(r["led_success"] for r in x),
                   x_led_by_2s_count=sum(r["lead_latency_seconds"] is not None and r["lead_latency_seconds"] <= 2 for r in x),
                   honest_submitted_attempts=len(honest),
                   honest_no_group_attempts=sum(not r["membership_success"] for r in honest),
                   honest_no_group_fraction=sum(not r["membership_success"] for r in honest) / len(honest) if honest else None,
                   honest_mean_capped_latency_seconds=statistics.mean(r["capped_latency_seconds"] for r in honest) if honest else None,
                   honest_group_memberships_in_submission_window=honest_memberships,
                   honest_peer_minutes=7 * config["duration"] / 60,
                   honest_group_memberships_per_honest_peer_minute=honest_memberships / (7 * config["duration"] / 60),
                   same_search_x_retry_count=len(retries))
    return dict(config=config, errors=errors, metrics=metrics, attempts=attempts, groups=groups,
                retries=retries, events_by_ref=by_ref)


def raw_retries(events, config):
    """Exact-pair re-selections witnessed within the same still-open X SEARCH."""
    x = config["peer_ids"]["X"]
    ads = [e for e in events if e["kind"] == "ad_started" and e["peer"] == x and e["detail"].get("active") is True]
    spans = {}
    for e in events:
        if e["peer"] == x and e["kind"] in ("search_span_start", "search_span_end"):
            spans.setdefault((e["pid"], e["detail"].get("search_id")), {})[e["kind"]] = e
    out = {}
    for e in events:
        if e["kind"] != "native_pop_selected" or e["other"] != x:
            continue
        d = e["detail"]
        new = d["selected_expiration"]
        past = {(r["peer"], r["expiration"]) for r in d.get("past_attempts", [])}
        if (x, new) in past:
            continue
        new_ads = [a for a in ads if a["monotonic"] <= e["monotonic"] and a["detail"].get("expiration") == new]
        for new_ad in new_ads:
            for span in spans.values():
                begin, end = span.get("search_span_start"), span.get("search_span_end")
                if not begin or not end or new_ad["pid"] != begin["pid"]:
                    continue
                if not begin["monotonic"] <= new_ad["monotonic"] <= e["monotonic"] <= end["monotonic"]:
                    continue
                old_values = {old for peer, old in past if peer == x and old < new and any(
                    a["pid"] == begin["pid"] and begin["monotonic"] <= a["monotonic"] <= e["monotonic"] and
                    a["detail"].get("expiration") == old for a in ads)}
                if old_values:
                    out[ref(e)] = dict(old_values=old_values, new_expiration=new,
                                       x_search_id=begin["detail"]["search_id"])
    return out


def csv_rows(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def verify_suite(suite, preservation=None):
    suite = Path(suite).resolve()
    manifest = read_json(suite / "suite.json")
    report = dict(experiment="step27", suite=str(suite), pass_=False, errors=[], trials=[], totals={})
    report["scope"] = "Independent raw assembly/member-attempt linkage, failure-inclusive capped metrics, exact renewed-pair witnesses, exported row checks, completion/provenance hashes. No tensor-success filtering."
    errors = report["errors"]
    import step27_control_plane as harness
    if manifest.get("experiment") != "step27" or manifest.get("jobs") != harness.make_jobs(manifest.get("phase")):
        errors.append("manifest differs from exact job matrix")
    harness.validate_sources(suite, read_json(suite / "provenance.json"))
    attempts_csv = csv_rows(suite / "attempts.csv")
    groups_csv = csv_rows(suite / "groups.csv")
    retries_csv = csv_rows(suite / "retries.csv")
    summaries = {r["trial_dir"]: r for r in read_json(suite / "summary.json")["trials"]}
    for job in manifest["jobs"]:
        name = harness.trial_name(job)
        path = suite / name
        row_errors = []
        try:
            raw = raw_trial(path)
            row_errors.extend(raw["errors"])
            harness.validate_trial_config(raw["config"], job)
            completion = read_json(path / "completion.json")
            if completion.get("all_peers_shutdown") is not True:
                row_errors.append("completion lacks shutdown evidence")
            for file, digest in completion["file_sha256"].items():
                if sha256(path / file) != digest:
                    row_errors.append(f"completion hash changed: {file}")
            stored = read_json(path / "control_analysis.json")
            if stored.get("integrity_pass") is not True:
                row_errors.append("stored control analysis is not passing")
            for key, value in raw["metrics"].items():
                for exported in (stored["summary_metrics"], summaries[name]["summary_metrics"]):
                    if not number_equal(value, exported.get(key)):
                        row_errors.append(f"raw metric mismatch: {key}, expected {value}, got {exported.get(key)}")
            ac = [r for r in attempts_csv if r["trial_dir"] == name]
            if len(ac) != len(raw["attempts"]):
                row_errors.append("exported attempt count differs from raw step_start count")
            seen_attempts = set()
            for row in ac:
                key = row["peer_id"], row["step_id"]
                expected = raw["attempts"].get(key)
                if expected is None or key in seen_attempts:
                    row_errors.append(f"unknown/duplicate exported attempt: {key}")
                    continue
                seen_attempts.add(key)
                for field in ("capped_latency_seconds", "lead_latency_seconds"):
                    if not number_equal(expected[field], row.get(field)):
                        row_errors.append(f"attempt {key}: {field} differs from raw")
                for field in ("first_assembly_group_id", "first_lead_group_id"):
                    if (expected[field] or "") != row.get(field, ""):
                        row_errors.append(f"attempt {key}: {field} differs from raw")
                for field in ("led_success", "membership_success"):
                    if str(expected[field]).lower() != row.get(field, "").lower():
                        row_errors.append(f"attempt {key}: {field} differs from raw")
            gc = [r for r in groups_csv if r["trial_dir"] == name]
            if len(gc) != len(raw["groups"]) or {r["group_id"] for r in gc} != set(raw["groups"]):
                row_errors.append("exported groups differ from unique raw group IDs")
            for row in gc:
                event = raw["groups"].get(row["group_id"])
                if event is None:
                    continue
                if row.get("leader_peer_id") != event["peer"] or sorted(json.loads(row["roster"])) != sorted(event["detail"]["roster"]):
                    row_errors.append(f"exported leader/roster differs: {row['group_id']}")
                marker = raw["events_by_ref"].get(row.get("assembly_deadline_event_ref"))
                if (marker is None or marker["kind"] != "assembly_deadline_state" or marker["peer"] != event["peer"] or
                        marker["detail"].get("step_id") != event["detail"].get("step_id") or
                        marker["monotonic"] > event["monotonic"] or
                        not number_equal(marker["detail"].get("leader_expiration"), row.get("leader_expiration_at_assembly_deadline"))):
                    row_errors.append(f"invalid leader expiration snapshot: {row['group_id']}")
            rc = [r for r in retries_csv if r["trial_dir"] == name]
            if len(rc) != len(raw["retries"]) or {r["selection_event_ref"] for r in rc} != set(raw["retries"]):
                row_errors.append("exported renewed-pair witnesses differ from independent raw reconstruction")
            for row in rc:
                witness = raw["retries"].get(row["selection_event_ref"])
                if witness and (float(row["old_pair_expiration"]) not in witness["old_values"] or
                                row["x_search_id"] != witness["x_search_id"]):
                    row_errors.append("exported retry has invalid old pair or X search")
            metrics = raw["metrics"]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            row_errors.append(f"{type(exc).__name__}: {exc}")
            metrics = {}
        errors.extend(f"{name}: {error}" for error in row_errors)
        report["trials"].append(dict(trial_dir=name, pass_=not row_errors, errors=row_errors, metrics=metrics,
                                     inputs_sha256={f: sha256(path / f) for f in ("config.json", "events.jsonl", "summary.json") if (path / f).exists()}))
    for key in ("total_groups", "x_led_groups", "x_submitted_attempts", "x_led_attempts", "x_led_by_2s_count"):
        report["totals"][key] = sum(r["metrics"].get(key, 0) for r in report["trials"])
    if preservation is not None:
        baseline = read_json(preservation)
        root = Path(__file__).resolve().parents[2]
        changed = [file for file, digest in baseline["files"].items() if not (root / file).exists() or sha256(root / file) != digest]
        report["preservation"] = dict(count=len(baseline["files"]), changed=changed, pass_=not changed)
        if changed:
            errors.append("prior source/raw evidence preservation failed")
    report["pass"] = not errors
    report.pop("pass_", None)
    report["verifier_sha256"] = sha256(__file__)
    report["exports_sha256"] = {p.name: sha256(p) for p in suite.glob("*.csv")}
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, type=Path)
    parser.add_argument("--preservation", type=Path)
    args = parser.parse_args()
    report = verify_suite(args.suite, args.preservation)
    target = args.suite / "raw_verification.json"
    target.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    print(json.dumps(dict(pass_=report["pass"], totals=report["totals"], report=str(target))))
    raise SystemExit(0 if report["pass"] else 1)


if __name__ == "__main__":
    main()
