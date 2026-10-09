"""Export Step 24 assembled-group winners and their traced declarations.

The deadline is reconstructed from the winner process's publisher declaration
trace. It describes the local declared expiration at assembly; it is not a
follower's observed deadline or evidence of a global election.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
import sys


SOURCE = "publisher-declaration trace reconstruction"
INTERPRETATION = (
    "Winner and deadline are reconstructed from the assembled event and the "
    "winner process's latest preceding ad_started event. This is not a "
    "follower-view deadline or a global election claim. X's state is the "
    "latest X ad_started event by monotonic time at assembly; the expiration "
    "gap is descriptive and does not establish why a group formed. Follower "
    "views are the latest logged ad_view before the matching handshake in "
    "that follower process; they are not exact native pop records or causal "
    "explanations."
)

CSV_FIELDS = [
    "case",
    "seed",
    "trial_dir",
    "group_id",
    "leader_name",
    "leader_peer_id",
    "leader_step_id",
    "absolute_declared_expiration",
    "assembly_wall",
    "remaining_seconds",
    "leader_expiration",
    "leader_remaining_seconds",
    "leader_group_key",
    "x_expiration",
    "x_remaining_seconds",
    "expiration_gap_x_minus_leader",
    "expiration_comparison",
    "x_ad_active",
    "x_inactive_reason",
    "x_last_ad_started_expiration",
    "x_current_step_id",
    "x_in_roster",
    "x_last_group_key",
    "leader_x_same_group_key",
    "x_declaration_event_ref",
    "roster",
    "group_size",
    "source",
    "declaration_event_ref",
    "assembly_event_ref",
    "last_active_ad_finished_expiration",
    "last_active_ad_finished_accepted",
    "last_active_ad_finished_event_ref",
    "follower_views",
]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _finite(value, label: str) -> float:
    _require(isinstance(value, (int, float)) and not isinstance(value, bool)
             and math.isfinite(value), f"invalid {label}")
    return float(value)


def _read_json(path: Path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc


def _read_events(trial_dir: Path) -> tuple[list[dict], dict[int, list[dict]]]:
    path = trial_dir / "events.jsonl"
    try:
        lines = path.read_text().splitlines()
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    _require(bool(lines), f"empty trace: {path}")

    events = []
    seen_numbers: set[tuple[int, int]] = set()
    for line_number, line in enumerate(lines, 1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on {path} line {line_number}") from exc
        _require(isinstance(event, dict), f"invalid event on {path} line {line_number}")
        for field in ("kind", "peer", "other", "pid", "event_number", "wall_time", "monotonic", "detail"):
            _require(field in event, f"event missing {field} on {path} line {line_number}")
        _require(isinstance(event["kind"], str), f"invalid event kind on {path} line {line_number}")
        _require(isinstance(event["peer"], str) and isinstance(event["other"], str),
                 f"invalid event peer/key on {path} line {line_number}")
        _require(isinstance(event["pid"], int) and not isinstance(event["pid"], bool),
                 f"invalid event pid on {path} line {line_number}")
        _require(isinstance(event["event_number"], int) and not isinstance(event["event_number"], bool),
                 f"invalid event number on {path} line {line_number}")
        _finite(event["wall_time"], "event wall time")
        _finite(event["monotonic"], "event monotonic time")
        _require(isinstance(event["detail"], dict), f"invalid event detail on {path} line {line_number}")
        ref = (event["pid"], event["event_number"])
        _require(ref not in seen_numbers, f"duplicate process event reference {ref} in {path}")
        seen_numbers.add(ref)
        events.append(event)

    # A multiprocessing queue interleaves processes, and wall timestamps are
    # not an ordering guarantee. Only the emitting process's counter is used
    # to reconstruct declarations around that process's assembly event.
    per_process: dict[int, list[dict]] = {}
    for event in events:
        per_process.setdefault(event["pid"], []).append(event)
    for process_events in per_process.values():
        process_events.sort(key=lambda event: event["event_number"])
    return events, per_process


def _event_ref(event: dict | None):
    if event is None:
        return None
    return f"{event['pid']}:{event['event_number']}"


def _follower_view_diagnostics(assembly: dict, all_events: list[dict],
                               names_by_peer: dict[str, str], x_peer: str,
                               group_id: str) -> list[dict]:
    replies = [event for event in all_events
               if event["kind"] == "native_client_reply"
               and event["other"] == assembly["peer"]
               and event["detail"].get("code") == "BEGIN_ALLREDUCE"
               and event["detail"].get("group_id") == group_id
               and event["peer"] != assembly["peer"]]
    replies.sort(key=lambda event: (event["peer"], event["pid"], event["event_number"]))

    observations = []
    seen_followers = set()
    for reply in replies:
        follower = reply["peer"]
        if follower in seen_followers:
            continue
        seen_followers.add(follower)
        _require(follower in names_by_peer,
                 f"unknown follower peer {follower} in group {group_id}")
        step_id = reply["detail"].get("step_id")
        prior_in_process = [event for event in all_events
                            if event["pid"] == reply["pid"]
                            and event["event_number"] < reply["event_number"]
                            and event["peer"] == follower]
        handshakes = [event for event in prior_in_process
                      if event["kind"] == "native_client_wait"
                      and event["other"] == assembly["peer"]
                      and event["detail"].get("phase") == "handshake"
                      and event["detail"].get("step_id") == step_id]
        handshakes.sort(key=lambda event: event["event_number"])
        handshake = handshakes[-1] if handshakes else None
        view = None
        status = "no_matching_handshake"
        if handshake is not None:
            views = [event for event in prior_in_process
                     if event["kind"] == "ad_view"
                     and event["event_number"] < handshake["event_number"]]
            views.sort(key=lambda event: event["event_number"])
            view = views[-1] if views else None
            status = ("latest_logged_view_before_handshake" if view is not None
                      else "no_prior_logged_view")

        observed = {}
        if view is not None:
            entries = view["detail"].get("entries")
            _require(isinstance(entries, list), f"invalid ad_view entries for group {group_id}")
            for entry in entries:
                _require(isinstance(entry, dict), f"invalid ad_view entry for group {group_id}")
                peer = entry.get("peer")
                if peer in (assembly["peer"], x_peer):
                    observed[peer] = _finite(entry.get("expiration"),
                                              f"ad_view expiration for group {group_id}")
        observed_leader = observed.get(assembly["peer"])
        observed_x = observed.get(x_peer)
        x_visible = (x_peer in observed) if view is not None else None
        gap = (observed_x - observed_leader
               if observed_x is not None and observed_leader is not None else None)
        observations.append({
            "follower_name": names_by_peer[follower],
            "follower_peer_id": follower,
            "leader_peer_id": assembly["peer"],
            "step_id": step_id,
            "reply_event_ref": _event_ref(reply),
            "status": status,
            "handshake_event_ref": _event_ref(handshake),
            "handshake_wall": handshake["wall_time"] if handshake is not None else None,
            "handshake_monotonic": handshake["monotonic"] if handshake is not None else None,
            "view_event_ref": _event_ref(view),
            "view_group_key": view["other"] if view is not None else None,
            "observed_leader_expiration": observed_leader,
            "observed_X_expiration": observed_x,
            "x_visible": x_visible,
            "deadline_gap_in_view": gap,
            "observation_scope": (
                "latest logged ad_view before the same-process handshake; "
                "not an exact native pop record or a causal explanation"
            ),
        })
    return observations


def _row_for_assembly(assembly: dict, process_events: list[dict], all_events: list[dict], config: dict,
                      trial_dir: Path) -> dict:
    detail = assembly["detail"]
    group_id = detail.get("group_id")
    step_id = detail.get("step_id")
    roster = detail.get("roster")
    _require(isinstance(group_id, str) and group_id, "assembled event missing group_id")
    _require(isinstance(step_id, str) and step_id, f"assembled group {group_id} missing step_id")
    _require(isinstance(roster, list) and roster and all(isinstance(peer, str) and peer for peer in roster),
             f"assembled group {group_id} has invalid roster")
    _require(len(set(roster)) == len(roster), f"assembled group {group_id} has duplicate roster entries")
    _require(assembly["peer"] in roster, f"assembled group {group_id} winner is absent from roster")

    peer_ids = config.get("peer_ids")
    _require(isinstance(peer_ids, dict), f"missing peer_ids in {trial_dir / 'config.json'}")
    names_by_peer = {peer: name for name, peer in peer_ids.items()
                     if isinstance(name, str) and isinstance(peer, str)}
    _require(assembly["peer"] in names_by_peer,
             f"unknown assembled winner peer {assembly['peer']} in {trial_dir.name}")

    prior = [event for event in process_events
             if event["event_number"] < assembly["event_number"]
             and event["peer"] == assembly["peer"]]
    declarations = [event for event in prior if event["kind"] == "ad_started"]
    _require(bool(declarations),
             f"assembled group {group_id} has no prior publisher declaration for winner")
    declaration = declarations[-1]
    active = declaration["detail"].get("active")
    _require(isinstance(active, bool), f"invalid declaration active flag for group {group_id}")
    _require(active, f"latest publisher declaration before group {group_id} is inactive")
    expiration = _finite(declaration["detail"].get("expiration"),
                         f"declared expiration for group {group_id}")
    assembly_wall = _finite(assembly["wall_time"], f"assembly wall time for group {group_id}")
    assembly_monotonic = _finite(assembly["monotonic"],
                                 f"assembly monotonic time for group {group_id}")

    # ad_finished is emitted after a declaration's store attempt returns. Keep
    # it as a distinct observation from the local publisher declaration.
    writes = [event for event in prior
              if event["kind"] == "ad_finished"
              and event["other"] == declaration["other"]
              and event["detail"].get("active") is True
              and event["detail"].get("accepted") is True]
    last_write = writes[-1] if writes else None
    if last_write is not None:
        _finite(last_write["detail"].get("expiration"),
                f"ad_finished expiration for group {group_id}")

    case = config.get("case")
    _require(isinstance(case, dict) and isinstance(case.get("name"), str),
             f"missing case name in {trial_dir / 'config.json'}")
    seed = config.get("seed")
    _require(isinstance(seed, int) and not isinstance(seed, bool),
             f"invalid seed in {trial_dir / 'config.json'}")

    x_peer = peer_ids.get("X")
    _require(isinstance(x_peer, str) and x_peer,
             f"missing X peer id in {trial_dir / 'config.json'}")
    x_declarations = [event for event in all_events
                      if event["kind"] == "ad_started" and event["peer"] == x_peer
                      and event["monotonic"] <= assembly_monotonic]
    x_declarations.sort(key=lambda event: (event["monotonic"], event["pid"], event["event_number"]))
    x_declaration = x_declarations[-1] if x_declarations else None
    x_active = False
    x_expiration = None
    x_inactive_reason = "never_declared"
    x_last_ad_started_expiration = None
    x_last_group_key = None
    if x_declaration is not None:
        x_active = x_declaration["detail"].get("active")
        _require(isinstance(x_active, bool), f"invalid X declaration active flag for group {group_id}")
        x_last_group_key = x_declaration["other"] or None
        raw_x_expiration = x_declaration["detail"].get("expiration")
        if raw_x_expiration is not None:
            x_last_ad_started_expiration = _finite(
                raw_x_expiration, f"X last ad_started expiration for group {group_id}")
        if x_active:
            x_expiration = _finite(raw_x_expiration, f"X declared expiration for group {group_id}")
            x_inactive_reason = None
        else:
            x_inactive_reason = "inactive_declaration"

    x_remaining = x_expiration - assembly_wall if x_expiration is not None else None
    expiration_gap = x_expiration - expiration if x_expiration is not None else None
    if x_expiration is None:
        expiration_comparison = "x_not_advertising"
    elif assembly["peer"] == x_peer:
        expiration_comparison = "x_is_leader"
    elif math.isclose(expiration_gap, 0.0, rel_tol=0.0, abs_tol=1e-9):
        expiration_comparison = "tie"
    elif expiration_gap > 0:
        expiration_comparison = "leader_earlier"
    else:
        expiration_comparison = "x_earlier"

    x_steps = [event for event in all_events
               if event["kind"] == "step_start" and event["peer"] == x_peer
               and event["monotonic"] <= assembly_monotonic
               and isinstance(event["detail"].get("step_id"), str)]
    x_steps.sort(key=lambda event: (event["monotonic"], event["pid"], event["event_number"]))
    x_current_step_id = x_steps[-1]["detail"]["step_id"] if x_steps else None
    leader_group_key = declaration["other"] or None
    leader_x_same_group_key = (
        leader_group_key == x_last_group_key if x_last_group_key is not None else None
    )
    follower_views = _follower_view_diagnostics(assembly, all_events, names_by_peer,
                                                x_peer, group_id)

    return {
        "case": case["name"],
        "seed": seed,
        "trial_dir": trial_dir.name,
        "group_id": group_id,
        "leader_name": names_by_peer[assembly["peer"]],
        "leader_peer_id": assembly["peer"],
        "leader_step_id": step_id,
        "absolute_declared_expiration": expiration,
        "assembly_wall": assembly_wall,
        "remaining_seconds": expiration - assembly_wall,
        "leader_expiration": expiration,
        "leader_remaining_seconds": expiration - assembly_wall,
        "leader_group_key": leader_group_key,
        "x_expiration": x_expiration,
        "x_remaining_seconds": x_remaining,
        "expiration_gap_x_minus_leader": expiration_gap,
        "expiration_comparison": expiration_comparison,
        "x_ad_active": x_active,
        "x_inactive_reason": x_inactive_reason,
        "x_last_ad_started_expiration": x_last_ad_started_expiration,
        "x_current_step_id": x_current_step_id,
        "x_in_roster": x_peer in roster,
        "x_last_group_key": x_last_group_key,
        "leader_x_same_group_key": leader_x_same_group_key,
        "x_declaration_event_ref": _event_ref(x_declaration),
        "roster": roster,
        "group_size": len(roster),
        "source": SOURCE,
        "declaration_event_ref": _event_ref(declaration),
        "assembly_event_ref": _event_ref(assembly),
        "last_active_ad_finished_expiration": (
            _finite(last_write["detail"]["expiration"], "ad_finished expiration")
            if last_write else None
        ),
        "last_active_ad_finished_accepted": (
            last_write["detail"]["accepted"] if last_write else None
        ),
        "last_active_ad_finished_event_ref": _event_ref(last_write),
        "follower_views": follower_views,
        # Internal comparison key is omitted from both output formats.
        "_roster_identity": tuple(sorted(roster)),
    }


def export_suite(suite_dir: str | Path) -> list[dict]:
    """Read completed child trials and return deduplicated election rows."""
    suite_dir = Path(suite_dir).resolve()
    _require(suite_dir.is_dir(), f"suite directory does not exist: {suite_dir}")
    trial_dirs = sorted(path for path in suite_dir.iterdir()
                        if path.is_dir() and (path / "summary.json").is_file())
    _require(bool(trial_dirs), f"no completed trial directories in {suite_dir}")

    rows_by_group: dict[str, dict] = {}
    identities: dict[str, tuple[str, tuple[str, ...]]] = {}
    for trial_dir in trial_dirs:
        config = _read_json(trial_dir / "config.json")
        _require(isinstance(config, dict), f"invalid config in {trial_dir}")
        events, by_process = _read_events(trial_dir)
        for assembly in events:
            if assembly["kind"] != "assembled":
                continue
            process_events = by_process[assembly["pid"]]
            row = _row_for_assembly(assembly, process_events, events, config, trial_dir)
            group_id = row["group_id"]
            identity = (row["leader_peer_id"], row["_roster_identity"])
            if group_id in identities:
                _require(identities[group_id] == identity,
                         f"conflicting group_id {group_id}: leaders or rosters differ")
                continue
            identities[group_id] = identity
            row.pop("_roster_identity")
            rows_by_group[group_id] = row

    return sorted(rows_by_group.values(),
                  key=lambda row: (row["case"], row["seed"], row["group_id"]))


def write_exports(rows: list[dict], output_dir: str | Path) -> tuple[Path, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "elections.csv"
    json_path = output_dir / "elections.json"

    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            csv_row = dict(row)
            csv_row["roster"] = json.dumps(row["roster"], separators=(",", ":"))
            csv_row["follower_views"] = json.dumps(row["follower_views"], separators=(",", ":"))
            writer.writerow(csv_row)

    document = {
        "count": len(rows),
        "source": SOURCE,
        "interpretation": INTERPRETATION,
        "rows": rows,
    }
    json_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return csv_path, json_path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite_dir", type=Path,
                        help="suite directory containing trial subdirectories")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="directory outside the suite for elections.csv and elections.json")
    args = parser.parse_args(argv)

    suite_dir = args.suite_dir.resolve()
    output_dir = args.output_dir.resolve()
    _require(output_dir != suite_dir and suite_dir not in output_dir.parents,
             "--output-dir must be outside the suite directory")
    rows = export_suite(suite_dir)
    csv_path, json_path = write_exports(rows, output_dir)
    print(f"Exported {len(rows)} assembled groups")
    print(f"CSV: {csv_path}")
    print(f"JSON: {json_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
