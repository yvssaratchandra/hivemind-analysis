"""Sample one Hivemind DHT key without publishing records.

For an averaging key (``<prefix>.0b<bits>``), boolean subrecords are availability
advertisements and their expirations are matchmaking deadlines. Other DHT keys can
still provide wide-area lookup timings, but their expirations have different meanings.

The default schedule samples for 20 seconds every 5 minutes over 24 hours. A single
read every few minutes would miss most short-lived averaging advertisements.

Example:
    python research/observe_dht_key.py --initial-peer MULTIADDR
        --key my_training_grad_averager.0b --output observations.jsonl

Use --duration-seconds 45 --period-seconds 15 --burst-seconds 10 for a short smoke run.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import hivemind
from hivemind.averaging.key_manager import is_valid_group


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def summarize(result, received_at: float, averaging_key: bool) -> dict:
    """Keep only record identities, expiry times and boolean availability flags."""
    if result is None:
        return {"records": [], "record_count": 0, "earliest_active_deadline_s": None}

    values = result.value if isinstance(result.value, dict) else {}
    records = []
    for subkey, entry in values.items():
        if not hasattr(entry, "expiration_time"):
            continue
        expiration = float(entry.expiration_time)
        state = entry.value if averaging_key and isinstance(entry.value, bool) else None
        records.append(
            {
                "subkey": subkey.hex() if isinstance(subkey, bytes) else str(subkey),
                "expiration_time": expiration,
                "remaining_s": expiration - received_at,
                "active": state,
            }
        )

    live_deadlines = [
        row["expiration_time"] for row in records if row["active"] is True and row["remaining_s"] > 0
    ]
    return {
        "record_expiration_time": float(result.expiration_time),
        "records": records,
        "record_count": len(records),
        "earliest_active_deadline_s": min(live_deadlines) - received_at if live_deadlines else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--initial-peer", action="append", required=True, help="Bootstrap multiaddr; repeat as needed")
    parser.add_argument("--key", required=True, help="Exact DHT key to sample")
    parser.add_argument("--output", type=Path, required=True, help="Append-only JSONL file")
    parser.add_argument("--duration-seconds", type=float, default=24 * 60 * 60)
    parser.add_argument("--period-seconds", type=float, default=5 * 60)
    parser.add_argument("--burst-seconds", type=float, default=20)
    parser.add_argument("--poll-seconds", type=float, default=2)
    args = parser.parse_args()
    if not (args.duration_seconds > 0 and args.period_seconds > 0 and args.poll_seconds > 0):
        parser.error("duration, period and poll seconds must be positive")
    if not 0 < args.burst_seconds <= args.period_seconds:
        parser.error("burst seconds must be positive and no greater than period seconds")
    return args


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    averaging_key = bool(is_valid_group(args.key))

    dht = hivemind.DHT(
        initial_peers=args.initial_peer,
        start=True,
        client_mode=True,
        cache_nearest=0,
        cache_locally=False,
        cache_refresh_before_expiry=0,
        refresh_timeout=None,
    )
    try:
        started = time.monotonic()
        deadline = started + args.duration_seconds
        burst_start = started
        with args.output.open("a", encoding="utf-8", buffering=1) as out:
            while time.monotonic() < deadline:
                burst_end = min(burst_start + args.burst_seconds, deadline)
                next_poll = burst_start
                while time.monotonic() < burst_end:
                    now = time.monotonic()
                    if now < next_poll:
                        time.sleep(max(0.0, min(next_poll - now, burst_end - now)))
                    if time.monotonic() >= burst_end:
                        break

                    request_started = time.monotonic()
                    row = {
                        "key": args.key,
                        "averaging_key": averaging_key,
                        "request_utc": utc_now(),
                        "elapsed_run_s": request_started - started,
                    }
                    try:
                        result = dht.get(args.key, latest=True)
                        received_at = time.time()
                        row.update(summarize(result, received_at, averaging_key))
                        row["status"] = "ok" if result is not None else "empty"
                    except Exception as exc:
                        row["status"] = "error"
                        row["error"] = f"{type(exc).__name__}: {exc}"
                    row["response_utc"] = utc_now()
                    row["read_s"] = time.monotonic() - request_started
                    out.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
                    next_poll += args.poll_seconds

                burst_start += args.period_seconds
                if burst_start > time.monotonic() and time.monotonic() < deadline:
                    time.sleep(max(0.0, min(burst_start - time.monotonic(), deadline - time.monotonic())))
    finally:
        dht.shutdown()


if __name__ == "__main__":
    main()
