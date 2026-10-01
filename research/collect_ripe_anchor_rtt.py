"""Collect a small, reproducible worldwide RTT matrix from RIPE Atlas.

Uses public IPv4 ICMP ping results from the active RIPE Atlas anchor mesh. Each
selected anchor is a real endpoint, and each directed edge keeps its own three
most recent reply RTTs. This is a convenience sample, not a population estimate
for all Internet hosts or for Hivemind TCP/libp2p traffic.

Run: .venv/bin/python research/collect_ripe_anchor_rtt.py
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

API = "https://atlas.ripe.net/api/v2"
RESULTS = Path(__file__).resolve().parent / "results"
COUNTRIES = ("SE", "US", "BR", "JP", "IN", "ZA", "AU")
PREFERRED_CITIES = {
    "SE": ("Stockholm", "Malmö"),
    "US": ("New York", "Ashburn"),
    "BR": ("São Paulo", "Sao Paulo"),
    "JP": ("Tokyo",),
    "IN": ("Mumbai",),
    "ZA": ("Johannesburg",),
    "AU": ("Sydney",),
}


def fetch_json(url: str) -> object:
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "hivemind-research-rtt/1.0"})
            with urllib.request.urlopen(request, timeout=25) as response:
                return json.load(response)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(0.5 * 2**attempt)
    raise RuntimeError(f"RIPE Atlas request failed: {url}: {last_error}")


def list_pages(url: str, page_size: int = 500) -> list[dict]:
    separator = "&" if "?" in url else "?"
    first = fetch_json(f"{url}{separator}page_size={page_size}&is_active=true&page=1")
    assert isinstance(first, dict)
    count = first["count"]
    pages = (count + min(page_size, 500) - 1) // min(page_size, 500)
    collected = list(first["results"])
    if pages > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            futures = {
                pool.submit(fetch_json, f"{url}{separator}page_size={page_size}&is_active=true&page={page}"): page
                for page in range(2, pages + 1)
            }
            for future in concurrent.futures.as_completed(futures):
                response = future.result()
                assert isinstance(response, dict)
                collected.extend(response["results"])
    return collected


def sort_candidates(items: list[dict], country: str) -> list[dict]:
    preferred = PREFERRED_CITIES[country]

    def order(anchor: dict) -> tuple[int, str, int]:
        city = (anchor.get("city") or "").casefold()
        rank = next((index for index, name in enumerate(preferred) if name.casefold() in city), len(preferred))
        return rank, city, anchor["id"]

    return sorted(
        (
            item
            for item in items
            if item.get("country") == country
            and item.get("probe")
            and item.get("ip_v4")
            and not item.get("is_disabled")
            and not item.get("date_decommissioned")
        ),
        key=order,
    )


def parse_ping_results(rows: object) -> dict[int, dict]:
    result = {}
    if not isinstance(rows, list):
        return result
    for row in rows:
        if row.get("af") != 4 or row.get("type") != "ping":
            continue
        samples = [reply["rtt"] for reply in row.get("result", []) if isinstance(reply, dict) and "rtt" in reply]
        if not samples:
            continue
        result[row["prb_id"]] = {"timestamp": row["timestamp"], "rtt_ms": samples}
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y%m%d"))
    args = parser.parse_args()

    print("loading active RIPE Atlas anchor measurements", flush=True)
    measurements = list_pages(f"{API}/anchor-measurements/")
    by_target: dict[int, list[dict]] = {}
    for item in measurements:
        if item.get("is_mesh") and item.get("type") == "ping":
            target = int(item["target"].rstrip("/").split("/")[-1])
            by_target.setdefault(target, []).append(item)

    anchors_by_country = {}
    for country in COUNTRIES:
        anchors = list_pages(f"{API}/anchors/?country={country}")
        anchors_by_country[country] = sort_candidates(anchors, country)

    selected = []
    for country in COUNTRIES:
        choice = None
        for anchor in anchors_by_country[country][:12]:
            options = by_target.get(anchor["id"], [])
            for option in options:
                measurement_id = int(option["measurement"].rstrip("/").split("/")[-1])
                latest_url = f"{API}/measurements/{measurement_id}/latest/"
                replies = parse_ping_results(fetch_json(latest_url))
                if len(replies) >= 4:
                    choice = {
                        "country": country,
                        "city": anchor["city"],
                        "anchor_id": anchor["id"],
                        "probe_id": anchor["probe"],
                        "measurement_id": measurement_id,
                        "measurement_url": latest_url,
                        "replies": replies,
                    }
                    break
            if choice is not None:
                break
        if choice is None:
            print(f"{country}: no usable active IPv4 mesh ping target", flush=True)
            continue
        print(
            f"{country}: {choice['city']} probe={choice['probe_id']} "
            f"measurement={choice['measurement_id']} sources={len(choice['replies'])}",
            flush=True,
        )
        selected.append(choice)

    if len(selected) < 5:
        raise RuntimeError(f"only {len(selected)} usable countries, need at least five")

    edges = []
    same_region = []
    for destination in selected:
        local_candidates = [
            candidate
            for candidate in anchors_by_country[destination["country"]]
            if candidate["probe"] != destination["probe_id"]
            and candidate["probe"] in destination["replies"]
        ]
        local_candidates.sort(
            key=lambda candidate: (
                candidate.get("city") == destination["city"], candidate["id"]
            )
        )
        if local_candidates:
            source = local_candidates[0]
            reply = destination["replies"][source["probe"]]
            same_region.append(
                {
                    "country": destination["country"],
                    "source_city": source["city"],
                    "destination_city": destination["city"],
                    "source_probe_id": source["probe"],
                    "destination_probe_id": destination["probe_id"],
                    "timestamp": reply["timestamp"],
                    "rtt_ms": reply["rtt_ms"],
                }
            )
        for source in selected:
            if source["probe_id"] == destination["probe_id"]:
                continue
            reply = destination["replies"].get(source["probe_id"])
            if reply is None:
                continue
            edges.append(
                {
                    "source": source["country"],
                    "destination": destination["country"],
                    "source_probe_id": source["probe_id"],
                    "destination_probe_id": destination["probe_id"],
                    "timestamp": reply["timestamp"],
                    "rtt_ms": reply["rtt_ms"],
                }
            )
    labels = [item["country"] for item in selected]
    missing = [(a, b) for a in labels for b in labels if a != b and not any(
        edge["source"] == a and edge["destination"] == b for edge in edges
    )]
    if missing:
        print(f"missing {len(missing)} directed country pairs: {missing}", flush=True)
    medians = [statistics.median(edge["rtt_ms"]) for edge in edges]
    print(f"collected {len(edges)}/{len(labels) * (len(labels) - 1)} directed pairs", flush=True)
    print(
        f"RTT median across pairs={statistics.median(medians):.1f} ms, "
        f"min={min(medians):.1f} ms, max={max(medians):.1f} ms",
        flush=True,
    )

    output = {
        "source": "RIPE Atlas active anchor-mesh IPv4 ICMP ping latest results",
        "api_catalog": f"{API}/anchor-measurements/",
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "caveat": "Seven selected anchors are a convenience sample; ICMP RTT is a proxy for libp2p RTT, not a WAN guarantee.",
        "anchors": [
            {key: item[key] for key in ("country", "city", "anchor_id", "probe_id", "measurement_id", "measurement_url")}
            for item in selected
        ],
        "edges": edges,
        "same_region": same_region,
        "missing_pairs": missing,
    }
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"ripe_anchor_rtt_{args.date}.json"
    with path.open("w") as handle:
        json.dump(output, handle, indent=2)
        handle.write("\n")
    print(f"wrote {path}", flush=True)


if __name__ == "__main__":
    main()
