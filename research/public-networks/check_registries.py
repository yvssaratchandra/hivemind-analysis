#!/usr/bin/env python3
"""Passive reads of documented public registries/pages; no peer connections."""
import concurrent.futures
import datetime
import json
import pathlib
import sys
import urllib.error
import urllib.request

URLS = {
    "golem_online": "https://api.stats.golem.network/v1/network/online",
    "akash_providers": "https://console-api.akash.network/v1/providers",
    "gensyn_dashboard": "https://dashboard.gensyn.ai/",
    "swarmllm_readme": "https://raw.githubusercontent.com/enapt/SwarmLLM/main/README.md",
    "chutes_metagraph": "https://api.chutes.ai/miner/metagraph",
}


def fetch(item):
    label, url = item
    record = {"label": label, "url": url,
              "checked_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        response = urllib.request.urlopen(urllib.request.Request(
            url, headers={"User-Agent": "AI-public-network-passive-check/1.0"}), timeout=20)
    except urllib.error.HTTPError as error:
        response = error
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        record["error"] = str(error)
        return record, b""
    with response:
        record.update(status=response.code, final_url=response.url,
                      headers={k: v for k, v in response.headers.items() if k.lower() != "set-cookie"})
        data = response.read(8_000_001)
        record["truncated"] = len(data) > 8_000_000
    return record, data[:8_000_000]


if __name__ == "__main__":
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True, exist_ok=False)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(URLS)) as pool:
        results = list(pool.map(fetch, URLS.items()))
    metadata = []
    for record, data in results:
        (output / (record["label"] + ".raw")).write_bytes(data)
        metadata.append(record)
        print(record["label"], record.get("status", record.get("error")), len(data))
    (output / "http_checks.json").write_text(json.dumps(metadata, indent=2) + "\n")
