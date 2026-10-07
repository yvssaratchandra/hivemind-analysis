#!/usr/bin/env python3
"""Read official public HTTP pages only; never join a P2P network."""

import concurrent.futures
import datetime
import json
import pathlib
import sys
import urllib.error
import urllib.request

URLS = (
    "https://localai.io/",
    "https://explorer.localai.io/",
    "https://explorer.localai.io/networks",
    "https://demo.localai.io/",
)


def check(url):
    result = {"url": url, "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    request = urllib.request.Request(url, headers={"User-Agent": "LocalAI-passive-directory-check/1.0"})
    try:
        response = urllib.request.urlopen(request, timeout=20)
    except urllib.error.HTTPError as error:
        response = error
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        result["error"] = str(error)
        return result
    with response:
        result.update(status=response.code, final_url=response.url,
                      headers={key: value for key, value in response.headers.items()
                               if key.lower() != "set-cookie"})
        body = response.read(65537)
        result["body_truncated"] = len(body) > 65536
        if url.endswith("/networks") and response.code == 200:
            # A live directory includes network tokens; save counts, not credentials.
            try:
                networks = json.loads(body)
                if not isinstance(networks, list):
                    raise ValueError("Directory is not a JSON list")
                result["directory_summary"] = {
                    "listed_networks": len(networks),
                    "clusters": sum(len(n.get("Clusters", [])) for n in networks),
                    "worker_entries": sum(len(c.get("Workers", []))
                                          for n in networks for c in n.get("Clusters", [])),
                }
                result["note"] = "Cached advertised worker entries; not a unique-peer census. Tokens omitted."
            except (ValueError, TypeError, AttributeError) as error:
                result["directory_parse_error"] = str(error)
        else:
            result["body_excerpt"] = body[:2000].decode("utf-8", errors="replace")
    return result


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 check_public_directory.py NEW_OUTPUT_DIRECTORY")
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True, exist_ok=False)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(URLS)) as pool:
        results = list(pool.map(check, URLS))
    (output / "http_checks.json").write_text(json.dumps(results, indent=2) + "\n")
    for result in results:
        print(result["url"], result.get("status", result.get("error")), result.get("directory_summary", ""))
