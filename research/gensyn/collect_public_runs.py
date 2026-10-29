#!/usr/bin/env python3
"""Collect public Gensyn RL Swarm dashboard and model-card metadata.

This collector uses only unauthenticated GETs linked from the public dashboard
or the public Hugging Face API. It does not query a blockchain, log in, or
download model weights. Raw response bodies are immutable within a snapshot;
the manifest records each requested URL and response digest.
"""

from __future__ import annotations

import argparse
import csv
import email.utils
import hashlib
import html.parser
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

USER_AGENT = "hivemind-analysis-public-research/1.0"
DEFAULT_MAX_BYTES = 4 * 1024**3
MAX_RETRIES = 2
MIN_HOST_INTERVAL_SECONDS = 1.05

DASHBOARDS = (
    "https://dashboard.gensyn.ai/",
    "https://dashboard-math.gensyn.ai/",
    "https://swarm.gensyn.ai/",
)
MATH_PUBLIC_API_PATHS = (
    "nodes-connected",
    "round-stage",
    "top-rewards",
    "leaderboard",
    "unique-voters",
    "gossip-messages",
)
OFFICIAL_CONTEXT_URLS = (
    "https://docs.gensyn.ai/testnet/rl-swarm",
    "https://docs.gensyn.ai/testnet/rl-swarm/how-it-works",
    "https://docs.gensyn.ai/testnet/rl-swarm/how-it-works/legacy-environments",
    "https://docs.gensyn.ai/testnet/rl-swarm/how-it-works/codezero",
    "https://www.gensyn.ai/news/rl-swarm-a-framework-for-collaborative-rl",
    "https://www.gensyn.ai/news/codezero-extending-rl-swarm-toward-cooperative-coding-agents",
)
HF_COLLECTION_API = "https://huggingface.co/api/collections/Gensyn/rl-swarm"
HF_MODEL_SEARCH = "https://huggingface.co/api/models?search=Gensyn-Swarm&limit=1000" + "".join(
    "&expand=" + urllib.parse.quote(field)
    for field in ("cardData", "createdAt", "lastModified", "downloads", "likes", "tags", "author", "pipeline_tag", "sha")
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("._-")
    return (slug[:90] or "response")


class LinkedScripts(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.scripts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "script":
            return
        src = dict(attrs).get("src")
        if src:
            self.scripts.append(src)


def response_name(url: str, content_type: str | None = None) -> str:
    parsed = urllib.parse.urlparse(url)
    tail = Path(urllib.parse.unquote(parsed.path)).name
    if not tail or "." not in tail:
        if content_type and "json" in content_type:
            tail = "response.json"
        elif content_type and "javascript" in content_type:
            tail = "response.js"
        else:
            tail = "response.html"
    stem, suffix = os.path.splitext(tail)
    identity = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]
    return f"{safe_slug(stem)}_{identity}{suffix or '.bin'}"


class Collector:
    def __init__(self, output: Path, max_bytes: int, resume: bool) -> None:
        self.output = output
        self.raw = output / "raw"
        self.manifest_path = output / "manifest.json"
        self.max_bytes = max_bytes
        self.last_request_by_host: dict[str, float] = {}
        self.bytes_saved = 0
        if resume:
            if not self.manifest_path.exists():
                raise ValueError(f"--resume requires an existing {self.manifest_path}")
            self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            self.validate_saved_responses()
            counted_paths: set[str] = set()
            for entry in self.manifest.get("entries", []):
                relative = entry.get("path")
                if relative and relative not in counted_paths:
                    counted_paths.add(relative)
                    self.bytes_saved += int(entry.get("bytes", 0) or 0)
        else:
            if self.output.exists() and any(self.output.iterdir()):
                raise ValueError(f"Output already exists and is not empty: {self.output}; use --resume or choose a new snapshot")
            self.output.mkdir(parents=True, exist_ok=True)
            self.raw.mkdir(parents=True, exist_ok=True)
            self.manifest = {
                "schema_version": 1,
                "project": "gensyn-rl-swarm",
                "started_utc": utc_now(),
                "user_agent": USER_AGENT,
                "max_bytes": max_bytes,
                "entries": [],
            }
            self.save_manifest()
        self.raw.mkdir(parents=True, exist_ok=True)

    def save_manifest(self) -> None:
        temp = self.manifest_path.with_suffix(".json.tmp")
        temp.write_text(json.dumps(self.manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temp, self.manifest_path)

    def _existing_success(self, url: str) -> dict[str, Any] | None:
        for entry in reversed(self.manifest["entries"]):
            if entry.get("url") == url and entry.get("status") == 200:
                relative = entry.get("path")
                if not relative:
                    raise ValueError(f"Successful response has no raw path; refusing to refetch: {url}")
                path = self.output / relative
                if not path.is_file():
                    raise ValueError(f"Successful response body is missing; refusing to refetch: {path}")
                if path.stat().st_size != int(entry.get("bytes", -1)) or self._hash_file(path) != entry.get("sha256"):
                    raise ValueError(f"Successful response body is corrupt; refusing to refetch: {path}")
                return entry
        return None

    def _hash_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def validate_saved_responses(self) -> None:
        for entry in self.manifest.get("entries", []):
            relative = entry.get("path")
            if not relative:
                if entry.get("status") == 200:
                    raise ValueError(f"Successful response has no saved path: {entry.get('url')}")
                continue
            path = self.output / relative
            if not path.is_file():
                raise ValueError(f"Saved public response is missing; refusing to replace evidence: {path}")
            if path.stat().st_size != int(entry.get("bytes", -1)):
                raise ValueError(f"Saved public response size changed; refusing to replace evidence: {path}")
            if self._hash_file(path) != entry.get("sha256"):
                raise ValueError(f"Saved public response hash changed; refusing to replace evidence: {path}")

    def get(self, url: str, discovered_from: str | None = None) -> tuple[bytes | None, dict[str, Any]]:
        saved = self._existing_success(url)
        if saved:
            return (self.output / saved["path"]).read_bytes(), saved
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc.lower()
        entry: dict[str, Any] = {
            "url": url,
            "path": None,
            "status": None,
            "sha256": None,
            "bytes": 0,
            "retrieved_at": utc_now(),
        }
        if discovered_from:
            entry["discovered_from"] = discovered_from
        for attempt in range(MAX_RETRIES + 1):
            delay = MIN_HOST_INTERVAL_SECONDS - (time.monotonic() - self.last_request_by_host.get(host, 0.0))
            if delay > 0:
                time.sleep(delay)
            self.last_request_by_host[host] = time.monotonic()
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json,text/html,application/javascript,*/*;q=0.5"})
            try:
                opener = urllib.request.build_opener(NoRedirectHandler()) if host == "swarm.gensyn.ai" else None
                open_response = opener.open if opener else urllib.request.urlopen
                with open_response(request, timeout=30) as response:
                    status = response.status
                    entry["status"] = status
                    entry["content_type"] = response.headers.get("Content-Type")
                    entry["content_length"] = response.headers.get("Content-Length")
                    entry["etag"] = response.headers.get("ETag")
                    entry["last_modified"] = response.headers.get("Last-Modified")
                    entry["link"] = response.headers.get("Link")
                    entry["x_total_count"] = response.headers.get("X-Total-Count")
                    entry["final_url"] = response.geturl()
                    remaining = self.max_bytes - self.bytes_saved
                    advertised_length = response.headers.get("Content-Length")
                    if advertised_length and int(advertised_length) > remaining:
                        entry["status"] = "budget_exceeded"
                        entry["error"] = f"Content-Length {advertised_length} exceeds remaining budget {remaining}"
                        self.manifest["entries"].append(entry)
                        self.save_manifest()
                        return None, entry
                    chunks: list[bytes] = []
                    received = 0
                    while True:
                        chunk = response.read(min(1024 * 1024, max(1, remaining - received + 1)))
                        if not chunk:
                            break
                        received += len(chunk)
                        if received > remaining:
                            entry["status"] = "budget_exceeded"
                            entry["bytes_observed"] = received
                            entry["bytes"] = 0
                            entry["error"] = "Response body exceeded remaining collection budget"
                            self.manifest["entries"].append(entry)
                            self.save_manifest()
                            return None, entry
                        chunks.append(chunk)
                    body = b"".join(chunks)
                    entry["bytes"] = len(body)
                    entry["sha256"] = hashlib.sha256(body).hexdigest()
                    name = response_name(url, entry.get("content_type"))
                    relative = Path("raw") / name
                    target = self.output / relative
                    temp = self.raw / f".{name}.tmp-{os.getpid()}-{time.time_ns()}"
                    with temp.open("xb") as handle:
                        handle.write(body)
                        handle.flush()
                        os.fsync(handle.fileno())
                    try:
                        # Hard-link creation is atomic and fails if target already exists.
                        os.link(temp, target)
                    except FileExistsError as exc:
                        raise ValueError(f"Refusing to overwrite immutable raw response: {target}") from exc
                    finally:
                        temp.unlink(missing_ok=True)
                    entry["path"] = str(relative)
                    self.bytes_saved += len(body)
                    self.manifest["entries"].append(entry)
                    self.save_manifest()
                    return body, entry
            except urllib.error.HTTPError as exc:
                entry["status"] = exc.code
                entry["content_type"] = exc.headers.get("Content-Type") if exc.headers else None
                entry["location"] = exc.headers.get("Location") if exc.headers else None
                entry["error"] = f"HTTP {exc.code}: {exc.reason}"
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                if retry_after:
                    try:
                        wait_seconds = max(0.0, float(retry_after))
                    except ValueError:
                        try:
                            retry_time = email.utils.parsedate_to_datetime(retry_after)
                            if retry_time.tzinfo is None:
                                retry_time = retry_time.replace(tzinfo=timezone.utc)
                            wait_seconds = max(0.0, (retry_time - datetime.now(timezone.utc)).total_seconds())
                        except (TypeError, ValueError, OverflowError):
                            wait_seconds = 0.0
                    if wait_seconds > 60:
                        entry["status"] = "deferred"
                        entry["retry_after_seconds"] = wait_seconds
                        entry["error"] = f"HTTP {exc.code}; Retry-After exceeds 60-second bounded wait"
                        self.manifest["entries"].append(entry)
                        self.save_manifest()
                        return None, entry
                else:
                    wait_seconds = 2 ** attempt
                if exc.code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                    time.sleep(max(MIN_HOST_INTERVAL_SECONDS, wait_seconds))
                    continue
                # Store a small public error response as evidence when permitted.
                try:
                    error_body = exc.read(4096)
                except Exception:
                    error_body = b""
                if error_body and self.bytes_saved + len(error_body) <= self.max_bytes:
                    name = response_name(url, entry.get("content_type"))
                    relative = Path("raw") / name
                    target = self.output / relative
                    temp = self.raw / f".{name}.tmp-{os.getpid()}-{time.time_ns()}"
                    with temp.open("xb") as handle:
                        handle.write(error_body)
                        handle.flush()
                        os.fsync(handle.fileno())
                    try:
                        os.link(temp, target)
                    except FileExistsError:
                        entry["error_body_saved"] = False
                    else:
                        entry["path"] = str(relative)
                        entry["bytes"] = len(error_body)
                        entry["sha256"] = hashlib.sha256(error_body).hexdigest()
                        entry["error_body_saved"] = True
                        self.bytes_saved += len(error_body)
                    finally:
                        temp.unlink(missing_ok=True)
                self.manifest["entries"].append(entry)
                self.save_manifest()
                return None, entry
            except Exception as exc:  # keep a durable coverage record on timeouts/DNS errors
                entry["status"] = "request_error"
                entry["error"] = f"{type(exc).__name__}: {exc}"
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                    continue
                self.manifest["entries"].append(entry)
                self.save_manifest()
                return None, entry


def discover_dashboard_scripts(collector: Collector) -> list[str]:
    scripts: list[tuple[str, str]] = []
    for url in DASHBOARDS:
        body, entry = collector.get(url, discovered_from="official dashboard URL inventory")
        if body is None or entry.get("status") != 200:
            continue
        parser = LinkedScripts()
        parser.feed(body.decode("utf-8", "replace"))
        for src in parser.scripts:
            absolute = urllib.parse.urljoin(url, src)
            # Follow scripts linked by the dashboard itself; skip unrelated analytics beacons.
            if urllib.parse.urlparse(absolute).hostname == urllib.parse.urlparse(url).hostname:
                scripts.append((absolute, url))
    return list(dict.fromkeys(scripts))


def collect(collector: Collector, discovery_only: bool = False) -> None:
    scripts = discover_dashboard_scripts(collector)
    for url, root_url in scripts:
        collector.get(url, discovered_from=root_url)
    if discovery_only:
        collector.manifest["completed_utc"] = utc_now()
        collector.manifest["discovery_only"] = True
        collector.save_manifest()
        return

    for url in OFFICIAL_CONTEXT_URLS:
        collector.get(url, discovered_from="official Gensyn RL Swarm documentation or news page")

    # These unauthenticated GET routes are explicitly called by the public
    # historical math dashboard's own linked JavaScript bundle.
    # The dashboard bundles on both hosts use the same `/api/v1` route names.
    # Only unauthenticated read routes rendered by the public frontend are used.
    for host in ("https://dashboard.gensyn.ai", "https://dashboard-math.gensyn.ai"):
        api_root = host + "/api/v1/"
        for endpoint in MATH_PUBLIC_API_PATHS:
            collector.get(urllib.parse.urljoin(api_root, endpoint), discovered_from=host + "/assets dashboard bundle")

    # Public Hugging Face metadata only: collection members and repository
    # metadata for names matching the swarm model naming convention. No files
    # from model repositories are requested.
    collection_body, collection_entry = collector.get(
        HF_COLLECTION_API,
        discovered_from="public Gensyn RL Swarm collection on Hugging Face",
    )
    if collection_body is not None and collection_entry.get("status") == 200:
        try:
            collection = json.loads(collection_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            collection = {}
        for item in collection.get("items", []) if isinstance(collection, dict) else []:
            model_id = item.get("id") if isinstance(item, dict) else None
            if not model_id or (item.get("type") not in (None, "model")):
                continue
            model_url = (
                "https://huggingface.co/api/models/"
                + urllib.parse.quote(str(model_id), safe="/")
                + "?expand=cardData"
            )
            collector.get(
                model_url,
                discovered_from=f"official curated collection item: {model_id}",
            )
            model_card_url = (
                "https://huggingface.co/"
                + urllib.parse.quote(str(model_id), safe="/")
                + "/raw/main/README.md"
            )
            collector.get(
                model_card_url,
                discovered_from=f"official curated collection model card: {model_id}",
            )
    next_url: str | None = HF_MODEL_SEARCH
    seen_urls: set[str] = set()
    page_number = 0
    while next_url and next_url not in seen_urls and page_number < 1000:
        seen_urls.add(next_url)
        page, entry = collector.get(next_url, discovered_from="public Hugging Face model search; next-page URL from Link header")
        page_number += 1
        link = entry.get("link")
        match = re.search(r'<([^>]+)>\s*;\s*rel="next"', link or "", re.IGNORECASE)
        next_url = urllib.parse.urljoin(next_url, match.group(1)) if match else None
        if page is None:
            break
    if next_url and page_number >= 1000:
        collector.manifest["coverage_warning"] = "Model search pagination stopped at the 1000-page safety limit."
    collector.manifest["completed_utc"] = utc_now()
    collector.manifest["discovery_only"] = False
    collector.save_manifest()


def read_json_entry(output: Path, entry: dict[str, Any]) -> Any | None:
    if entry.get("status") != 200 or not entry.get("path"):
        return None
    path = output / entry["path"]
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def list_records(payload: Any, preferred: Iterable[str]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in preferred:
        candidate = payload.get(key)
        if isinstance(candidate, list):
            return [x for x in candidate if isinstance(x, dict)]
    for value in payload.values():
        if isinstance(value, (dict, list)):
            found = list_records(value, preferred)
            if found:
                return found
    return []


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(row.get(key), ensure_ascii=False) if isinstance(row.get(key), (dict, list)) else row.get(key) for key in fields})


def analyze(output: Path) -> dict[str, Any]:
    manifest_path = output / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError(f"No manifest found at {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    all_entries = manifest.get("entries", [])
    validate_manifest_integrity(output, all_entries)
    # A resumed snapshot can contain an earlier failed attempt followed by a
    # successful retry. Analyze the newest success for each URL; when every
    # attempt failed, retain the newest failure as a coverage record.
    latest_by_url: dict[str, dict[str, Any]] = {}
    for entry in all_entries:
        url = entry.get("url", "")
        previous = latest_by_url.get(url)
        if previous is None or entry.get("status") == 200 or previous.get("status") != 200:
            latest_by_url[url] = entry
    entries = list(latest_by_url.values())
    normalized = output / "normalized"
    normalized.mkdir(parents=True, exist_ok=True)
    leaderboard_rows: list[dict[str, Any]] = []
    top_reward_rows: list[dict[str, Any]] = []
    model_rows: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    gossip_rows: list[dict[str, Any]] = []

    for entry in entries:
        if entry.get("status") != 200:
            continue
        url = entry.get("url", "")
        payload = read_json_entry(output, entry)
        if payload is None:
            continue
        if "/api/v1/leaderboard" in url:
            records = list_records(payload, ("peers", "entries", "leaderboard", "nodes", "results"))
            for position, record in enumerate(records, start=1):
                leaderboard_rows.append({
                    "source_url": url,
                    "evidence_path": entry.get("path"),
                    "peer_id": record.get("peerId", record.get("id", record.get("peer_id"))),
                    "peer_name": record.get("peerName", record.get("name")),
                    "eoa": record.get("eoa"),
                    "rank_in_response": record.get("rank", position),
                    "score_raw": record.get("score"),
                    "participation_score": record.get("score"),
                    "training_reward": record.get("reward", record.get("rewards")),
                    "online": record.get("online"),
                    "last_update": record.get("lastUpdate", record.get("last_update")),
                    "raw_record": record,
                })
        elif "/api/v1/top-rewards" in url:
            records = list_records(payload, ("entries", "peers", "results"))
            for position, record in enumerate(records, start=1):
                top_reward_rows.append({
                    "source_url": url,
                    "evidence_path": entry.get("path"),
                    "rank_in_response": record.get("rank", position),
                    "peer_id": record.get("id", record.get("peerId")),
                    "peer_name": record.get("name", record.get("peerName")),
                    "reward": record.get("reward"),
                    "raw_record": record,
                })
        elif "/api/models?" in url:
            models = [x for x in payload if isinstance(x, dict)] if isinstance(payload, list) else list_records(payload, ("items", "models", "results"))
            for model in models:
                model_rows.append({
                    "source_url": url,
                    "evidence_path": entry.get("path"),
                    "model_id": model.get("id", model.get("modelId")),
                    "model_id_raw": model.get("modelId"),
                    "evidence_class": "name_search_candidate",
                    "author": model.get("author"),
                    "pipeline_tag": model.get("pipeline_tag"),
                    "last_modified": model.get("lastModified"),
                    "created_at": model.get("createdAt"),
                    "downloads": model.get("downloads"),
                    "likes": model.get("likes"),
                    "trending_score": model.get("trendingScore"),
                    "revision_sha": model.get("sha"),
                    "tags": model.get("tags"),
                    "card_data": model.get("cardData"),
                    "parameter_count": model.get("numParameters"),
                })
        elif "/api/models/" in url:
            if isinstance(payload, dict):
                model = payload
                model_rows.append({
                    "source_url": url,
                    "evidence_path": entry.get("path"),
                    "model_id": model.get("id"),
                    "model_id_raw": model.get("modelId"),
                    "evidence_class": "official_curated_collection_model_card_metadata",
                    "author": model.get("author"),
                    "pipeline_tag": model.get("pipeline_tag"),
                    "last_modified": model.get("lastModified"),
                    "created_at": model.get("createdAt"),
                    "downloads": model.get("downloads"),
                    "likes": model.get("likes"),
                    "trending_score": model.get("trendingScore"),
                    "revision_sha": model.get("sha"),
                    "parameter_count": model.get("numParameters"),
                    "tags": model.get("tags"),
                    "card_data": model.get("cardData"),
                })
        elif "api/collections/" in url:
            models = list_records(payload, ("items", "models"))
            for model in models:
                model_rows.append({
                    "source_url": url,
                    "evidence_path": entry.get("path"),
                    "model_id": model.get("id", model.get("repoId")),
                    "model_id_raw": model.get("repoId"),
                    "evidence_class": "official_curated_collection_member",
                    "author": model.get("author"),
                    "pipeline_tag": model.get("pipeline_tag"),
                    "last_modified": model.get("lastModified"),
                    "created_at": model.get("createdAt"),
                    "downloads": model.get("downloads"),
                    "likes": model.get("likes"),
                    "trending_score": model.get("trendingScore"),
                    "revision_sha": model.get("sha"),
                    "tags": model.get("tags"),
                    "card_data": model.get("cardData"),
                    "parameter_count": model.get("numParameters"),
                })
        elif "/api/v1/gossip-messages" in url:
            records = list_records(payload, ("messages", "gossip", "items"))
            for record in records:
                gossip_rows.append({"source_url": url, "evidence_path": entry.get("path"), **record})
        elif "/api/v1/" in url and isinstance(payload, (dict, list, int, float, str)):
            # Keep API summaries as individual scalar observations. This avoids
            # assuming that counters across different eras share one meaning.
            def visit(value: Any, pointer: str = "") -> None:
                if isinstance(value, dict):
                    for key, child in value.items():
                        visit(child, f"{pointer}/{key}")
                elif isinstance(value, list):
                    for index, child in enumerate(value):
                        visit(child, f"{pointer}/{index}")
                elif value is None or isinstance(value, (str, int, float, bool)):
                    metric_rows.append({"source_url": url, "evidence_path": entry.get("path"), "json_pointer": pointer or "/", "value": value})
            visit(payload)

    write_csv(normalized / "nodes.csv", leaderboard_rows,
              ["source_url", "evidence_path", "peer_id", "peer_name", "eoa", "rank_in_response", "score_raw", "participation_score", "training_reward", "online", "last_update", "raw_record"])
    write_csv(normalized / "top_rewards.csv", top_reward_rows,
              ["source_url", "evidence_path", "rank_in_response", "peer_id", "peer_name", "reward", "raw_record"])
    write_csv(normalized / "models.csv", model_rows,
              ["source_url", "evidence_path", "model_id", "model_id_raw", "evidence_class", "author", "pipeline_tag", "last_modified", "created_at", "downloads", "likes", "trending_score", "revision_sha", "parameter_count", "tags", "card_data"])
    write_csv(normalized / "metrics.csv", metric_rows,
              ["source_url", "evidence_path", "json_pointer", "value"])
    write_csv(normalized / "gossip_messages.csv", gossip_rows,
              ["source_url", "evidence_path", "id", "peerId", "peer_id", "name", "round", "stage", "message", "content", "timestamp"])

    counts: dict[str, int] = {}
    for entry in all_entries:
        host = urllib.parse.urlparse(entry.get("url", "")).netloc
        counts[host] = counts.get(host, 0) + 1
    status_counts: dict[str, int] = {}
    for entry in all_entries:
        key = str(entry.get("status"))
        status_counts[key] = status_counts.get(key, 0) + 1
    model_evidence_counts: dict[str, int] = {}
    for row in model_rows:
        key = row["evidence_class"]
        model_evidence_counts[key] = model_evidence_counts.get(key, 0) + 1
    summary = {
        "project": "gensyn-rl-swarm",
        "analyzed_utc": utc_now(),
        "source_snapshot_started_utc": manifest.get("started_utc"),
        "requested_responses": len(all_entries),
        "response_status_counts": status_counts,
        "requests_by_host": counts,
        "nodes_normalized": len(leaderboard_rows),
        "top_reward_rows": len(top_reward_rows),
        "model_metadata_records": len(model_rows),
        "model_metadata_records_by_evidence_class": model_evidence_counts,
        "gossip_messages": len(gossip_rows),
        "dashboard_metric_scalars": len(metric_rows),
        "total_bytes": sum(int(entry.get("bytes", 0) or 0) for entry in all_entries),
        "notes": [
            "The linked dashboard frontend labels the raw leaderboard score field as participation; reward and participation are leaderboard measures, not measured GPU time or per-node compute share.",
            "The unique-voters endpoint is a separate aggregate counter, not the number of rows returned by the leaderboard endpoint.",
            "Hugging Face name-search results are candidate self-published model repositories, not verified participants in a particular public run.",
            "Official curated collection membership and model-card metadata show project-associated repositories; neither is linked to a particular dashboard peer or run.",
            "No blockchain reconstruction was performed. The CodeZero repository config sets log_with=wandb, but no official public global run project or peer-specific W&B link was found in consulted sources.",
            "In this snapshot, the identified public dashboards, official documentation, repository, and technical report exposed no raw per-node train-step histories; this is a discovery limitation, not an exclusion from the research question.",
        ],
    }
    (normalized / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_inventory(output, summary)
    return summary


def validate_manifest_integrity(output: Path, entries: list[dict[str, Any]]) -> None:
    for entry in entries:
        relative = entry.get("path")
        if not relative:
            if entry.get("status") == 200:
                raise ValueError(f"Successful response has no raw path: {entry.get('url')}")
            continue
        path = output / relative
        if not path.is_file():
            raise ValueError(f"Manifest response file is missing: {path}")
        digest = hashlib.sha256()
        count = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                count += len(chunk)
                digest.update(chunk)
        if count != int(entry.get("bytes", -1)) or digest.hexdigest() != entry.get("sha256"):
            raise ValueError(f"Manifest response integrity check failed: {path}")


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Record the public legacy host's direct response without following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def write_inventory(output: Path, summary: dict[str, Any]) -> None:
    project_dir = Path(__file__).resolve().parent
    manifest_path = output / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    entries = manifest.get("entries", [])
    root = project_dir.parent.parent.resolve()

    def display_path(path: Path) -> str:
        resolved = path.resolve()
        try:
            return str(resolved.relative_to(root))
        except ValueError:
            return str(resolved)

    earlier_snapshots = []
    for previous_path in sorted((project_dir / "data").glob("public_runs_*")):
        if not previous_path.is_dir() or previous_path.resolve() == output.resolve():
            continue
        previous_manifest = previous_path / "manifest.json"
        if not previous_manifest.is_file():
            continue
        previous_bytes = previous_manifest.read_bytes()
        previous_data = json.loads(previous_bytes)
        previous_entries = previous_data.get("entries", [])
        previous_statuses: dict[str, int] = {}
        for previous_entry in previous_entries:
            key = str(previous_entry.get("status"))
            previous_statuses[key] = previous_statuses.get(key, 0) + 1
        earlier_snapshots.append({
            "path": display_path(previous_path),
            "manifest_path": display_path(previous_manifest),
            "manifest_sha256": hashlib.sha256(previous_bytes).hexdigest(),
            "entry_count": len(previous_entries),
            "stored_bytes": sum(int(item.get("bytes", 0) or 0) for item in previous_entries),
            "status_counts": previous_statuses,
            "disposition": "superseded attempt retained for provenance; not used as the latest normalized snapshot",
        })

    inventory = {
        "schema_version": 1,
        "project": "gensyn-rl-swarm",
        "generated_utc": utc_now(),
        "latest_snapshot": display_path(output),
        "manifest_path": display_path(manifest_path),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "discovery_log": display_path(project_dir / "DISCOVERY_LOG.md"),
        "total_bytes": summary["total_bytes"],
        "summary": summary,
        "source_checkout": json.loads((project_dir / "SOURCE.json").read_text(encoding="utf-8")),
        "earlier_snapshots": earlier_snapshots,
        "raw_files": [
            {
                "url": entry.get("url"),
                "path": display_path(output / entry["path"]) if entry.get("path") else None,
                "status": entry.get("status"),
                "sha256": entry.get("sha256"),
                "bytes": entry.get("bytes", 0),
                "retrieved_at": entry.get("retrieved_at"),
                "discovered_from": entry.get("discovered_from"),
            }
            for entry in entries
        ],
        "limitations": [
            "The dashboard leaderboard endpoint returns a finite ranked entries list (100 entries in the observed math snapshot); it exposes no pagination control in the linked client.",
            "The top-rewards endpoint returns a current top-ten summary, not a historical learning curve.",
            "The RL Swarm docs page captured in this snapshot stated that no official swarms were running at retrieval time, while CodeZero-specific pages described CodeZero as active; these statements are preserved as dated evidence and were not reconciled into a claim of live training.",
            "The repository's technical report contains an older small-scale experiment and plotted relative-regret/answer-length results; it does not expose raw per-node training logs in this snapshot.",
            "Dashboard score/reward values do not measure GPU-hours or a normalized compute contribution.",
            "The unique-voters aggregate is not a count of leaderboard entries and must not be used as leaderboard coverage.",
            "The CodeZero config sets log_with=wandb, but the official repository and pages consulted do not link a public global run project or peer-specific W&B run.",
            "In this snapshot, the identified public dashboard APIs, linked official documentation, repository, and paper exposed no raw per-node train-step histories or downloadable training traces. This is an access/discovery limitation, not an exclusion from the research question.",
            "No Internet Archive response is archived for this snapshot; archive availability was not verified. See DISCOVERY_LOG.md.",
            "On-chain history reconstruction was out of scope.",
        ],
    }
    (project_dir / "INVENTORY.json").write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="Snapshot directory (raw/ and manifest.json are created here)")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES, help="Maximum raw response bytes saved in this snapshot (default: 4 GiB)")
    parser.add_argument("--resume", action="store_true", help="Resume into an existing snapshot, skipping verified successful URLs")
    parser.add_argument("--analyze-only", action="store_true", help="Regenerate normalized CSV/summary outputs offline from the saved manifest")
    parser.add_argument("--discovery-only", action="store_true", help="Save dashboard roots and only their same-host linked scripts")
    args = parser.parse_args(argv)
    if args.max_bytes < 1:
        parser.error("--max-bytes must be positive")
    try:
        if args.analyze_only:
            summary = analyze(args.output)
        else:
            collector = Collector(args.output, args.max_bytes, args.resume)
            collect(collector, args.discovery_only)
            summary = analyze(args.output)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
