#!/usr/bin/env python3
"""Collect public DSTRBTD dashboard and W&B histories without project dependencies."""

from __future__ import annotations

import argparse
import csv
import email.utils
import gzip
import hashlib
import html
import http.client
import io
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
from typing import Any


USER_AGENT = "hivemind-analysis-public-research/1.0"
DASHBOARD = "https://dash.dstrbtd.ai/mech1"
WANDB_GRAPHQL = "https://api.wandb.ai/graphql"
WANDB_PROJECTS = (
    "distributed_training_miners",
    "distributed_training_validators",
    "DeMoCo",
)
WANDB_FULL_HISTORY_PROJECTS = (
    "distributed_training_validators",
    "distributed_training_miners",
    "DeMoCo",
)
WANDB_ENTITY = "kmfoda"
WANDB_BATCH_SIZE = 20
WANDB_HISTORY_SAMPLES = 5000
WANDB_HISTORY_WINDOW = 1000
WANDB_VALIDATOR_HISTORY_WINDOW = 100
WANDB_WINDOWS_PER_QUERY = 5
MIN_REQUEST_INTERVAL = 1.05
MAX_RETRIES = 3
MAX_RESPONSE_BYTES = 128 * 1024 * 1024
SENSITIVE_KEY = re.compile(
    r"(?i)(?:password|secret|credential|influxdb[_-]?token|"
    r"(?:api|access|auth)[_-]?token|api[_-]?key|private[_-]?key|authorization)"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def retry_delay(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            date = email.utils.parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0.0, (date - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_response_cap_error(error: Any) -> bool:
    message = str(error or "").lower()
    return "single response exceeded" in message or "decompressed response exceeded" in message


def decode_response_body(data: bytes, content_encoding: str | None = None) -> tuple[bytes, str]:
    """Decode gzip responses with an output cap; callers archive the original bytes."""
    encoding = (content_encoding or "identity").strip().lower()
    is_gzip = data.startswith(b"\x1f\x8b")
    if encoding not in ("", "identity", "gzip"):
        raise RuntimeError(f"unsupported HTTP content encoding: {encoding}")
    if encoding == "gzip" and not is_gzip:
        raise RuntimeError("HTTP declared gzip but the response has no gzip signature")
    if encoding != "gzip" and not is_gzip:
        return data, "identity"

    decoded: list[bytes] = []
    total = 0
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as stream:
            while True:
                chunk = stream.read(min(1024 * 1024, MAX_RESPONSE_BYTES - total + 1))
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_RESPONSE_BYTES:
                    raise RuntimeError(
                        f"decompressed response exceeded {MAX_RESPONSE_BYTES}-byte safety cap"
                    )
                decoded.append(chunk)
    except (OSError, EOFError) as exc:
        raise RuntimeError(f"invalid gzip response: {exc}") from exc
    return b"".join(decoded), "gzip"


def parse_json(data: bytes | str) -> Any:
    if isinstance(data, bytes):
        data, _ = decode_response_body(data)
        data = data.decode("utf-8")
    return json.loads(data)


def iter_sensitive_keys(value: Any):
    if isinstance(value, dict):
        for key, child in value.items():
            if SENSITIVE_KEY.search(str(key)):
                yield str(key)
            yield from iter_sensitive_keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_sensitive_keys(child)
    elif isinstance(value, str) and value[:1] in ("{", "["):
        # W&B stores history rows and some metadata objects as JSON strings.
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return
        if isinstance(decoded, (dict, list)):
            yield from iter_sensitive_keys(decoded)


def iter_sensitive_key_paths(value: Any, path: str = "$", depth: int = 0):
    """Yield key names and paths only; never include values from a withheld response."""
    if depth > 40:
        return
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if SENSITIVE_KEY.search(str(key)):
                yield (child_path, str(key))
            yield from iter_sensitive_key_paths(child, child_path, depth + 1)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from iter_sensitive_key_paths(child, f"{path}[{index}]", depth + 1)
    elif isinstance(value, str) and value[:1] in ("{", "["):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return
        if isinstance(decoded, (dict, list)):
            yield from iter_sensitive_key_paths(decoded, path, depth + 1)


def validate_wandb_safe(doc: Any) -> tuple[bool, str | None, list[str]]:
    """Exclude a response only if it unexpectedly contains credential-like fields."""
    matches = sorted(set(iter_sensitive_key_paths(doc)))
    if not matches:
        return True, None, []
    unsafe_runs: set[str] = set()
    try:
        edges = doc["data"]["project"]["runs"]["edges"]
    except (KeyError, TypeError):
        edges = []
    for path, _key in matches:
        edge_match = re.match(r"^\$\.data\.project\.runs\.edges\[(\d+)\]\.node(?:\.|\[|$)", path)
        if edge_match:
            index = int(edge_match.group(1))
            if index < len(edges) and isinstance(edges[index], dict):
                node = edges[index].get("node", {})
                unsafe_runs.add(str(node.get("name", "<unknown>")))
    preview = ", ".join(f"{path} (key `{key}`)" for path, key in matches[:20])
    remainder = f"; {len(matches) - 20} additional matched paths omitted" if len(matches) > 20 else ""
    return (
        False,
        "credential-like key-name screen matched; response not archived; matched key paths "
        f"(no values retained): {preview}{remainder}",
        sorted(unsafe_runs),
    )


class Collector:
    def __init__(self, output: Path, max_bytes: int, resume: bool):
        self.output = output
        self.raw = output / "raw"
        self.manifest_path = output / "manifest.json"
        self.max_bytes = max_bytes
        self.resume = resume
        self.last_request_by_host: dict[str, float] = {}
        self.entries: list[dict[str, Any]] = []
        self.entry_by_id: dict[str, dict[str, Any]] = {}
        if self.manifest_path.exists():
            if not resume:
                raise RuntimeError(f"{output} already contains a manifest; pass --resume")
            try:
                saved = json.loads(self.manifest_path.read_text())
                self.entries = list(saved.get("entries", []))
                self.entry_by_id = {entry["id"]: entry for entry in self.entries if "id" in entry}
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"cannot read existing manifest: {exc}") from exc
        else:
            output.mkdir(parents=True, exist_ok=True)
            self.save_manifest(started_utc=utc_now())

    def current_bytes(self) -> int:
        if not self.raw.exists():
            return 0
        return sum(
            path.stat().st_size for path in self.raw.rglob("*")
            if path.is_file() and path != self.manifest_path
        )

    def save_manifest(self, **updates: Any) -> None:
        current: dict[str, Any] = {}
        if self.manifest_path.exists():
            try:
                current = json.loads(self.manifest_path.read_text())
            except (OSError, json.JSONDecodeError):
                current = {}
        current.update(
            {
                "schema_version": 1,
                "collection_label": self.output.name,
                "max_raw_bytes": self.max_bytes,
                "raw_bytes": self.current_bytes(),
                "entries": self.entries,
            }
        )
        current.update(updates)
        self.output.mkdir(parents=True, exist_ok=True)
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.manifest_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n")
        tmp.replace(self.manifest_path)

    def _existing(self, request_id: str, relative: str) -> dict[str, Any] | None:
        entry = self.entry_by_id.get(request_id)
        target = self.output / relative
        if entry and entry.get("status") == 200 and entry.get("path"):
            if not target.is_file():
                raise RuntimeError(f"saved response is missing for {request_id}: {target}")
            body = target.read_bytes()
            if len(body) != entry.get("bytes") or sha256(body) != entry.get("sha256"):
                raise RuntimeError(f"saved response failed manifest verification for {request_id}: {target}")
            if not self.resume:
                raise RuntimeError(f"response already exists for {request_id}; pass --resume")
            return entry
        if target.exists():
            raise RuntimeError(f"untracked response already exists for {request_id}: {target}")
        if not self.resume or not entry or entry.get("status") == 200:
            return None
        return None

    def fetch(
        self,
        request_id: str,
        url: str,
        relative: str,
        *,
        method: str = "GET",
        payload: bytes | None = None,
        validate_wandb: bool = False,
    ) -> tuple[dict[str, Any] | None, bytes | None]:
        saved = self.entry_by_id.get(request_id)
        if self.resume and saved and saved.get("application_status") == "excluded":
            # Excluded response bodies are intentionally not archived. Reusing
            # them would bypass the safety gate; callers may retry with a safer,
            # narrower query under a distinct request ID instead.
            return saved, None
        if self.resume and saved and is_response_cap_error(saved.get("error")):
            return saved, None
        existing = self._existing(request_id, relative)
        if existing:
            body = (self.output / relative).read_bytes()
            decode_response_body(body, existing.get("content_encoding"))
            return existing, body

        host = urllib.parse.urlparse(url).netloc
        for attempt in range(1, MAX_RETRIES + 1):
            remaining = MIN_REQUEST_INTERVAL - (time.monotonic() - self.last_request_by_host.get(host, 0.0))
            if remaining > 0:
                time.sleep(remaining)
            self.last_request_by_host[host] = time.monotonic()
            headers = {"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*"}
            if payload is not None:
                headers["Content-Type"] = "application/json"
                if urllib.parse.urlparse(url).hostname == "api.wandb.ai":
                    headers["Accept-Encoding"] = "gzip"
            req = urllib.request.Request(url, data=payload, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=60) as response:
                    status = response.status
                    stored_data = self._read_limited(response, self.max_bytes - self.current_bytes())
                    decoded_data, content_encoding = decode_response_body(
                        stored_data, response.headers.get("Content-Encoding")
                    )
                    if status < 200 or status >= 300:
                        return self._record_failure(request_id, url, method, relative, status, "HTTP status"), None
                    if validate_wandb:
                        try:
                            safe, reason, unsafe_runs = validate_wandb_safe(parse_json(decoded_data))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            safe, reason, unsafe_runs = False, "invalid JSON response; response not archived", []
                        if not safe:
                            failed = self._record_failure(
                                request_id, url, method, relative, status, reason or "safety gate", payload
                            )
                            failed["application_status"] = "excluded"
                            failed["excluded_runs"] = unsafe_runs
                            self.save_manifest()
                            return failed, stored_data
                    target = self.output / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    tmp = target.with_name(f".{target.name}.{os.getpid()}.{time.time_ns()}.tmp")
                    with tmp.open("xb") as f:
                        f.write(stored_data)
                        f.flush()
                        os.fsync(f.fileno())
                    try:
                        os.link(tmp, target)
                    except FileExistsError as exc:
                        raise RuntimeError(f"refusing to overwrite existing response: {target}") from exc
                    finally:
                        tmp.unlink(missing_ok=True)
                    entry = {
                        "id": request_id,
                        "url": url,
                        "method": method,
                        "request_body": payload.decode("utf-8") if payload else None,
                        "path": relative,
                        "status": status,
                        "content_encoding": content_encoding,
                        "decoded_bytes": len(decoded_data),
                        "sha256": sha256(stored_data),
                        "bytes": len(stored_data),
                        "retrieved_at": utc_now(),
                    }
                    self._record(entry)
                    if validate_wandb:
                        try:
                            decoded = parse_json(decoded_data)
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            decoded = None
                        if isinstance(decoded, dict) and decoded.get("errors"):
                            entry["application_status"] = "graphql_error"
                            entry["query_errors"] = decoded["errors"]
                            self.save_manifest()
                    return entry, stored_data
            except urllib.error.HTTPError as exc:
                status = int(exc.code)
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                reason = f"HTTP {status}"
                if status in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                    wait = retry_delay(retry_after) if retry_after else 1.5 * attempt
                    if wait > 30.0:
                        return self._record_failure(
                            request_id, url, method, relative, status,
                            f"retry deferred to honor Retry-After ({wait:.1f}s)", payload
                        ), None
                    time.sleep(wait)
                    continue
                return self._record_failure(request_id, url, method, relative, status, reason, payload), None
            except (urllib.error.URLError, TimeoutError, OSError, RuntimeError, http.client.HTTPException) as exc:
                reason = f"{type(exc).__name__}: {exc}"
                if is_response_cap_error(reason):
                    return self._record_failure(request_id, url, method, relative, None, reason, payload), None
                if attempt < MAX_RETRIES and "budget" not in reason.lower():
                    time.sleep(1.5 * attempt)
                    continue
                return self._record_failure(request_id, url, method, relative, None, reason, payload), None
        return None, None

    @staticmethod
    def _read_limited(response: Any, remaining: int) -> bytes:
        if remaining <= 0:
            raise RuntimeError("raw data budget exhausted")
        limit = min(remaining, MAX_RESPONSE_BYTES)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = response.read(min(1024 * 1024, limit - total + 1))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > limit:
                if limit == remaining:
                    raise RuntimeError("raw data budget exhausted")
                raise RuntimeError(f"single response exceeded {MAX_RESPONSE_BYTES}-byte safety cap")
        return b"".join(chunks)

    def _record(self, entry: dict[str, Any]) -> None:
        previous = self.entry_by_id.get(entry["id"])
        if previous:
            attempts = list(previous.get("attempts", []))
            attempts.append({k: v for k, v in previous.items() if k not in {"attempts", "id"}})
            entry["attempts"] = attempts
            self.entries[self.entries.index(previous)] = entry
        else:
            self.entries.append(entry)
        self.entry_by_id[entry["id"]] = entry
        self.save_manifest()

    def _record_failure(
        self,
        request_id: str,
        url: str,
        method: str,
        relative: str,
        status: int | None,
        reason: str,
        payload: bytes | None = None,
    ) -> dict[str, Any]:
        entry = {
            "id": request_id,
            "url": url,
            "method": method,
            "request_body": payload.decode("utf-8") if payload else None,
            "path": None,
            "status": status,
            "sha256": None,
            "bytes": 0,
            "retrieved_at": utc_now(),
            "error": reason,
        }
        self._record(entry)
        return entry


def graphql_body(
    project: str,
    after: str | None,
    batch_size: int = WANDB_BATCH_SIZE,
    history_samples: int = WANDB_HISTORY_SAMPLES,
    include_history: bool = True,
) -> bytes:
    after_arg = f", after:{json.dumps(after)}" if after else ""
    history_field = f" history(samples:{history_samples})" if include_history else ""
    query = (
        'query { project(name:' + json.dumps(project) + ', entityName:' + json.dumps(WANDB_ENTITY) + ') {'
        ' runCount runs(first:' + str(batch_size) + ', order:"createdAt"' + after_arg + ') {'
        ' edges { cursor node { name displayName state createdAt heartbeatAt historyLineCount' + history_field + ' } } '
        'pageInfo { endCursor hasNextPage } } } }'
    )
    return json.dumps({"query": query}, separators=(",", ":")).encode("utf-8")


def history_keys_body(project: str, run_ids: list[str]) -> tuple[bytes, list[dict[str, str]]]:
    aliases = [{"alias": f"r{index:03d}", "run_id": run_id} for index, run_id in enumerate(run_ids)]
    fields = " ".join(
        f"{row['alias']}: run(name:{json.dumps(row['run_id'])}) {{ historyKeys }}"
        for row in aliases
    )
    query = (
        "query { project(name:" + json.dumps(project) + ", entityName:" + json.dumps(WANDB_ENTITY)
        + ") { " + fields + " } }"
    )
    return json.dumps({"query": query}, separators=(",", ":")).encode("utf-8"), aliases


def history_windows_body(
    project: str, run_id: str, windows: list[tuple[int, int]], samples: int = WANDB_HISTORY_WINDOW
) -> tuple[bytes, list[dict[str, Any]]]:
    aliases = [
        {"alias": f"h{index:03d}", "min_step": start, "max_step": end}
        for index, (start, end) in enumerate(windows)
    ]
    fields = " ".join(
        f"{row['alias']}: history(minStep:{row['min_step']},maxStep:{row['max_step']},samples:{samples})"
        for row in aliases
    )
    query = (
        "query { project(name:" + json.dumps(project) + ", entityName:" + json.dumps(WANDB_ENTITY)
        + ") { run(name:" + json.dumps(run_id) + ") { " + fields + " } } }"
    )
    return json.dumps({"query": query}, separators=(",", ":")).encode("utf-8"), aliases


def raw_doc(output: Path, relative: str) -> Any | None:
    path = output / relative
    if not path.is_file():
        return None
    try:
        return parse_json(path.read_bytes())
    except (OSError, RuntimeError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def collect_dashboard(collector: Collector) -> None:
    source_url = "https://www.dstrbtd.ai/"
    dashboard_url = DASHBOARD
    collector.fetch("site-home", source_url, "raw/site/home.html")
    _, dash_html = collector.fetch("dashboard-html", dashboard_url, "raw/dashboard/mech1.html")
    js_url = None
    if dash_html:
        match = re.search(rb'<script[^>]+src=["\']([^"\']+\.js)', dash_html)
        if match:
            js_path = match.group(1).decode("utf-8", "replace")
            js_url = urllib.parse.urljoin(dashboard_url, html.unescape(js_path))
    if not js_url:
        js_url = "https://dash.dstrbtd.ai/assets/index-B5n3muS4.js"
    _, js = collector.fetch("dashboard-app-js", js_url, "raw/dashboard/mech1-app.js")
    api_base = "https://ngrok-dash1.dstrbtd.ai"
    iframe_url = "https://ngrok-dash0.dstrbtd.ai"
    if js:
        text = js.decode("utf-8", "replace")
        api_match = re.search(r'kf="(https://[^"\\]+)"', text)
        if api_match:
            api_base = api_match.group(1)
        frame_match = re.search(r'src:"(https://ngrok-dash0\.[^"\\]+)"', text)
        if frame_match:
            iframe_url = frame_match.group(1)
    collector.fetch("dashboard-mech0", iframe_url, "raw/dashboard/mech0.html")

    history_url = api_base.rstrip("/") + "/api/mech1/history"
    strategies_url = api_base.rstrip("/") + "/api/mech1/strategies"
    _, history = collector.fetch("mech1-history", history_url, "raw/mech1/history.json")
    _, strategies = collector.fetch("mech1-strategies-index", strategies_url, "raw/mech1/strategies-index.json")
    if not strategies:
        return
    try:
        doc = parse_json(strategies)
        blocks = doc.get("available_blocks", [])
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
        return
    for block in blocks:
        block_text = str(block)
        url = strategies_url + "?" + urllib.parse.urlencode({"block": block_text})
        collector.fetch(
            f"mech1-strategies-block-{block_text}",
            url,
            f"raw/mech1/strategies-block-{block_text}.json",
        )


def collect_huggingface(collector: Collector) -> None:
    models = (
        ("distributed/llama-4b", "linked-in-dashboard; model/run use not verified"),
        ("dstrbtd/llama-1b", "repository-default tokenizer; model/run use not verified"),
    )
    for model_id, _context in models:
        safe_name = model_id.replace("/", "-")
        api_url = "https://huggingface.co/api/models/" + model_id
        card_url = "https://huggingface.co/" + model_id + "/raw/main/README.md"
        collector.fetch(f"hf-model-meta-{safe_name}", api_url, f"raw/huggingface/{safe_name}.json")
        collector.fetch(f"hf-model-card-{safe_name}", card_url, f"raw/huggingface/{safe_name}-README.md")


def collect_wandb(collector: Collector, batch_size: int = WANDB_BATCH_SIZE) -> None:
    for project in WANDB_PROJECTS:
        # Validator histories can be much wider per run than miner histories.
        # Fetch them one run at a time to bound each GraphQL response.
        project_batch_size = 1 if project == "distributed_training_validators" else batch_size
        project_history_samples = WANDB_HISTORY_SAMPLES
        page = 0
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            page_request_id = f"wandb-{project}-page-{page:04d}"
            page_id_prefix = page_request_id + "-samples-"
            page_entries = (
                [
                    item for item in collector.entries
                    if item.get("id") == page_request_id
                    or str(item.get("id", "")).startswith(page_id_prefix)
                    or item.get("id") == f"{page_request_id}-metadata-only"
                ]
                if project == "distributed_training_validators" else []
            )
            sample_options = [project_history_samples]
            previously_excluded = any(item.get("application_status") == "excluded" for item in page_entries)
            if project == "distributed_training_validators":
                # Skip any previously capped sample size and resume at the
                # smallest bounded variant that has not been ruled out.
                failed_samples: list[int] = []
                for saved in page_entries:
                    attempts = list(saved.get("attempts", []))
                    if saved.get("error"):
                        attempts.append(saved)
                    failed_samples.extend(
                        int(match.group(1))
                        for attempt in attempts
                        if is_response_cap_error(attempt.get("error"))
                        for match in [re.search(r"history\(samples:(\d+)\)", attempt.get("request_body") or "")]
                        if match
                    )
                successful_samples = {
                    int(match.group(1))
                    for item in page_entries
                    if item.get("status") == 200
                    for match in [re.search(r"history\(samples:(\d+)\)", item.get("request_body") or "")]
                    if match
                }
                if failed_samples:
                    prior = failed_samples[-1]
                    sample_options = [size for size in (100, 10) if size < prior]
                elif successful_samples:
                    project_history_samples = min(successful_samples)
                    sample_options = [project_history_samples, *[size for size in (100, 10) if size < project_history_samples]]
                else:
                    sample_options = [project_history_samples, *[size for size in (100, 10) if size < project_history_samples]]
            entry = body = None
            if not previously_excluded:
                for history_samples in sample_options:
                    if history_samples == WANDB_HISTORY_SAMPLES:
                        request_id = page_request_id
                        relative = f"raw/wandb/{WANDB_ENTITY}/{project}/page-{page:04d}.json"
                    else:
                        request_id = f"{page_request_id}-samples-{history_samples:04d}"
                        relative = f"raw/wandb/{WANDB_ENTITY}/{project}/page-{page:04d}-samples-{history_samples:04d}.json"
                    payload = graphql_body(project, cursor, project_batch_size, history_samples)
                    entry, body = collector.fetch(
                        request_id,
                        WANDB_GRAPHQL,
                        relative,
                        method="POST",
                        payload=payload,
                        validate_wandb=True,
                    )
                    if body and entry and entry.get("status") == 200:
                        project_history_samples = min(project_history_samples, history_samples)
                        break
                    if not is_response_cap_error((entry or {}).get("error")):
                        break
            if previously_excluded or (entry and entry.get("application_status") == "excluded"):
                # Keep the excluded history response out of the archive, then
                # retry only identity/state/count fields to preserve pagination.
                safe_id = f"{page_request_id}-metadata-only"
                safe_rel = f"raw/wandb/{WANDB_ENTITY}/{project}/page-{page:04d}-metadata-only.json"
                safe_payload = graphql_body(project, cursor, project_batch_size, include_history=False)
                entry, body = collector.fetch(
                    safe_id, WANDB_GRAPHQL, safe_rel, method="POST", payload=safe_payload,
                    validate_wandb=True,
                )
            if not body or not entry or entry.get("status") != 200:
                break
            try:
                doc = parse_json(body)
                project_doc = doc.get("data", {}).get("project")
            except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
                break
            if doc.get("errors"):
                saved = collector.entry_by_id.get(request_id)
                if saved:
                    saved["application_status"] = "graphql_error"
                    saved["query_errors"] = doc["errors"]
                    collector.save_manifest()
                break
            if project_doc is None:
                saved = collector.entry_by_id.get(request_id)
                if saved:
                    saved["application_status"] = "project_not_found"
                    collector.save_manifest()
                break
            try:
                page_info = project_doc["runs"]["pageInfo"]
            except (KeyError, TypeError):
                saved = collector.entry_by_id.get(request_id)
                if saved:
                    saved["application_status"] = "unexpected_graphql_shape"
                    collector.save_manifest()
                break
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                saved = collector.entry_by_id.get(request_id)
                if saved:
                    saved["application_status"] = "missing_end_cursor"
                    collector.save_manifest()
                break
            cursor = str(cursor)
            if cursor in seen_cursors:
                saved = collector.entry_by_id.get(request_id)
                if saved:
                    saved["application_status"] = "repeated_pagination_cursor"
                    saved["error"] = "W&B returned an already-seen endCursor; pagination stopped to avoid looping"
                    collector.save_manifest()
                break
            seen_cursors.add(cursor)
            page += 1
            if page % 10 == 0:
                print(f"W&B {project}: fetched {page * project_batch_size} run records", flush=True)


def collect_wandb_full_history(collector: Collector) -> None:
    """Fetch every step range for runs truncated by the sampled history query."""
    for project in WANDB_FULL_HISTORY_PROJECTS:
        partial_runs: dict[str, dict[str, Any]] = {}
        suffix = f"/wandb/{WANDB_ENTITY}/{project}/page-"
        for entry in collector.entries:
            rel = entry.get("path") or ""
            if entry.get("status") != 200 or suffix not in rel or not rel.endswith(".json"):
                continue
            doc = raw_doc(collector.output, rel)
            try:
                nodes = doc["data"]["project"]["runs"]["edges"]
            except (TypeError, KeyError):
                continue
            for edge in nodes:
                node = edge.get("node", {})
                run_id = str(node.get("name") or "")
                reported = node.get("historyLineCount")
                history = node.get("history") or []
                count_mismatch = type(reported) is not int or reported < 0 or reported != len(history)
                if run_id and count_mismatch:
                    partial_runs[run_id] = {
                        "run_id": run_id,
                        "history_line_count": reported,
                        "sampled_rows": len(history),
                        "sampled_raw_ref": rel,
                    }
        if not partial_runs:
            continue

        run_ids = sorted(partial_runs)
        steps_by_run: dict[str, int | None] = {}
        for page, offset in enumerate(range(0, len(run_ids), WANDB_BATCH_SIZE)):
            batch = run_ids[offset : offset + WANDB_BATCH_SIZE]
            payload, aliases = history_keys_body(project, batch)
            request_id = f"wandb-{project}-history-keys-page-{page:04d}"
            relative = f"raw/wandb/{WANDB_ENTITY}/{project}/history-keys-page-{page:04d}.json"
            entry, body = collector.fetch(
                request_id, WANDB_GRAPHQL, relative, method="POST", payload=payload, validate_wandb=True
            )
            archived_safe_response = bool(
                entry
                and entry.get("status") == 200
                and entry.get("path")
                and entry.get("application_status") not in ("excluded", "graphql_error")
                and body
            )
            if not archived_safe_response:
                for alias in aliases:
                    steps_by_run[alias["run_id"]] = None
                    partial_runs[alias["run_id"]]["last_step"] = None
                if entry and entry.get("application_status") == "excluded":
                    # A single sensitive key in a batched historyKeys response
                    # should not withhold safe siblings. Never inspect the
                    # excluded body; re-query each run independently.
                    resolved_run_ids: list[str] = []
                    excluded_run_ids: list[str] = []
                    entry.pop("history_key_runs", None)
                    for alias in aliases:
                        run_id = alias["run_id"]
                        single_payload, single_aliases = history_keys_body(project, [run_id])
                        single_alias = single_aliases[0]
                        single_id = (
                            f"wandb-{project}-history-keys-run-"
                            f"{urllib.parse.quote(run_id, safe='')}"
                        )
                        single_relative = (
                            f"raw/wandb/{WANDB_ENTITY}/{project}/history-keys-runs/"
                            f"{urllib.parse.quote(run_id, safe='')}.json"
                        )
                        single_entry, single_body = collector.fetch(
                            single_id,
                            WANDB_GRAPHQL,
                            single_relative,
                            method="POST",
                            payload=single_payload,
                            validate_wandb=True,
                        )
                        single_safe = bool(
                            single_entry
                            and single_entry.get("status") == 200
                            and single_entry.get("path")
                            and single_entry.get("application_status") not in ("excluded", "graphql_error")
                            and single_body
                        )
                        if not single_safe:
                            excluded_run_ids.append(run_id)
                            partial_runs[run_id]["history_key_exclusion_reason"] = (
                                "single-run historyKeys response excluded or unavailable"
                                if single_entry and single_entry.get("application_status") == "excluded"
                                else "single-run historyKeys response unavailable"
                            )
                            continue
                        try:
                            single_response = parse_json(single_body)
                            single_project = single_response["data"]["project"]
                        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
                            single_response = {}
                            single_project = {}
                        if single_response.get("errors"):
                            single_entry["application_status"] = "graphql_error"
                            single_entry["query_errors"] = single_response["errors"]
                            collector.save_manifest()
                            excluded_run_ids.append(run_id)
                            partial_runs[run_id]["history_key_exclusion_reason"] = (
                                "single-run historyKeys GraphQL error"
                            )
                            continue
                        single_info = single_project.get(single_alias["alias"]) or {}
                        single_keys = single_info.get("historyKeys") if isinstance(single_info, dict) else None
                        if isinstance(single_keys, str):
                            try:
                                single_keys = parse_json(single_keys)
                            except (UnicodeDecodeError, json.JSONDecodeError):
                                single_keys = None
                        last_step = single_keys.get("lastStep") if isinstance(single_keys, dict) else None
                        if type(last_step) is not int or last_step < 0:
                            last_step = None
                        steps_by_run[run_id] = last_step
                        partial_runs[run_id]["history_keys_raw_ref"] = single_entry["path"]
                        partial_runs[run_id]["last_step"] = last_step
                        single_entry["history_key_runs"] = [
                            {
                                "project": project,
                                "run_id": run_id,
                                "alias": single_alias["alias"],
                                "last_step": last_step,
                            }
                        ]
                        collector.save_manifest()
                        resolved_run_ids.append(run_id)
                    entry["resolved_run_ids"] = resolved_run_ids
                    entry["excluded_run_ids"] = excluded_run_ids
                    collector.save_manifest()
                continue
            try:
                response = parse_json(body)
                run_map = response["data"]["project"]
            except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
                response = {}
                run_map = {}
            if response.get("errors"):
                entry["application_status"] = "graphql_error"
                entry["query_errors"] = response["errors"]
                collector.save_manifest()
                for alias in aliases:
                    steps_by_run[alias["run_id"]] = None
                continue
            scan_meta = []
            for alias in aliases:
                run_info = run_map.get(alias["alias"]) or {}
                history_keys = run_info.get("historyKeys") if isinstance(run_info, dict) else None
                if isinstance(history_keys, str):
                    try:
                        history_keys = parse_json(history_keys)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        history_keys = None
                last_step = history_keys.get("lastStep") if isinstance(history_keys, dict) else None
                if type(last_step) is not int or last_step < 0:
                    last_step = None
                steps_by_run[alias["run_id"]] = last_step
                partial_runs[alias["run_id"]]["history_keys_raw_ref"] = relative
                partial_runs[alias["run_id"]]["last_step"] = last_step
                scan_meta.append(
                    {
                        "project": project,
                        "alias": alias["alias"],
                        **partial_runs[alias["run_id"]],
                    }
                )
            entry["history_key_runs"] = scan_meta
            collector.save_manifest()

        for run_id in run_ids:
            source_last_step = steps_by_run.get(run_id)
            if source_last_step is None:
                partial_runs[run_id]["scan_skip_reason"] = partial_runs[run_id].get(
                    "history_key_exclusion_reason", "public historyKeys.lastStep unavailable"
                )
                continue
            window_size = WANDB_VALIDATOR_HISTORY_WINDOW if project == "distributed_training_validators" else WANDB_HISTORY_WINDOW
            windows_per_query = WANDB_WINDOWS_PER_QUERY
            partial_runs[run_id]["window_size"] = window_size
            ranges = [(start, min(start + window_size, source_last_step + 1))
                      for start in range(0, source_last_step + 1, window_size)]
            expected_ranges = set(ranges)
            per_run_meta: list[dict[str, Any]] = []

            # Reuse exact windows already archived by an earlier batching scheme.
            # Their old request IDs may differ; range metadata plus verified bytes
            # provide stable identity for this purpose.
            cached_ranges: dict[tuple[int, int], dict[str, Any]] = {}
            for saved in collector.entries:
                scan = saved.get("history_scan") or {}
                if (
                    scan.get("project") != project
                    or scan.get("run_id") != run_id
                    or saved.get("status") != 200
                    or saved.get("application_status")
                    or not saved.get("path")
                ):
                    continue
                saved_path = collector.output / saved["path"]
                if not saved_path.is_file():
                    raise RuntimeError(f"saved full-history response is missing: {saved.get('id')}")
                saved_bytes = saved_path.read_bytes()
                if len(saved_bytes) != saved.get("bytes") or sha256(saved_bytes) != saved.get("sha256"):
                    raise RuntimeError(f"saved full-history response failed hash verification: {saved.get('id')}")
                saved_doc = raw_doc(collector.output, saved["path"])
                for window in scan.get("windows", []):
                    range_key = (window.get("min_step"), window.get("max_step"))
                    if range_key not in expected_ranges or range_key in cached_ranges:
                        continue
                    returned, saved_rows = window_response_rows(
                        collector.output, saved, window, doc=saved_doc
                    )
                    if not returned:
                        continue
                    cached_ranges[range_key] = {
                        "alias": window["alias"],
                        "min_step": range_key[0],
                        "max_step": range_key[1],
                        "alias_returned": True,
                        "returned_rows": len(saved_rows),
                        "request_id": saved.get("id"),
                        "raw_ref": saved["path"],
                        "reused_prior_request_name": True,
                    }
            per_run_meta.extend(cached_ranges.values())
            missing_ranges = [item for item in ranges if item not in cached_ranges]

            def fetch_window_batch(batch_ranges: list[tuple[int, int]]) -> None:
                if not batch_ranges or partial_runs[run_id].get("scan_skip_reason") == "raw data budget exhausted":
                    return
                payload, aliases = history_windows_body(project, run_id, batch_ranges, samples=window_size)
                first_step = batch_ranges[0][0]
                batch_end = batch_ranges[-1][1]
                query_fingerprint = sha256(payload)[:12]
                request_id = (
                    f"wandb-{project}-fullhist-{urllib.parse.quote(run_id, safe='')}-"
                    f"steps-{first_step}-{batch_end}-q-{query_fingerprint}"
                )
                relative = (
                    f"raw/wandb/{WANDB_ENTITY}/{project}/history-windows/"
                    f"{urllib.parse.quote(run_id, safe='')}-steps-{first_step}-{batch_end}-q-{query_fingerprint}.json"
                )
                entry, body = collector.fetch(
                    request_id, WANDB_GRAPHQL, relative, method="POST", payload=payload, validate_wandb=True
                )
                if entry and is_response_cap_error(entry.get("error")) and len(batch_ranges) > 1:
                    middle = len(batch_ranges) // 2
                    fetch_window_batch(batch_ranges[:middle])
                    fetch_window_batch(batch_ranges[middle:])
                    return
                windows_meta = []
                response_runs: dict[str, Any] = {}
                if (
                    entry
                    and entry.get("status") == 200
                    and entry.get("path")
                    and entry.get("application_status") != "excluded"
                    and body
                ):
                    try:
                        response = parse_json(body)
                        response_runs = response["data"]["project"]["run"] or {}
                    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
                        response = {}
                        response_runs = {}
                    if response.get("errors"):
                        entry["application_status"] = "graphql_error"
                        entry["query_errors"] = response["errors"]
                        collector.save_manifest()
                        response_runs = {}
                for alias in aliases:
                    rows = response_runs.get(alias["alias"])
                    alias_returned = isinstance(rows, list)
                    if not alias_returned:
                        rows = []
                    windows_meta.append(
                        {
                            "alias": alias["alias"],
                            "min_step": alias["min_step"],
                            "max_step": alias["max_step"],
                            "alias_returned": alias_returned,
                            "returned_rows": len(rows),
                        }
                    )
                if entry:
                    entry["history_scan"] = {
                        "project": project,
                        "run_id": run_id,
                        "history_line_count": partial_runs[run_id]["history_line_count"],
                        "last_step": source_last_step,
                        "source_last_step": source_last_step,
                        "history_keys_raw_ref": partial_runs[run_id].get("history_keys_raw_ref"),
                        "window_size": window_size,
                        "windows": windows_meta,
                    }
                    collector.save_manifest()
                per_run_meta.extend(
                    {
                        **window,
                        "request_id": request_id,
                        "raw_ref": entry.get("path") if entry else None,
                    }
                    for window in windows_meta
                )
                if entry and entry.get("error") and "budget" in entry["error"].lower():
                    partial_runs[run_id]["scan_skip_reason"] = "raw data budget exhausted"
            for offset in range(0, len(missing_ranges), windows_per_query):
                fetch_window_batch(missing_ranges[offset : offset + windows_per_query])
            partial_runs[run_id]["window_request_refs"] = per_run_meta
            print(
                f"W&B {project}: full-history windows for {run_id} "
                f"({len(per_run_meta)} ranges, window={window_size}, lastStep={source_last_step})",
                flush=True,
            )
        (collector.output / f"wandb-full-history-plan-{project}.json").write_text(
            json.dumps(list(partial_runs.values()), indent=2, sort_keys=True) + "\n"
        )


def manifest_doc(output: Path) -> dict[str, Any]:
    path = output / "manifest.json"
    if not path.is_file():
        return {"entries": []}
    return json.loads(path.read_text())


def verify_raw_manifest(output: Path) -> list[dict[str, Any]]:
    """Fail closed if any response marked successful is missing or changed."""
    verified = []
    for entry in manifest_doc(output).get("entries", []):
        if entry.get("status") != 200 or not entry.get("path"):
            continue
        path = output / entry["path"]
        if not path.is_file():
            raise RuntimeError(f"successful raw response is missing: {entry.get('id')} ({path})")
        body = path.read_bytes()
        if len(body) != entry.get("bytes") or sha256(body) != entry.get("sha256"):
            raise RuntimeError(f"successful raw response failed hash verification: {entry.get('id')} ({path})")
        verified.append(entry)
    return verified


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def decode_history_row(value: Any) -> dict[str, Any] | None:
    try:
        row = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return None
    return row if isinstance(row, dict) else None


def iter_sampled_history(output: Path, entries: list[dict[str, Any]]):
    for entry in entries:
        rel = entry.get("path") or ""
        if entry.get("status") != 200 or "/wandb/" not in rel or "/history-windows/" in rel or "history-keys-page" in rel:
            continue
        doc = raw_doc(output, rel)
        try:
            edges = doc["data"]["project"]["runs"]["edges"]
        except (TypeError, KeyError):
            continue
        project = Path(rel).parent.name
        for edge in edges:
            node = edge.get("node", {})
            hist = node.get("history") or []
            reported = node.get("historyLineCount")
            matches = type(reported) is int and reported > 0 and len(hist) == reported
            coverage = (
                "count_unknown" if type(reported) is not int or reported < 0 else
                "zero_reported" if reported == 0 else
                "count_matches_reported" if matches else
                "returned_count_differs_from_reported"
            )
            for index, raw_row in enumerate(hist):
                metrics = decode_history_row(raw_row)
                if metrics is None:
                    continue
                yield {
                    "entity": WANDB_ENTITY,
                    "project": project,
                    "run_id": node.get("name"),
                    "display_name": node.get("displayName"),
                    "state": node.get("state"),
                    "created_at": node.get("createdAt"),
                    "heartbeat_at": node.get("heartbeatAt"),
                    "row_index": index,
                    "history_line_count": reported,
                    "returned_history_rows": len(hist),
                    "history_count_matches_reported": matches,
                    "history_coverage": coverage,
                    "raw_ref": rel,
                    "metrics": metrics,
                }


def window_response_rows(
    output: Path,
    entry: dict[str, Any],
    window: dict[str, Any],
    *,
    doc: Any | None = None,
) -> tuple[bool, list[Any]]:
    if doc is None:
        doc = raw_doc(output, entry["path"])
    try:
        response = doc["data"]["project"]["run"]
    except (TypeError, KeyError):
        return False, []
    if not isinstance(response, dict) or window["alias"] not in response:
        return False, []
    rows = response[window["alias"]]
    return (True, rows) if isinstance(rows, list) else (False, [])


def verified_history_key_last_steps(output: Path, entries: list[dict[str, Any]]) -> dict[tuple[str, str], int]:
    """Read lastStep only from successful, archived historyKeys responses."""
    result: dict[tuple[str, str], int] = {}
    project_pattern = re.compile(r'\bproject\s*\(\s*name\s*:\s*("(?:\\.|[^"\\])*")')
    run_pattern = re.compile(
        r'([A-Za-z_][A-Za-z0-9_]*)\s*:\s*run\s*\(\s*name\s*:\s*("(?:\\.|[^"\\])*")\s*\)\s*\{\s*historyKeys\s*\}'
    )
    for entry in entries:
        relative = entry.get("path") or ""
        if (
            entry.get("status") != 200
            or not relative
            or entry.get("application_status")
            or "history-keys" not in str(entry.get("id", ""))
        ):
            continue
        query_body = entry.get("request_body")
        try:
            query = json.loads(query_body).get("query") if isinstance(query_body, str) else None
        except json.JSONDecodeError:
            query = None
        project_match = project_pattern.search(query or "")
        if not project_match:
            continue
        project = json.loads(project_match.group(1))
        target = output / relative
        if not target.is_file():
            raise RuntimeError(f"saved historyKeys response is missing: {entry.get('id')}")
        body = target.read_bytes()
        if len(body) != entry.get("bytes") or sha256(body) != entry.get("sha256"):
            raise RuntimeError(f"saved historyKeys response failed manifest verification: {entry.get('id')}")
        doc = raw_doc(output, relative)
        try:
            response_project = doc["data"]["project"]
        except (TypeError, KeyError):
            continue
        for alias, quoted_run_id in run_pattern.findall(query or ""):
            run_id = json.loads(quoted_run_id)
            run_info = response_project.get(alias) if isinstance(response_project, dict) else None
            history_keys = run_info.get("historyKeys") if isinstance(run_info, dict) else None
            if isinstance(history_keys, str):
                try:
                    history_keys = parse_json(history_keys)
                except (UnicodeDecodeError, json.JSONDecodeError, RuntimeError):
                    history_keys = None
            last_step = history_keys.get("lastStep") if isinstance(history_keys, dict) else None
            if type(last_step) is int and last_step >= 0:
                result[(project, run_id)] = last_step
    return result


def full_history_coverage(output: Path, entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    verified_last_steps = verified_history_key_last_steps(output, entries)
    by_run: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for entry in entries:
        scan = entry.get("history_scan")
        if scan:
            by_run.setdefault((scan["project"], scan["run_id"]), []).append(entry)

    coverage: dict[str, dict[str, Any]] = {}
    for (project, run_id), run_entries in sorted(by_run.items()):
        run_entries.sort(key=lambda entry: min((window["min_step"] for window in entry.get("history_scan", {}).get("windows", [])), default=0))
        scan = run_entries[0]["history_scan"]
        window_size = scan.get("window_size", WANDB_HISTORY_WINDOW)
        # The scan's manifest metadata can outlive an excluded or missing
        # historyKeys response. Treat lastStep as verified only when it can be
        # re-derived from an archived, hash-checked successful response.
        last_step = verified_last_steps.get((project, run_id))
        expected = [
            (start, min(start + window_size, last_step + 1))
            for start in range(0, last_step + 1, window_size)
        ] if type(last_step) is int and last_step >= 0 else []
        seen_fingerprints: set[str] = set()
        seen_steps: set[Any] = set()
        raw_rows = 0
        windows_returned: set[tuple[int, int]] = set()
        duplicate_rows = 0
        for entry in run_entries:
            if entry.get("status") != 200 or entry.get("application_status"):
                continue
            doc = raw_doc(output, entry["path"])
            for window in entry.get("history_scan", {}).get("windows", []):
                alias_returned, rows = window_response_rows(output, entry, window, doc=doc)
                if not alias_returned:
                    continue
                windows_returned.add((window["min_step"], window["max_step"]))
                for raw_row in rows:
                    metrics = decode_history_row(raw_row)
                    if metrics is None:
                        continue
                    raw_rows += 1
                    fingerprint = sha256(json.dumps(metrics, sort_keys=True, separators=(",", ":")).encode())
                    if fingerprint in seen_fingerprints:
                        duplicate_rows += 1
                    else:
                        seen_fingerprints.add(fingerprint)
                    if "_step" in metrics:
                        seen_steps.add(metrics["_step"])
        reported = scan.get("history_line_count")
        all_ranges_returned = bool(expected) and all(window in windows_returned for window in expected)
        coverage[f"{project}/{run_id}"] = {
            "project": project,
            "run_id": run_id,
            "history_line_count_reported": reported,
            "last_step": last_step,
            "last_step_verified": type(last_step) is int,
            "window_size": window_size,
            "expected_step_ranges": len(expected),
            "step_ranges_returned": len(windows_returned),
            "all_expected_step_ranges_returned": all_ranges_returned,
            "raw_window_rows": raw_rows,
            "unique_row_fingerprints": len(seen_fingerprints),
            "duplicate_row_fingerprints": duplicate_rows,
            "distinct_steps_observed": len(seen_steps),
            "unique_rows_match_reported_line_count": type(reported) is int and len(seen_fingerprints) == reported,
            "all_ranges_and_row_count_match": all_ranges_returned and type(reported) is int and len(seen_fingerprints) == reported,
        }
    return coverage


def summarize_full_history(
    run_rows: list[dict[str, Any]],
    coverage: dict[str, dict[str, Any]],
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    """Summarize mismatch candidates from verified run rows and recorded scans.

    Collection may stop before per-project planning files are written. The raw
    manifest and normalized run/coverage tables are the durable sources for
    reporting in that case; missing plan files must not imply zero candidates.
    """
    candidate_rows = [
        row for row in run_rows
        if row.get("history_coverage") in ("returned_count_differs_from_reported", "count_unknown")
    ]
    candidate_keys = {
        (str(row.get("project") or ""), str(row.get("run_id") or ""))
        for row in candidate_rows
        if row.get("project") and row.get("run_id")
    }
    candidate_coverage = {
        (str(value.get("project") or ""), str(value.get("run_id") or "")): value
        for value in coverage.values()
        if (str(value.get("project") or ""), str(value.get("run_id") or "")) in candidate_keys
    }
    scanned_keys = set(candidate_coverage)
    matched_keys = {
        key for key, value in candidate_coverage.items()
        if value.get("all_ranges_and_row_count_match")
    }

    requested_ranges: set[tuple[str, str, int, int]] = set()
    for entry in entries:
        scan = entry.get("history_scan") or {}
        key = (str(scan.get("project") or ""), str(scan.get("run_id") or ""))
        if key not in candidate_keys:
            continue
        for window in scan.get("windows", []):
            low, high = window.get("min_step"), window.get("max_step")
            if type(low) is int and type(high) is int:
                requested_ranges.add((*key, low, high))

    unresolved = []
    for row in candidate_rows:
        key = (str(row.get("project") or ""), str(row.get("run_id") or ""))
        if key in matched_keys:
            continue
        value = candidate_coverage.get(key)
        unresolved.append({
            "project": key[0],
            "run_id": key[1],
            "history_line_count": row.get("history_line_count"),
            "last_step": (value or {}).get("last_step"),
            "scan_skip_reason": None if value else "no archived full-history scan",
            "returned_ranges": (value or {}).get("step_ranges_returned", 0),
            "expected_ranges": (value or {}).get("expected_step_ranges"),
            "all_expected_step_ranges_returned": (value or {}).get("all_expected_step_ranges_returned", False),
            "unique_rows_match_reported_line_count": (value or {}).get("unique_rows_match_reported_line_count", False),
        })

    return {
        "wandb_full_history_candidate_runs": len(candidate_keys),
        "wandb_full_history_scanned_candidates": len(scanned_keys),
        "wandb_full_history_expected_ranges": sum(
            value.get("expected_step_ranges", 0) for value in candidate_coverage.values()
        ),
        "wandb_full_history_requested_ranges": len(requested_ranges),
        "wandb_full_history_returned_ranges": sum(
            value.get("step_ranges_returned", 0) for value in candidate_coverage.values()
        ),
        "wandb_full_history_ranges_returned_all": sum(
            bool(value.get("all_expected_step_ranges_returned")) for value in candidate_coverage.values()
        ),
        "wandb_full_history_row_count_matches": sum(
            bool(value.get("unique_rows_match_reported_line_count")) for value in candidate_coverage.values()
        ),
        "wandb_full_history_ranges_and_count_match": len(matched_keys),
        "wandb_full_history_unresolved_runs": len(unresolved),
        "wandb_full_history_unresolved_plan_runs": unresolved,
        "wandb_full_history_rows": sum(value.get("unique_row_fingerprints", 0) for value in candidate_coverage.values()),
    }


def iter_full_history(output: Path, entries: list[dict[str, Any]], coverage: dict[str, dict[str, Any]]):
    ordered = [entry for entry in entries if entry.get("history_scan")]
    ordered.sort(
        key=lambda entry: (
            entry["history_scan"]["project"],
            entry["history_scan"]["run_id"],
            min((window["min_step"] for window in entry["history_scan"].get("windows", [])), default=0),
        )
    )
    current_run: tuple[str, str] | None = None
    seen: set[str] = set()
    row_index = 0
    for entry in ordered:
        scan = entry["history_scan"]
        run_key = (scan["project"], scan["run_id"])
        if run_key != current_run:
            current_run = run_key
            seen = set()
            row_index = 0
        if entry.get("status") != 200 or entry.get("application_status"):
            continue
        doc = raw_doc(output, entry["path"])
        for window in sorted(scan.get("windows", []), key=lambda item: item["min_step"]):
            alias_returned, rows = window_response_rows(output, entry, window, doc=doc)
            if not alias_returned:
                continue
            for raw_row in rows:
                metrics = decode_history_row(raw_row)
                if metrics is None:
                    continue
                fingerprint = sha256(json.dumps(metrics, sort_keys=True, separators=(",", ":")).encode())
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                run_coverage = coverage.get(f"{scan['project']}/{scan['run_id']}", {})
                reported = scan.get("history_line_count")
                yield {
                    "entity": WANDB_ENTITY,
                    "project": scan["project"],
                    "run_id": scan["run_id"],
                    "row_index": row_index,
                    "history_line_count": reported,
                    "last_step": scan.get("source_last_step", scan.get("last_step")),
                    "window_min_step": window["min_step"],
                    "window_max_step": window["max_step"],
                    "all_ranges_and_row_count_match": run_coverage.get("all_ranges_and_row_count_match", False),
                    "raw_ref": entry.get("path"),
                    "metrics": metrics,
                }
                row_index += 1


def write_gzip_csv(path: Path, fieldnames: list[str], rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as compressed:
            with io.TextIOWrapper(compressed, newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)


def write_gzip_jsonl(path: Path, rows) -> None:
    """Stream sparse JSON rows so wide projects do not create giant padded CSVs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=6, mtime=0) as compressed:
            with io.TextIOWrapper(compressed, newline="", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
                    handle.write("\n")


def normalize(output: Path) -> dict[str, Any]:
    entries = manifest_doc(output).get("entries", [])
    verify_raw_manifest(output)
    run_rows: list[dict[str, Any]] = []
    strategy_rows: list[dict[str, Any]] = []
    strategy_by_identity: dict[str, dict[str, Any]] = {}
    strategy_keys: set[str] = set()
    eval_rows: list[dict[str, Any]] = []
    counts: dict[str, dict[str, int]] = {}
    project_run_counts: dict[str, int | None] = {}
    project_pages: dict[str, list[dict[str, Any]]] = {}
    project_seen_runs: dict[str, set[str]] = {}

    for entry in entries:
        if entry.get("status") != 200 or not entry.get("path"):
            continue
        rel = entry["path"]
        if "/wandb/" in rel:
            doc = raw_doc(output, rel)
            if not isinstance(doc, dict) or doc.get("errors"):
                continue
            try:
                project_doc = doc["data"]["project"]
                project = project_doc["runs"]
            except (KeyError, TypeError):
                continue
            project_name = Path(rel).parent.name
            project_run_counts[project_name] = project_doc.get("runCount")
            project_pages.setdefault(project_name, []).append(
                {
                    "path": rel,
                    "has_next_page": project.get("pageInfo", {}).get("hasNextPage"),
                    "end_cursor": project.get("pageInfo", {}).get("endCursor"),
                }
            )
            project_seen_runs.setdefault(project_name, set())
            for edge in project.get("edges", []):
                node = edge.get("node", {})
                run_id = str(node.get("name") or "")
                if run_id in project_seen_runs[project_name]:
                    continue
                if run_id:
                    project_seen_runs[project_name].add(run_id)
                hist = node.get("history") or []
                reported_raw = node.get("historyLineCount")
                reported = reported_raw if type(reported_raw) is int and reported_raw >= 0 else None
                count_matches = reported is not None and reported > 0 and len(hist) == reported
                if reported is None:
                    coverage = "count_unknown"
                elif reported == 0:
                    coverage = "zero_reported"
                elif count_matches:
                    coverage = "count_matches_reported"
                else:
                    coverage = "returned_count_differs_from_reported"
                row = {
                    "entity": WANDB_ENTITY,
                    "project": project_name,
                    "run_id": run_id,
                    "display_name": node.get("displayName"),
                    "state": node.get("state"),
                    "created_at": node.get("createdAt"),
                    "heartbeat_at": node.get("heartbeatAt"),
                    "history_line_count": reported_raw,
                    "returned_history_rows": len(hist),
                    "history_count_matches_reported": count_matches,
                    "history_coverage": coverage,
                    "raw_ref": rel,
                }
                run_rows.append(row)
                stats = counts.setdefault(row["project"], {"runs": 0, "history_lines_reported": 0, "history_rows_returned": 0, "count_matches_reported": 0, "count_differs": 0, "count_unknown": 0, "zero_reported": 0})
                stats["runs"] += 1
                if reported is not None:
                    stats["history_lines_reported"] += reported
                stats["history_rows_returned"] += len(hist)
                stats[{
                    "count_matches_reported": "count_matches_reported",
                    "returned_count_differs_from_reported": "count_differs",
                    "count_unknown": "count_unknown",
                    "zero_reported": "zero_reported",
                }[coverage]] += 1
        elif rel == "raw/mech1/history.json":
            doc = raw_doc(output, rel)
            if isinstance(doc, dict):
                for row in doc.get("history", []):
                    eval_rows.append({"raw_ref": rel, **row})
        elif rel.startswith("raw/mech1/strategies-block-"):
            doc = raw_doc(output, rel)
            if not isinstance(doc, dict):
                continue
            block_hint = None
            match = re.search(r"strategies-block-(\d+)", rel)
            if match:
                block_hint = int(match.group(1))
            for row in doc.get("strategies", []):
                out = {"block": block_hint, "raw_ref": rel, **row}
                identity = json.dumps(
                    {key: value for key, value in out.items() if key != "raw_ref"},
                    sort_keys=True,
                    separators=(",", ":"),
                )
                existing = strategy_by_identity.get(identity)
                if existing is None:
                    out["raw_refs"] = [rel]
                    strategy_by_identity[identity] = out
                    strategy_rows.append(out)
                elif rel not in existing["raw_refs"]:
                    existing["raw_refs"].append(rel)
                strategy_keys.update(out)

    run_fields = ["entity", "project", "run_id", "display_name", "state", "created_at", "heartbeat_at", "history_line_count", "returned_history_rows", "history_count_matches_reported", "history_coverage", "raw_ref"]
    write_csv(output / "normalized/wandb_runs.csv", run_fields, run_rows)
    write_gzip_jsonl(output / "normalized/wandb_history_sampled.jsonl.gz", iter_sampled_history(output, entries))
    full_coverage = full_history_coverage(output, entries)
    write_gzip_jsonl(
        output / "normalized/wandb_history_full.jsonl.gz",
        iter_full_history(output, entries, full_coverage),
    )
    (output / "wandb_full_history_coverage.json").write_text(
        json.dumps(full_coverage, indent=2, sort_keys=True) + "\n"
    )
    write_csv(output / "normalized/mech1_eval_history.csv", ["raw_ref", "block", "best_loss", "best_communication", "best_throughput", "miner_count"], eval_rows)
    strategy_fields = ["block", "raw_ref", "raw_refs", *sorted(strategy_keys - {"block", "raw_ref", "raw_refs"})]
    write_csv(output / "normalized/mech1_strategies.csv", strategy_fields, strategy_rows)

    wandb_project_coverage = {}
    wandb_page_sample_counts: dict[str, dict[str, dict[str, int]]] = {}
    for project_name in WANDB_PROJECTS:
        pages = sorted(project_pages.get(project_name, []), key=lambda row: row["path"])
        reported_runs = project_run_counts.get(project_name)
        captured_runs = len(project_seen_runs.get(project_name, set()))
        terminated = bool(pages) and pages[-1]["has_next_page"] is False
        wandb_project_coverage[project_name] = {
            "reported_run_count": reported_runs,
            "distinct_run_ids_captured": captured_runs,
            "pages_captured": len(pages),
            "final_page_has_next_page": pages[-1]["has_next_page"] if pages else None,
            "pagination_terminated": terminated,
            "run_count_matches": type(reported_runs) is int and captured_runs == reported_runs,
            "coverage_complete_by_public_run_count": type(reported_runs) is int and captured_runs == reported_runs and terminated,
            "page_refs": [row["path"] for row in pages],
        }
        requested_samples: dict[str, int] = {}
        successful_samples: dict[str, int] = {}
        request_prefix = f"wandb-{project_name}-page-"
        for entry in entries:
            if not str(entry.get("id", "")).startswith(request_prefix):
                continue
            match = re.search(r"history\(samples:(\d+)\)", entry.get("request_body") or "")
            if not match:
                continue
            size = match.group(1)
            requested_samples[size] = requested_samples.get(size, 0) + 1
            if entry.get("status") == 200:
                successful_samples[size] = successful_samples.get(size, 0) + 1
        wandb_page_sample_counts[project_name] = {
            "requests_by_samples": requested_samples,
            "successful_pages_by_samples": successful_samples,
        }
    excluded_history_responses = []
    entry_by_id = {entry.get("id"): entry for entry in entries}
    for entry in entries:
        if entry.get("application_status") != "excluded" or "-page-" not in str(entry.get("id", "")):
            continue
        request_id = str(entry.get("id", ""))
        base_id = re.sub(r"-samples-\d+$", "", request_id)
        match = re.search(r"wandb-([^-]+(?:_[^-]+)*)-page-(\d+)", base_id)
        if match:
            project_name, page_number = match.group(1), int(match.group(2))
        else:
            project_name, page_number = None, None
        metadata_entry = entry_by_id.get(base_id + "-metadata-only")
        withheld_run_ids = list(entry.get("excluded_runs", []))
        if metadata_entry and metadata_entry.get("status") == 200 and metadata_entry.get("path"):
            metadata_doc = raw_doc(output, metadata_entry["path"])
            try:
                metadata_edges = metadata_doc["data"]["project"]["runs"]["edges"]
                withheld_run_ids = [
                    str(edge.get("node", {}).get("name"))
                    for edge in metadata_edges
                    if edge.get("node", {}).get("name") is not None
                ]
            except (KeyError, TypeError):
                pass
        excluded_history_responses.append({
            "request_id": request_id,
            "project": project_name,
            "page": page_number,
            "run_ids": sorted(set(withheld_run_ids)),
            "metadata_only_recovered": bool(metadata_entry and metadata_entry.get("status") == 200 and metadata_entry.get("path")),
            "key_name_screen_reason": entry.get("error"),
        })
    full_history_summary = summarize_full_history(run_rows, full_coverage, entries)
    summary = {
        "snapshot": output.name,
        "generated_utc": utc_now(),
        "valid_raw_entries": len(verify_raw_manifest(output)),
        "failed_entries": [
            *[
                {"id": entry.get("id"), "url": entry.get("url"), "status": entry.get("status"), "application_status": entry.get("application_status"), "error": entry.get("error") or entry.get("query_errors")}
                for entry in entries if entry.get("status") != 200 or entry.get("application_status")
            ],
            *[
                {"id": entry.get("id"), "url": entry.get("url"), "status": attempt.get("status"), "application_status": attempt.get("application_status"), "error": attempt.get("error") or attempt.get("query_errors"), "prior_attempt": True}
                for entry in entries for attempt in entry.get("attempts", [])
                if attempt.get("status") != 200 or attempt.get("application_status")
            ],
        ],
        "wandb_by_project": counts,
        "wandb_project_coverage": wandb_project_coverage,
        "wandb_page_sample_counts": wandb_page_sample_counts,
        "wandb_excluded_history_responses": excluded_history_responses,
        "wandb_full_history_scans": len(full_coverage),
        **full_history_summary,
        "wandb_history_sampling": "GraphQL Run.history uses a 5,000-row request for miners and DeMoCo; validators use adaptive 5,000/100/10-row requests after response-cap failures. Returned row counts are compared with historyLineCount, but count equality is not treated as proof of full history completeness.",
        "mech1_eval_rows": len(eval_rows),
        "mech1_strategy_rows": len(strategy_rows),
        "mech1_strategy_blocks": sorted({row.get("block") for row in strategy_rows if row.get("block") is not None}),
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    report = render_report(summary)
    (output / "REPORT.md").write_text(report)
    return summary


def render_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Public Distributed Training run snapshot",
        "",
        f"Snapshot: `{summary['snapshot']}`  ",
        f"Generated (UTC): `{summary['generated_utc']}`",
        "",
        "This snapshot contains public dashboard and W&B evidence. Run configuration objects were omitted because no safe projection for them was verified. Model, tokenizer, and dataset details below come from upstream repository defaults and code, plus the public dashboard’s linked model card; they are not asserted as settings for any individual historical run.",
        "",
        "## W&B coverage",
        "",
        "| Project | Public runCount | Distinct runs captured | Pages | Pagination ended | Count complete |",
        "|---|---:|---:|---:|---|---|",
    ]
    for project, coverage in sorted(summary["wandb_project_coverage"].items()):
        lines.append(
            f"| `{project}` | {coverage['reported_run_count']} | {coverage['distinct_run_ids_captured']} | {coverage['pages_captured']} | {coverage['pagination_terminated']} | {coverage['coverage_complete_by_public_run_count']} |"
        )
    lines.extend(
        [
        "",
        "| Project | Runs captured | History lines reported | Rows retrieved | Count matches | Count differs | Count unknown/invalid | Reported zero |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for project, values in sorted(summary["wandb_by_project"].items()):
        lines.append(
            f"| `{project}` | {values['runs']} | {values['history_lines_reported']} | {values['history_rows_returned']} | {values['count_matches_reported']} | {values['count_differs']} | {values['count_unknown']} | {values['zero_reported']} |"
        )
    if summary.get("wandb_page_sample_counts"):
        lines.extend(["", "Run-page history sample sizes (requests / successful pages):"])
        for project, samples in sorted(summary["wandb_page_sample_counts"].items()):
            requested = samples["requests_by_samples"]
            successful = samples["successful_pages_by_samples"]
            sizes = sorted(set(requested) | set(successful), key=int)
            counts_text = ", ".join(
                f"{size}: {requested.get(size, 0)} / {successful.get(size, 0)}" for size in sizes
            ) or "none"
            lines.append(f"- `{project}`: {counts_text}")
    lines.extend(
        [
            "",
            f"{summary['wandb_history_sampling']} The sparse JSONL files preserve each run's metric dictionary; returned row counts are compared with W&B `historyLineCount`, but equality is not proof that W&B returned every original row. Training loss is each miner run's own logged loss, not a single shared model's global convergence curve.",
            "",
            "## Unsampled step-window scans",
            "",
            f"- Candidate runs with sampled count mismatch or unknown count: {summary['wandb_full_history_candidate_runs']}",
            f"- Candidates with an archived step-window scan: {summary['wandb_full_history_scanned_candidates']}",
            f"- Expected ranges implied by verified `lastStep` values for scanned candidates: {summary['wandb_full_history_expected_ranges']}",
            f"- Distinct requested ranges recorded in the raw manifest: {summary['wandb_full_history_requested_ranges']}",
            f"- Returned ranges: {summary['wandb_full_history_returned_ranges']}",
            f"- Scanned candidates with all expected ranges returned: {summary['wandb_full_history_ranges_returned_all']} / {summary['wandb_full_history_scanned_candidates']}",
            f"- Scanned candidates whose unique window rows match W&B `historyLineCount`: {summary['wandb_full_history_row_count_matches']}",
            f"- Scanned candidates with all ranges returned and row count matching: {summary['wandb_full_history_ranges_and_count_match']}",
            f"- Candidates unresolved or not fully validated: {summary['wandb_full_history_unresolved_runs']}",
            "- A step-window scan is recorded as observed ranges and row counts; it is not independently verified against the training service’s complete event store.",
            "",
            "## Dashboard coverage",
            "",
            f"- Mechanism 1 evaluation summary rows: {summary['mech1_eval_rows']}",
            f"- Mechanism 1 strategy observations: {summary['mech1_strategy_rows']}",
            f"- Mechanism 1 blocks with strategy records: {', '.join(map(str, summary['mech1_strategy_blocks'])) or 'none'}",
            "- Strategy records preserve the dashboard’s `is_benchmark` flag. Benchmark results must be analyzed separately from miner strategies.",
            "- `best_loss`, `best_communication`, and `best_throughput` are dashboard-selected best values for a block; they are not a single model’s convergence trace.",
            "",
            "## Recorded request failures and exclusions",
            "",
            "The manifest retains failed attempts, including response-cap requests that may have succeeded through a smaller query variant and early transport failures that may later have succeeded. These records are not, by themselves, unresolved source gaps; assess remaining gaps from the current successful pages and the coverage counts above.",
            f"A conservative key-name screen withheld {len(summary.get('wandb_excluded_history_responses', []))} W&B history page response(s). A key-name match does not establish that the matched value is a credential; withheld responses were not archived. Safe metadata-only query results recover public run identity where available.",
            "",
        ]
    )
    for excluded in summary.get("wandb_excluded_history_responses", []):
        ids = ", ".join(f"`{run_id}`" for run_id in excluded["run_ids"]) or "run ID unavailable"
        lines.append(
            f"- Withheld `{excluded['project']}` page {excluded['page']} history for {ids}; "
            f"metadata-only identity captured: {excluded['metadata_only_recovered']}."
        )
    if summary["failed_entries"]:
        for entry in summary["failed_entries"]:
            lines.append(f"- `{entry['id']}`: HTTP status `{entry['status']}`; {entry.get('error') or 'not collected'} ({entry.get('url')})")
    else:
        lines.append("- No failed source requests recorded.")
    lines.extend(
        [
            "- W&B run configs were not queried; the collector uses a narrow allowlist of run identity, state, timestamps, history counts, and metric history. No safe projection for config objects was verified.",
            "- Public dashboard `Mechanism 0` is a separate iframe; any unavailable status is recorded in the snapshot manifest.",
            "- Model weights, dataset contents, and blockchain data were not downloaded.",
            "",
            "## Files",
            "",
            "- `manifest.json`: request provenance and SHA-256 for each raw response.",
            "- `raw/`: immutable responses to public GET requests and safe W&B GraphQL queries.",
            "- `normalized/wandb_history_sampled.jsonl.gz`: streamed sparse sampled histories, one row per run-history record with metrics nested under `metrics`.",
            "- `normalized/wandb_history_full.jsonl.gz`: streamed sparse public step-window histories for sampled histories that were truncated or had unknown counts.",
            "- `wandb_full_history_coverage.json`: requested ranges, returned rows, duplicate rows, and count comparisons for full-history scans.",
            "- `normalized/`: CSV run records, metric histories, and per-block dashboard observations.",
            "- `summary.json`: machine-readable counts and coverage.",
            "",
        ]
    )
    return "\n".join(lines)


def write_project_inventory(output: Path) -> None:
    """Write compact tracked request provenance outside the ignored data snapshot."""
    project_dir = Path(__file__).resolve().parent
    manifest_path = output / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    output_ref = os.path.relpath(output.resolve(), project_dir)
    inventory = {
        "snapshot": output_ref,
        "manifest_path": f"{output_ref}/manifest.json",
        "manifest_sha256": sha256(manifest_bytes),
        "entries": [
            {
                "id": entry.get("id"),
                "path": entry.get("path"),
                "url": entry.get("url"),
                "status": entry.get("status"),
                "application_status": entry.get("application_status"),
                "sha256": entry.get("sha256"),
                "bytes": entry.get("bytes"),
                "content_encoding": entry.get("content_encoding"),
                "decoded_bytes": entry.get("decoded_bytes"),
                "retrieved_at": entry.get("retrieved_at"),
                "error": entry.get("error"),
                "attempts": [
                    {
                        "status": attempt.get("status"),
                        "bytes": attempt.get("bytes"),
                        "retrieved_at": attempt.get("retrieved_at"),
                        "error": attempt.get("error"),
                        "application_status": attempt.get("application_status"),
                    }
                    for attempt in entry.get("attempts", [])
                ],
            }
            for entry in manifest.get("entries", [])
        ],
    }
    requests = {
        "snapshot": output_ref,
        "manifest_sha256": inventory["manifest_sha256"],
        "requests": [
            {
                "id": entry.get("id"),
                "method": entry.get("method"),
                "url": entry.get("url"),
                "request_body": entry.get("request_body"),
            }
            for entry in manifest.get("entries", [])
        ],
    }
    (project_dir / "INVENTORY.json").write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
    (project_dir / "REQUESTS.json").write_text(json.dumps(requests, indent=2, sort_keys=True) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Snapshot directory")
    parser.add_argument("--max-bytes", type=int, required=True, help="Maximum bytes downloaded into raw/")
    parser.add_argument("--resume", action="store_true", help="Reuse raw responses whose manifest hash verifies")
    parser.add_argument("--analyze-only", action="store_true", help="Regenerate normalized CSV and report offline")
    parser.add_argument("--batch-size", type=int, default=WANDB_BATCH_SIZE, help="Runs per W&B history request (default: 20)")
    args = parser.parse_args(argv)
    if args.max_bytes <= 0:
        parser.error("--max-bytes must be positive")
    if not (1 <= args.batch_size <= 50):
        parser.error("--batch-size must be between 1 and 50")
    args.output = args.output.resolve()
    try:
        collector = Collector(args.output, args.max_bytes, args.resume or args.analyze_only)
        if args.analyze_only:
            summary = normalize(args.output)
            write_project_inventory(args.output)
            print(json.dumps(summary, indent=2))
            return 0
        collector.save_manifest(started_utc=utc_now(), collection_status="running")
        collect_dashboard(collector)
        collect_huggingface(collector)
        collect_wandb(collector, args.batch_size)
        collect_wandb_full_history(collector)
        summary = normalize(args.output)
        collector.save_manifest(
            finished_utc=utc_now(),
            collection_status="complete_with_source_gaps" if summary["failed_entries"] else "complete",
            summary_path="summary.json",
        )
        write_project_inventory(args.output)
        print(json.dumps(summary, indent=2))
        return 0
    except Exception as exc:
        try:
            collector.save_manifest(finished_utc=utc_now(), collection_status="failed", fatal_error=f"{type(exc).__name__}: {exc}")
            if (args.output / "manifest.json").is_file():
                write_project_inventory(args.output)
        except (UnboundLocalError, NameError):
            pass
        print(f"collection failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
