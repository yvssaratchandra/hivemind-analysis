#!/usr/bin/env python3
"""Collect public Node0 run evidence from the Pluralis dashboard APIs.

The collector is deliberately read-only: it fetches public HTTP GET endpoints,
keeps the response bodies immutable under ``raw/``, and writes derived CSVs.
It never joins a swarm, triggers a refresh endpoint, or downloads model weights.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import email.utils
import gzip
import hashlib
import http.client
import io
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
REPOSITORY = "https://github.com/PluralisResearch/node0"
REGISTRY_URL = "https://agora.pluralis.ai/api/runs"
NODE0_DASHBOARD_URL = "https://dashboard.pluralis.ai/"
API_BASE = "https://agora.pluralis.ai/api/runs"
DEFAULT_BUDGET = 4 * 1024**3
USER_AGENT = "hivemind-analysis-public-research/1.0 (read-only; Node0 history export)"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def safe_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value).strip("._") or "response"


def url_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def read_manifest(path: Path, max_bytes: int) -> dict[str, Any]:
    manifest_path = path / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest.get("entries"), list):
            raise ValueError(f"Invalid manifest entries in {manifest_path}")
        manifest["max_bytes"] = max_bytes
        return manifest
    return {"schema_version": 1, "created_at": utc_now(), "max_bytes": max_bytes, "entries": []}


def save_manifest(output: Path, manifest: dict[str, Any]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    manifest["updated_at"] = utc_now()
    temp_path = output / "manifest.json.tmp"
    temp_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_path.replace(output / "manifest.json")


def _entry_for_url(manifest: dict[str, Any], url: str) -> dict[str, Any] | None:
    for entry in reversed(manifest["entries"]):
        if entry.get("url") == url:
            return entry
    return None


def _raw_bytes(output: Path, manifest: dict[str, Any]) -> int:
    total = 0
    for entry in manifest["entries"]:
        rel = entry.get("path")
        if entry.get("status") == 200 and rel:
            file_path = output / rel
            if file_path.is_file():
                total += file_path.stat().st_size
    return total


class PublicFetcher:
    """Small bounded GET client with per-host pacing and resumable raw files."""

    def __init__(self, output: Path, manifest: dict[str, Any], max_bytes: int, resume: bool):
        self.output = output
        self.manifest = manifest
        self.max_bytes = max_bytes
        self.resume = resume
        self.last_request: dict[str, float] = {}

    def _pace(self, url: str) -> None:
        host = urllib.parse.urlsplit(url).netloc
        now = time.monotonic()
        delay = 1.0 - (now - self.last_request.get(host, -100.0))
        if delay > 0:
            time.sleep(delay)
        self.last_request[host] = time.monotonic()

    def get(self, key: str, url: str) -> bytes | None:
        prior = _entry_for_url(self.manifest, url)
        successful = next((e for e in reversed(self.manifest["entries"]) if e.get("url") == url and e.get("status") == 200), None)
        if successful:
            file_path = self.output / str(successful.get("path", ""))
            if not file_path.is_file():
                raise ValueError(f"Successful raw evidence is missing for {url}: {file_path}")
            content = file_path.read_bytes()
            digest = hashlib.sha256(content).hexdigest()
            if len(content) != successful.get("bytes") or digest != successful.get("sha256"):
                raise ValueError(f"Successful raw evidence failed size/hash verification for {url}: {file_path}")
            if self.resume:
                return content
            raise ValueError(f"Refusing to overwrite immutable successful raw evidence for {url}; choose a new --output or use --resume")

        entry: dict[str, Any] = {
            "key": key,
            "url": url,
            "path": None,
            "status": None,
            "sha256": None,
            "bytes": 0,
            "retrieved_at": None,
            "content_type": None,
            "error": None,
        }
        raw_dir = self.output / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        target = raw_dir / f"{safe_name(key)}_{url_key(url)[:16]}.body"
        partial = target.with_suffix(target.suffix + ".partial")
        if target.exists():
            raise ValueError(f"Refusing to overwrite untracked raw evidence at {target}; inspect the snapshot or choose a new --output")
        partial.unlink(missing_ok=True)

        attempts = 0
        content: bytes | None = None
        attempt_history: list[dict[str, Any]] = []
        while attempts < 3:
            attempts += 1
            self._pace(url)
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json,*/*;q=0.5"})
            try:
                with urllib.request.urlopen(request, timeout=45) as response:
                    status = int(response.status)
                    attempt_history.append({"attempt": attempts, "status": status, "at": utc_now()})
                    entry["status"] = status
                    entry["content_type"] = response.headers.get("Content-Type")
                    h = hashlib.sha256()
                    total = 0
                    with partial.open("wb") as stream:
                        while True:
                            chunk = response.read(1024 * 1024)
                            if not chunk:
                                break
                            remaining = self.max_bytes - _raw_bytes(self.output, self.manifest) - total
                            if len(chunk) > remaining:
                                stream.close()
                                partial.unlink(missing_ok=True)
                                entry["status"] = "skipped_budget"
                                entry["error"] = f"response exceeds max_bytes={self.max_bytes}"
                                entry["retrieved_at"] = utc_now()
                                break
                            stream.write(chunk)
                            h.update(chunk)
                            total += len(chunk)
                    if entry["status"] == "skipped_budget":
                        break
                    partial.replace(target)
                    entry["path"] = str(target.relative_to(self.output))
                    entry["bytes"] = total
                    entry["sha256"] = h.hexdigest()
                    entry["retrieved_at"] = utc_now()
                    entry["error"] = None
                    content = target.read_bytes()
                    break
            except urllib.error.HTTPError as exc:
                status = int(exc.code)
                retry_after_header = exc.headers.get("Retry-After") if exc.headers else None
                attempt_history.append({"attempt": attempts, "status": status, "at": utc_now(), "retry_after": retry_after_header})
                entry["status"] = int(exc.code)
                entry["content_type"] = exc.headers.get("Content-Type") if exc.headers else None
                entry["error"] = f"HTTP {exc.code}"
                entry["retrieved_at"] = utc_now()
                if (exc.code == 429 or retry_after_header) and attempts < 3:
                    retry_after = retry_after_header or "1"
                    try:
                        wait = max(0.0, float(retry_after))
                    except ValueError:
                        try:
                            retry_time = email.utils.parsedate_to_datetime(retry_after)
                            if retry_time.tzinfo is None:
                                retry_time = retry_time.replace(tzinfo=dt.timezone.utc)
                            wait = max(0.0, (retry_time - dt.datetime.now(dt.timezone.utc)).total_seconds())
                        except (TypeError, ValueError, OverflowError):
                            wait = 1.0
                    if wait > 30.0:
                        entry["error"] = f"HTTP {exc.code}; Retry-After {wait:.1f}s exceeds bounded wait; deferred without early retry"
                        content = None
                        exc.close()
                        break
                    exc.close()
                    time.sleep(wait)
                    continue
                if exc.code >= 500 and attempts < 3:
                    exc.close()
                    time.sleep(attempts)
                    continue
                exc.close()
                content = None
                break
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                partial.unlink(missing_ok=True)
                attempt_history.append({"attempt": attempts, "status": "network_error", "at": utc_now(), "error": f"{type(exc).__name__}: {exc}"})
                entry["status"] = "network_error"
                entry["error"] = f"{type(exc).__name__}: {exc}"
                entry["retrieved_at"] = utc_now()
                if attempts < 3:
                    time.sleep(attempts)
                    continue
                content = None
                break
        else:
            content = None

        entry["attempts"] = attempts
        entry["attempt_history"] = attempt_history
        self.manifest["entries"].append(entry)
        save_manifest(self.output, self.manifest)
        return content


def parse_json(content: bytes | None, description: str) -> Any | None:
    if content is None:
        return None
    try:
        return json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not parse JSON response for {description}: {exc}") from exc


def verify_successful_raw(output: Path, manifest: dict[str, Any]) -> None:
    for entry in manifest.get("entries", []):
        if entry.get("status") != 200:
            continue
        rel = entry.get("path")
        if not rel:
            raise ValueError(f"Successful response has no raw path: {entry.get('url')}")
        file_path = output / rel
        if not file_path.is_file():
            raise ValueError(f"Successful raw evidence is missing: {file_path}")
        content = file_path.read_bytes()
        if len(content) != entry.get("bytes") or hashlib.sha256(content).hexdigest() != entry.get("sha256"):
            raise ValueError(f"Successful raw evidence failed size/hash verification: {file_path}")


def extract_runs(registry: Any) -> list[dict[str, Any]]:
    if isinstance(registry, dict):
        runs = registry.get("runs", [])
    else:
        runs = registry
    return [row for row in runs if isinstance(row, dict)] if isinstance(runs, list) else []


def find_node0_run(registry: Any) -> dict[str, Any] | None:
    runs = extract_runs(registry)
    for run in runs:
        if str(run.get("id", "")).lower() == "node0-7-5b":
            return run
    for run in runs:
        label = str(run.get("label", "")).lower()
        model = str((run.get("meta") or {}).get("model", "")).lower()
        if "node0" in label and ("7.5" in label or "7.5" in model or "olmo2" in model):
            return run
    return None


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".gz":
        with path.open("wb") as binary_stream:
            with gzip.GzipFile(filename="", mode="wb", fileobj=binary_stream, mtime=0) as compressed:
                text_stream = io.TextIOWrapper(compressed, newline="", encoding="utf-8")
                writer = csv.DictWriter(text_stream, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                for row in rows:
                    writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})
                text_stream.flush()
                text_stream.detach()
        return
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return value


def _peer_rows(payload: Any, run_id: str, raw_ref: str) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        peers = payload.get("peers", payload.get("nodes", payload.get("items", [])))
    else:
        peers = payload
    if not isinstance(peers, list):
        return []
    rows = []
    for index, peer in enumerate(peers):
        if not isinstance(peer, dict):
            continue
        rows.append({
            "run_id": run_id,
            "record_index": index,
            "peer_id": peer.get("peer_id", peer.get("id")),
            "identity_kind": "peer_id" if peer.get("peer_id") or peer.get("id") else ("name" if peer.get("name") else "unknown"),
            "name": peer.get("name"),
            "username": peer.get("username"),
            "is_pluralis": peer.get("is_pluralis"),
            "status": peer.get("status"),
            "is_contributing": peer.get("is_contributing"),
            "is_warming_up": peer.get("is_warming_up"),
            "flop_pflop": peer.get("flop_pflop"),
            "device": peer.get("device"),
            "stage": peer.get("stage_raw", peer.get("stage")),
            "location": peer.get("location"),
            "city": peer.get("city"),
            "cloud_provider": peer.get("cloud_provider"),
            "active_time": peer.get("active_time"),
            "bandwidth_up": peer.get("bandwidth_up"),
            "bandwidth_down": peer.get("bandwidth_down"),
            "latency": peer.get("latency"),
            "joined_at": peer.get("joined_at", peer.get("joined")),
            "last_active_at": peer.get("last_active_at"),
            "sync_phase": peer.get("sync_phase"),
            "sync_progress": peer.get("sync_progress"),
            "fields_json": peer,
            "raw_ref": raw_ref,
        })
    return rows


def _leaderboard_rows(payload: Any, run_id: str, raw_ref: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = None
        for key in ("leaderboard", "entries", "workers", "nodes", "results", "peers"):
            if isinstance(payload.get(key), list):
                items = payload[key]
                break
        if items is None and payload and all(isinstance(value, dict) for value in payload.values()):
            items = [dict(value, _key=key) for key, value in payload.items()]
        if items is None:
            items = []
    else:
        items = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        rows.append({
            "run_id": run_id,
            "record_index": index,
            "rank": item.get("rank", item.get("position")),
            "peer_id": item.get("peer_id", item.get("id", item.get("worker_id"))),
            "name": item.get("name", item.get("worker", item.get("_key"))),
            "username": item.get("username", item.get("user")),
            "score": item.get("score"),
            "active_time": item.get("active_time"),
            "status": item.get("status"),
            "joined": item.get("joined"),
            "fields_json": item,
            "raw_ref": raw_ref,
        })
    return rows


def _series_rows(payload: Any, run_id: str, raw_ref: str, kind: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return rows

    if kind == "training":
        series = payload.get("metrics", {})
        if isinstance(series, dict):
            for metric_name, points in series.items():
                if not isinstance(points, list):
                    continue
                for index, point in enumerate(points):
                    if not isinstance(point, dict):
                        continue
                    value = point.get("y", point.get("value"))
                    rows.append({
                        "run_id": run_id,
                        "source": kind,
                        "metric": metric_name,
                        "timestamp": point.get("timestamp"),
                        "step": point.get("step"),
                        "sample_index": index,
                        "worker": point.get("worker", point.get("name")),
                        "value": value,
                        "unit": "dashboard-defined; not inferred",
                        "fields_json": point,
                        "raw_ref": raw_ref,
                    })
        # Retain scalar and non-series progress fields as metadata observations.
        progress = payload.get("progress")
        if isinstance(progress, dict):
            for metric_name, value in progress.items():
                if isinstance(value, (int, float, str)) or value is None:
                    rows.append({
                        "run_id": run_id,
                        "source": kind,
                        "metric": f"progress.{metric_name}",
                        "timestamp": None,
                        "step": None,
                        "sample_index": None,
                        "worker": None,
                        "value": value,
                        "unit": "dashboard-defined; not inferred",
                        "fields_json": {"value": value, "run_start_epoch": payload.get("run_start_epoch")},
                        "raw_ref": raw_ref,
                    })
        return rows

    operations = payload.get("operations", payload)
    if not isinstance(operations, dict):
        return rows
    for metric_name, operation in operations.items():
        if not isinstance(operation, dict):
            continue
        timestamps = operation.get("timestamps", [])
        workers = operation.get("workers", [])
        values = operation.get("values", [])
        if isinstance(values, list) and values and isinstance(values[0], list):
            if not isinstance(workers, list) or len(values) != len(workers):
                raise ValueError(f"Misaligned worker/value matrix for metric {metric_name}: {len(workers) if isinstance(workers, list) else 'no workers'} workers, {len(values)} series")
            for worker_index, worker_values in enumerate(values):
                worker = workers[worker_index] if isinstance(workers, list) and worker_index < len(workers) else None
                if not isinstance(worker_values, list):
                    continue
                if not isinstance(timestamps, list) or len(worker_values) != len(timestamps):
                    raise ValueError(f"Misaligned timestamp/value series for metric {metric_name}, worker index {worker_index}")
                for sample_index, value in enumerate(worker_values):
                    if value is None:
                        continue
                    timestamp = timestamps[sample_index] if isinstance(timestamps, list) and sample_index < len(timestamps) else None
                    rows.append({
                        "run_id": run_id,
                        "source": kind,
                        "metric": metric_name,
                        "timestamp": timestamp,
                        "step": None,
                        "sample_index": sample_index,
                        "worker": worker,
                        "value": value,
                        "unit": "dashboard-defined; not inferred",
                        "fields_json": {"worker_index": worker_index, "sample_index": sample_index},
                        "raw_ref": raw_ref,
                    })
        elif isinstance(values, list):
            # Aggregate time series: one value per timestamp.
            if not isinstance(timestamps, list) or len(values) != len(timestamps):
                raise ValueError(f"Misaligned aggregate timestamp/value series for metric {metric_name}")
            for sample_index, value in enumerate(values):
                if value is None:
                    continue
                timestamp = timestamps[sample_index] if isinstance(timestamps, list) and sample_index < len(timestamps) else None
                rows.append({
                    "run_id": run_id,
                    "source": kind,
                    "metric": metric_name,
                    "timestamp": timestamp,
                    "step": None,
                    "sample_index": sample_index,
                    "worker": None,
                    "value": value,
                    "unit": "dashboard-defined; not inferred",
                    "fields_json": {"sample_index": sample_index},
                    "raw_ref": raw_ref,
                })
    return rows


NODE_FIELDS = ["run_id", "record_index", "peer_id", "identity_kind", "name", "username", "is_pluralis", "status", "is_contributing", "is_warming_up", "flop_pflop", "device", "stage", "location", "city", "cloud_provider", "active_time", "bandwidth_up", "bandwidth_down", "latency", "joined_at", "last_active_at", "sync_phase", "sync_progress", "fields_json", "raw_ref"]
LEADERBOARD_FIELDS = ["run_id", "record_index", "rank", "peer_id", "name", "username", "score", "active_time", "status", "joined", "fields_json", "raw_ref"]
METRIC_FIELDS = ["run_id", "source", "metric", "timestamp", "step", "sample_index", "worker", "value", "unit", "fields_json", "raw_ref"]


def _load_entry_json(output: Path, manifest: dict[str, Any], key: str) -> tuple[Any | None, str | None]:
    entry = next((e for e in manifest["entries"] if e.get("key") == key and e.get("status") == 200), None)
    if not entry or not entry.get("path"):
        return None, None
    path = output / entry["path"]
    if not path.is_file():
        return None, entry.get("path")
    try:
        return json.loads(path.read_text(encoding="utf-8")), entry.get("path")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, entry.get("path")


def _find_success(manifest: dict[str, Any], key: str) -> dict[str, Any] | None:
    return next((e for e in reversed(manifest["entries"]) if e.get("key") == key and e.get("status") == 200), None)


def analyze(output: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    verify_successful_raw(output, manifest)
    registry, registry_ref = _load_entry_json(output, manifest, "runs_registry")
    run = find_node0_run(registry) if registry is not None else None
    run_id = str((run or {}).get("id") or "node0-7-5b")

    peers_payload, peers_ref = _load_entry_json(output, manifest, "node0_peers")
    training_payload, training_ref = _load_entry_json(output, manifest, "node0_training")
    telemetry_payload, telemetry_ref = _load_entry_json(output, manifest, "node0_telemetry_workers")
    leaderboard_payload, leaderboard_ref = _load_entry_json(output, manifest, "node0_leaderboard")

    node_rows = _peer_rows(peers_payload, run_id, peers_ref or "")
    leaderboard_rows = _leaderboard_rows(leaderboard_payload, run_id, leaderboard_ref or "")
    metric_rows = []
    metric_rows += _series_rows(training_payload, run_id, training_ref or "", "training")
    metric_rows += _series_rows(telemetry_payload, run_id, telemetry_ref or "", "telemetry_workers")
    _write_csv(output / "nodes.csv", NODE_FIELDS, node_rows)
    _write_csv(output / "leaderboard.csv", LEADERBOARD_FIELDS, leaderboard_rows)
    _write_csv(output / "metrics.csv.gz", METRIC_FIELDS, metric_rows)

    model_meta = (run or {}).get("meta", {}) if isinstance((run or {}).get("meta", {}), dict) else {}
    metric_counts: dict[str, int] = {}
    metric_workers: dict[str, set[str]] = {}
    for row in metric_rows:
        label = f"{row['source']}:{row['metric']}"
        metric_counts[label] = metric_counts.get(label, 0) + 1
        if row.get("worker"):
            metric_workers.setdefault(label, set()).add(str(row["worker"]))

    train_losses = [row for row in metric_rows if row["source"] == "training" and row["metric"] in ("train_loss", "training_loss", "loss") and isinstance(row.get("value"), (int, float))]
    train_losses.sort(key=lambda row: (float(row["step"]) if row.get("step") is not None else math.inf, float(row["timestamp"]) if row.get("timestamp") is not None else math.inf))
    raw_entries = [e for e in manifest["entries"] if e.get("status") == 200]
    total_raw_bytes = sum(int(e.get("bytes", 0)) for e in raw_entries)
    retrieval_times = [e.get("retrieved_at") for e in manifest["entries"] if e.get("retrieved_at")]
    flop_values = [row["flop_pflop"] for row in node_rows if isinstance(row.get("flop_pflop"), (int, float))]
    leaderboard_scores = [row["score"] for row in leaderboard_rows if isinstance(row.get("score"), (int, float))]
    positive_flop_rows = sum(value > 0 for value in flop_values)
    peer_usernames = {str(row["username"]) for row in node_rows if row.get("username")}
    leaderboard_usernames = {str(row["username"]) for row in leaderboard_rows if row.get("username")}
    status_counts: dict[str, int] = {}
    for row in node_rows:
        label = str(row.get("status") or "missing")
        status_counts[label] = status_counts.get(label, 0) + 1

    report = [
        "# Node0 public run evidence",
        "",
        f"- Snapshot: `{output.name}`",
        f"- Snapshot created at: {manifest.get('created_at', 'unknown')}",
        f"- Latest response retrieved at: {max(retrieval_times) if retrieval_times else 'unknown'}",
        f"- Repository: {REPOSITORY}",
        f"- Public dashboard: {NODE0_DASHBOARD_URL}",
        f"- Shared history API: {REGISTRY_URL}",
        f"- Run ID: `{run_id}`",
        f"- Run label: {(run or {}).get('label', 'unavailable')}",
        f"- Status: {(run or {}).get('status', 'unavailable')}",
        f"- Published model: {model_meta.get('model', 'unavailable')}",
        f"- Published dataset label: {model_meta.get('dataset', 'unavailable')}",
        f"- Published `tokens` field: {model_meta.get('tokens', 'unavailable')} (the API does not establish that this is processed-token count)",
        f"- Published parameter count (billions): {model_meta.get('params_b', 'unavailable')}",
        f"- Published precision: {model_meta.get('precision', 'unavailable')}",
        f"- Peer records: {len(node_rows)}",
        f"- Distinct peer-table contributor usernames: {len(peer_usernames)}",
        f"- Leaderboard records: {len(leaderboard_rows)}",
        f"- Distinct leaderboard usernames: {len(leaderboard_usernames)}",
        f"- Non-null normalized metric observations: {len(metric_rows)}",
        f"- Raw response bytes saved: {total_raw_bytes}",
        "",
        "## Collection coverage",
        "",
    ]
    for entry in manifest["entries"]:
        report.append(f"- `{entry.get('key')}` — HTTP/status `{entry.get('status')}`, {entry.get('bytes', 0)} bytes; {entry.get('url')}")
        if entry.get("error"):
            report.append(f"  - Collection note: {entry['error']}")
    report.extend(["", "## Observations", ""])
    if run is None:
        report.append("The public run registry response did not contain the expected Node0 run. No run-specific model or performance claims are made.")
    else:
        caps = run.get("capabilities", {})
        report.append(f"The registry marks the run `{run_id}` as `{run.get('status', 'unknown')}`. Its advertised capability flags are `{json.dumps(caps, sort_keys=True)}`; no endpoint is inferred from a disabled capability.")
    if node_rows:
        present = [field for field in ("status", "is_contributing", "flop_pflop", "active_time", "stage", "device", "bandwidth_up", "bandwidth_down", "latency", "joined_at", "last_active_at") if any(row.get(field) is not None for row in node_rows)]
        contributing_nodes = sum(row.get("is_contributing") is True for row in node_rows)
        report.append(f"Peer history contains {len(node_rows)} rows; {contributing_nodes} rows have `is_contributing=true`. Available peer fields include: {', '.join(present) if present else 'no recognized fields'}. Names are not treated as cryptographic peer IDs unless the API provides an ID field.")
        report.append(f"At collection time the API summary was `{json.dumps({k: peers_payload.get(k) for k in ('active_nodes', 'contributing_nodes', 'total_nodes') if isinstance(peers_payload, dict) and k in peers_payload}, sort_keys=True)}`; returned row statuses were `{json.dumps(status_counts, sort_keys=True)}`. Because this was a finished run, current offline/`is_contributing` flags do not mean those nodes never contributed historically.")
        if flop_values:
            report.append(f"The peer table's raw `flop_pflop` field is nonzero on {positive_flop_rows}/{len(flop_values)} rows; the arithmetic sum is {sum(flop_values):.12g} and maximum is {max(flop_values):.12g}. Units and exact scale are not inferred from the field name.")
            top_nodes = sorted((row for row in node_rows if isinstance(row.get("flop_pflop"), (int, float)) and row["flop_pflop"] > 0), key=lambda row: row["flop_pflop"], reverse=True)[:5]
            if top_nodes:
                report.append("Largest per-node values reported in `flop_pflop` (source field, no unit conversion):")
                for row in top_nodes:
                    report.append(f"- node label `{row.get('name')}`; username `{row.get('username')}`; value {row['flop_pflop']}; device `{row.get('device')}`; stage `{row.get('stage')}`; status `{row.get('status')}`")
    else:
        report.append("No peer records were available in the saved response.")
    if train_losses:
        first, last = train_losses[0], train_losses[-1]
        report.append(f"The dashboard loss series has {len(train_losses)} numeric points, from {first['value']} at step {first.get('step')} to {last['value']} at step {last.get('step')}. This describes the exported training curve; it does not by itself establish convergence or generalization.")
    else:
        report.append("No numeric training-loss series was found in the saved training response.")
    if metric_counts:
        report.append("Normalized series counts and worker-label coverage:")
        for label in sorted(metric_counts):
            n_workers = len(metric_workers.get(label, set()))
            report.append(f"- `{label}`: {metric_counts[label]} observations; {n_workers} distinct worker labels")
    else:
        report.append("No time-series observations were normalized from the saved API responses.")
    if leaderboard_payload is None:
        report.append("No public leaderboard payload was saved; node-level contribution rankings are unavailable in this snapshot.")
    else:
        report.append(f"The public leaderboard returned {len(leaderboard_rows)} contributor records. Its `score` and `active_time` fields are preserved as reported in `leaderboard.csv`; the dashboard UI describes `active_time` as cumulative hours online and contributing. The UI defines score as `(FLOPs + presence credit) / scaling`, with FLOPs summed across the contributor's nodes and presence credit based on active time. Thus score combines compute and presence; it is not a raw FLOP total. The saved UI source is recorded in `research/agora/data/public_runs_20261029T095600Z/manifest.json` (chunk `3dgii68sp_1ak.js`, SHA-256 `59bc716902fbb28cb46c8a071ddbe4feb4c35dd775ca31cffea86db441931bd1`).")
        if flop_values and leaderboard_scores:
            score_delta = sum(leaderboard_scores) - sum(flop_values)
            report.append(f"Arithmetic check: leaderboard scores sum to {sum(leaderboard_scores):.12g}; peer `flop_pflop` values sum to {sum(flop_values):.12g} (difference {score_delta:.6g}). The numerical match is an observation, not evidence that the two fields have the same definition or unit; see the dashboard's documented presence-credit formula.")
        top_contributors = sorted((row for row in leaderboard_rows if isinstance(row.get("score"), (int, float))), key=lambda row: row["score"], reverse=True)[:5]
        if top_contributors:
            report.append("Highest leaderboard contributor entries (dashboard `score`; cumulative active hours as labeled by the UI):")
            for row in top_contributors:
                report.append(f"- rank {row.get('rank')}: `{row.get('username')}`; score {row.get('score')}; active_time {row.get('active_time')}; status `{row.get('status')}`")
    report.extend([
        "",
        "## Limits",
        "",
        "- The README-linked Node0 dashboard host returned HTTP 403 during discovery. Historical data here comes from the public Agora run registry/API, not from an authenticated dashboard session.",
        "- The API's `tokens` metadata field is retained verbatim. It is not treated as processed tokens without independent field documentation.",
        "- `is_contributing` is the dashboard's row-level flag. `flop_pflop` and leaderboard `score` are preserved as reported; the score includes presence credit, so neither should be treated as standalone compute without the dashboard's exact raw-field semantics.",
        "- All normalized values preserve a reference to their immutable raw response in `raw/`; metric units remain dashboard-defined where not explicitly documented.",
        "- API history may be sampled or aggregated. This export does not establish distinct join/leave events or prove convergence.",
        "",
        "## Files",
        "",
        "- `manifest.json`: URLs, times, statuses, response sizes, hashes, and errors.",
        "- `nodes.csv`: normalized peer rows with a JSON field copy and raw file reference.",
        "- `leaderboard.csv`: normalized leaderboard rows with all source fields preserved in JSON.",
        "- `metrics.csv.gz`: deterministic gzip-compressed normalized training and worker telemetry CSV with metric names and raw references.",
        "",
        "## Reproduction command",
        "",
        f"`python research/node0/collect_public_runs.py --output {output.relative_to(ROOT.parent.parent) if output.is_relative_to(ROOT.parent.parent) else output} --analyze-only`",
    ])
    (output / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    return {
        "run_id": run_id,
        "run": run,
        "node_count": len(node_rows),
        "leaderboard_count": len(leaderboard_rows),
        "metric_count": len(metric_rows),
        "raw_bytes": total_raw_bytes,
        "registry_ref": registry_ref,
        "peer_usernames": len(peer_usernames),
        "leaderboard_usernames": len(leaderboard_usernames),
        "active_nodes": peers_payload.get("active_nodes") if isinstance(peers_payload, dict) else None,
        "contributing_nodes": peers_payload.get("contributing_nodes") if isinstance(peers_payload, dict) else None,
        "flop_nonzero_rows": positive_flop_rows,
        "flop_sum": sum(flop_values) if flop_values else None,
        "flop_max": max(flop_values) if flop_values else None,
        "top_nodes": [
            {key: row.get(key) for key in ("name", "username", "flop_pflop", "device", "stage", "status")}
            for row in sorted((row for row in node_rows if isinstance(row.get("flop_pflop"), (int, float)) and row["flop_pflop"] > 0), key=lambda row: row["flop_pflop"], reverse=True)[:5]
        ],
        "top_contributors": [
            {key: row.get(key) for key in ("rank", "username", "score", "active_time", "status")}
            for row in sorted((row for row in leaderboard_rows if isinstance(row.get("score"), (int, float))), key=lambda row: row["score"], reverse=True)[:5]
        ],
        "loss_first": {key: train_losses[0].get(key) for key in ("step", "timestamp", "value")} if train_losses else None,
        "loss_last": {key: train_losses[-1].get(key) for key in ("step", "timestamp", "value")} if train_losses else None,
        "loss_count": len(train_losses),
    }


def collect(output: Path, max_bytes: int, resume: bool) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    manifest = read_manifest(output, max_bytes)
    existing_bytes = _raw_bytes(output, manifest)
    if existing_bytes > max_bytes:
        raise ValueError(f"Existing raw snapshot is {existing_bytes} bytes, exceeding --max-bytes={max_bytes}")
    fetcher = PublicFetcher(output, manifest, max_bytes, resume)

    registry_bytes = fetcher.get("runs_registry", REGISTRY_URL)
    registry = parse_json(registry_bytes, "run registry")
    run = find_node0_run(registry) if registry is not None else None
    run_id = str((run or {}).get("id") or "node0-7-5b")

    # The project README links this dashboard, but it may require browser or
    # account access. Record the public GET outcome without attempting login.
    fetcher.get("node0_dashboard_home", NODE0_DASHBOARD_URL)

    if run is not None:
        specs = [
            ("node0_peers", f"{API_BASE}/{urllib.parse.quote(run_id, safe='')}/peers"),
            ("node0_training", f"{API_BASE}/{urllib.parse.quote(run_id, safe='')}/training?range=all"),
            ("node0_leaderboard", f"{API_BASE}/{urllib.parse.quote(run_id, safe='')}/leaderboard"),
        ]
        capabilities = run.get("capabilities") or {}
        if capabilities.get("evals"):
            specs.append(("node0_evals", f"{API_BASE}/{urllib.parse.quote(run_id, safe='')}/evals"))
        # `workers` is the dashboard's documented default detail mode. Never
        # call the separate /refresh route because it may trigger server work.
        specs.append(("node0_telemetry_workers", f"{API_BASE}/{urllib.parse.quote(run_id, safe='')}/prometheus-timeseries?range=all"))
        for key, url in specs:
            content = fetcher.get(key, url)
            if content is not None and key != "node0_dashboard_home":
                try:
                    json.loads(content)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    entry = _entry_for_url(manifest, url)
                    if entry:
                        entry["error"] = "HTTP 200 response was not valid JSON"
                        save_manifest(output, manifest)
    else:
        note_url = f"{API_BASE}/{urllib.parse.quote(run_id, safe='')}/peers"
        fetcher.get("node0_peers", note_url)

    summary = analyze(output, manifest)
    return summary


def write_project_metadata(summary: dict[str, Any], output: Path, manifest: dict[str, Any]) -> None:
    upstream = ROOT / "upstream"
    commit = subprocess.run(["git", "-C", str(upstream), "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip() if upstream.exists() else None
    commit_date = subprocess.run(["git", "-C", str(upstream), "log", "-1", "--format=%cI"], capture_output=True, text=True, check=False).stdout.strip() if upstream.exists() else None
    source = {
        "repository": REPOSITORY,
        "checkout_path": "research/node0/upstream/",
        "branch": "main",
        "commit": commit,
        "commit_date": commit_date,
        "license": "Apache-2.0 (upstream LICENSE)",
        "lfs_smudge": "disabled during clone; no model weights fetched",
    }
    (ROOT / "SOURCE.json").write_text(json.dumps(source, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    inventory = {
        "schema_version": 1,
        "project": "Node0 (Pluralis)",
        "source": source,
        "latest_snapshot": str(output.relative_to(ROOT.parent.parent)) if output.is_relative_to(ROOT.parent.parent) else str(output),
        "manifest_sha256": hashlib.sha256((output / "manifest.json").read_bytes()).hexdigest() if (output / "manifest.json").is_file() else None,
        "run_id": summary.get("run_id"),
        "request_count": len(manifest["entries"]),
        "raw_response_count": sum(e.get("status") == 200 and bool(e.get("path")) for e in manifest["entries"]),
        "raw_bytes": summary.get("raw_bytes", 0),
        "coverage": [{"key": e.get("key"), "url": e.get("url"), "path": e.get("path"), "status": e.get("status"), "bytes": e.get("bytes", 0), "sha256": e.get("sha256"), "retrieved_at": e.get("retrieved_at"), "error": e.get("error")} for e in manifest["entries"]],
        "note": "Raw snapshot is intentionally excluded from Git; its manifest and hashes remain in the snapshot directory and the compact coverage list is tracked here.",
    }
    (ROOT / "INVENTORY.json").write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    public_runs = [
        "# Node0 public run collection",
        "",
        f"Source checkout: [PluralisResearch/node0](https://github.com/PluralisResearch/node0) at `{source.get('commit')}` (Apache-2.0).",
        "",
        f"Latest collection: `{inventory['latest_snapshot']}`; run `{summary.get('run_id')}`; {summary.get('raw_bytes', 0)} raw bytes across {len(manifest['entries'])} requests.",
        "See [INVENTORY.json](INVENTORY.json) for per-endpoint coverage and errors. The bulk responses and normalized tables are in the ignored snapshot directory.",
        "",
        "## Run findings",
        "",
    ]
    run_info = summary.get("run") or {}
    meta = run_info.get("meta") or {}
    public_runs.extend([
        f"- Registry model: **{meta.get('model', 'unavailable')}**, {meta.get('params_b', 'unknown')}B parameters; dataset `{meta.get('dataset', 'unavailable')}`; published `tokens` field `{meta.get('tokens', 'unavailable')}` (not assumed to be processed tokens).",
        f"- Dashboard peer records: {summary.get('node_count', 0)} across {summary.get('peer_usernames', 0)} usernames; leaderboard records: {summary.get('leaderboard_count', 0)} across {summary.get('leaderboard_usernames', 0)} usernames.",
        f"- Peer `flop_pflop` was nonzero on {summary.get('flop_nonzero_rows', 0)} rows; sum {summary.get('flop_sum')}; this field is preserved without inferred unit or scale.",
        f"- Exported training loss: {summary.get('loss_count', 0)} points, from `{(summary.get('loss_first') or {}).get('value')}` at step `{(summary.get('loss_first') or {}).get('step')}` to `{(summary.get('loss_last') or {}).get('value')}` at step `{(summary.get('loss_last') or {}).get('step')}`. This curve alone does not establish convergence or generalization.",
        "- The dashboard UI describes leaderboard score as `(FLOPs + presence credit) / scaling`; do not read it as a raw FLOP total. Its source asset and hash are noted in the generated report.",
        "",
        "## Reproduction",
        "",
        f"`python research/node0/collect_public_runs.py --output {inventory['latest_snapshot']} --analyze-only`",
        "",
    ])
    public_runs.extend([
        "",
        "The Node0 README links `https://dashboard.pluralis.ai/`, which returned HTTP 403 to the public read-only collector. Accessible historical telemetry was collected from the shared Agora registry API. This reports dashboard observations, not independently verified compute contribution.",
    ])
    (ROOT / "PUBLIC_RUNS.md").write_text("\n".join(public_runs) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="snapshot directory (defaults to research/node0/data/public_runs_<UTC>)")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_BUDGET, help="maximum aggregate raw response bytes; default 4 GiB")
    parser.add_argument("--resume", action="store_true", help="reuse checksum-verified successful files and retry incomplete entries")
    parser.add_argument("--analyze-only", action="store_true", help="regenerate nodes.csv, metrics.csv.gz, and report.md from saved raw files")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.max_bytes < 1:
        print("--max-bytes must be positive", file=sys.stderr)
        return 2
    output = args.output or (ROOT / "data" / f"public_runs_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}")
    if not output.is_absolute():
        output = (Path.cwd() / output).resolve()
    manifest = read_manifest(output, args.max_bytes)
    if args.analyze_only:
        summary = analyze(output, manifest)
    else:
        summary = collect(output, args.max_bytes, args.resume)
        manifest = read_manifest(output, args.max_bytes)
    write_project_metadata(summary, output, manifest)
    print(json.dumps({"output": str(output), **{k: summary.get(k) for k in ("run_id", "node_count", "leaderboard_count", "metric_count", "raw_bytes")}}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
