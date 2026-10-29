#!/usr/bin/env python3
"""Bounded collector for public IOTA run dashboard data and primary references.

The run resources are taken only from the public dashboard's advertised
manifest. Raw evidence remains hash-manifested under ``http/`` and the browser
snapshots; normalized CSV is a compact convenience layer over those responses.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

PROJECT = Path(__file__).resolve().parent
REPO = PROJECT.parent / "public-networks"
sys.path.insert(0, str(REPO))
from archive_http import Archive  # noqa: E402

MAX_PROJECT_BYTES = 3_000_000_000
RAW_CAP_BYTES = 1_000_000_000
BASE = "https://iota.macrocosmos.ai"
CHARTS = {
    "ov_config": "json", "ov_loss_ma": "ndjson", "ov_throughput": "json",
    "ov_mfu": "json", "loss": "ndjson", "pl_gantt": "json",
    "pl_phase_progress": "json", "pl_layer_duration": "json", "pl_flow": "json",
    "p2p_sankey": "json", "p2p_submissions": "json", "p2p_completion": "json",
    "mn_gpu_over_time": "json", "mn_activations": "json", "mn_weights": "json",
    "mn_partition": "json", "mn_leaderboard": "json", "geography": "json",
}
DOCS = [
    ("iota-docs", "https://docs.macrocosmos.ai/subnets/subnet-9-iota"),
    ("iota-paper-html", "https://arxiv.org/html/2507.17766"),
    ("iota-paper-pdf", "https://arxiv.org/pdf/2507.17766"),
    ("iota-primer-pdf", "https://www.macrocosmos.ai/research/iota_primer.pdf"),
    ("fineweb-edu-dataset-metadata", "https://huggingface.co/api/datasets/HuggingFaceFW/fineweb-edu-2"),
]


def tree_bytes(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) if root.exists() else 0


def raw_budget(max_bytes: int) -> int:
    if max_bytes <= 0 or max_bytes > MAX_PROJECT_BYTES:
        raise ValueError(f"max_bytes must be in 1..{MAX_PROJECT_BYTES}")
    return min(max_bytes // 2, RAW_CAP_BYTES)


def browser_run_manifest(output: Path) -> dict | None:
    for snapshot in sorted(output.glob("dashboard_*/manifest.json")):
        try:
            manifest = json.loads(snapshot.read_text())
            for entry in manifest.get("entries", []):
                if entry.get("status") != 200 or "/api/data/manifest.json" not in (entry.get("url") or ""):
                    continue
                body = snapshot.parent / entry["path"]
                value = json.loads(body.read_text())
                if isinstance(value.get("runs"), list) and isinstance(value.get("charts"), list):
                    return value
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return None


def verify_existing(output: Path, require_manifest: bool = False) -> int:
    manifests = list(output.glob("dashboard_*/manifest.json"))
    http_manifest = output / "http" / "manifest.json"
    if http_manifest.exists():
        manifests.append(http_manifest)
    if require_manifest and not manifests:
        raise FileNotFoundError("No saved dashboard or HTTP manifest under --output")
    checked = 0
    for manifest_path in manifests:
        payload = json.loads(manifest_path.read_text())
        for entry in payload.get("entries", []):
            rel = entry.get("path")
            if not rel:
                continue
            evidence = manifest_path.parent / rel
            if not evidence.is_file():
                raise FileNotFoundError(f"Manifest evidence missing: {evidence}")
            if evidence.stat().st_size != entry.get("bytes"):
                raise ValueError(f"Evidence size mismatch: {evidence}")
            digest = hashlib.sha256(evidence.read_bytes()).hexdigest()
            if digest != entry.get("sha256"):
                raise ValueError(f"Evidence hash mismatch: {evidence}")
            checked += 1
    return checked


def json_resource(path: Path, chart_id: str):
    text = path.read_text(encoding="utf-8")
    if chart_id.endswith("loss_ma") or chart_id == "loss":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return json.loads(text)


def source_rows(run_name: str, chart_id: str, data, source_url: str):
    """Yield long-form values while retaining the dashboard's native fields."""
    columns = list(data) if isinstance(data, dict) else []
    if chart_id == "mn_leaderboard" and isinstance(data, dict):
        keys = data.get("columns", [])
        for i, values in enumerate(data.get("rows", [])):
            fields = dict(zip(keys, values))
            yield {"run": run_name, "chart_id": chart_id, "trace": "leaderboard",
                   "series_index": 0, "point_index": i, "x": fields.get("#"),
                   "y": None, "z": None, "customdata": None,
                   "hotkey": fields.get("HOTKEY"), "layer": None,
                   "metric_value": json.dumps(fields, ensure_ascii=False), "source_url": source_url}
        return

    lines = data if isinstance(data, list) and data and isinstance(data[0], dict) and "format" in data[0] else None
    traces = []
    if lines is not None:
        metadata = {int(t.get("i", i)): t.get("name", f"trace_{i}")
                    for i, t in enumerate(lines[0].get("traces", [])) if isinstance(t, dict)}
        chunk_counts: dict[int, int] = {}
        for t in lines[1:]:
            if not isinstance(t, dict):
                continue
            trace_id = int(t.get("i", 0))
            chunk_index = chunk_counts.get(trace_id, 0)
            chunk_counts[trace_id] = chunk_index + 1
            traces.append(dict(t, _trace_name=metadata.get(trace_id, f"trace_{trace_id}"),
                               _trace_id=trace_id, _chunk_index=chunk_index))
    elif isinstance(data, dict):
        traces = data.get("data", []) if isinstance(data.get("data"), list) else []
        if not traces and any(isinstance(data.get(k), list) for k in ("x", "y", "z")):
            traces = [data]
    for si, trace in enumerate(traces):
        if not isinstance(trace, dict):
            continue
        name = trace.get("name", trace.get("_trace_name", f"trace_{si}"))
        xs, ys, zs, customs = trace.get("x", []), trace.get("y", []), trace.get("z", []), trace.get("customdata", [])
        x_time, x_tokens = trace.get("x_time", []), trace.get("x_tokens", [])
        if isinstance(zs, list) and zs and isinstance(zs[0], list):
            for yi, row in enumerate(zs):
                for xi, val in enumerate(row):
                    y_label = ys[yi] if yi < len(ys) else yi
                    x_label = xs[xi] if xi < len(xs) else xi
                    custom = customs[yi][xi] if yi < len(customs) and isinstance(customs[yi], list) and xi < len(customs[yi]) else None
                    layer, hotkey = None, None
                    if isinstance(y_label, str) and " · " in y_label:
                        layer, hotkey = y_label.split(" · ", 1)
                    yield {"run": run_name, "chart_id": chart_id, "trace": name,
                           "series_index": si, "point_index": f"{yi}:{xi}", "x": x_label,
                           "chunk_index": trace.get("_chunk_index"),
                           "x_time": None, "x_tokens": None,
                           "y": y_label, "z": val, "customdata": json.dumps(custom, ensure_ascii=False),
                           "hotkey": hotkey, "layer": layer,
                           "metric_value": val, "source_url": source_url}
        else:
            n = max(len(xs) if isinstance(xs, list) else 0, len(ys) if isinstance(ys, list) else 0,
                    len(x_time) if isinstance(x_time, list) else 0,
                    len(x_tokens) if isinstance(x_tokens, list) else 0)
            for i in range(n):
                x, y = (xs[i] if i < len(xs) else None), (ys[i] if i < len(ys) else None)
                custom = customs[i] if isinstance(customs, list) and i < len(customs) else None
                yield {"run": run_name, "chart_id": chart_id, "trace": name,
                       "series_index": trace.get("_trace_id", si), "chunk_index": trace.get("_chunk_index"),
                       "point_index": i, "x": x,
                       "x_time": x_time[i] if isinstance(x_time, list) and i < len(x_time) else None,
                       "x_tokens": x_tokens[i] if isinstance(x_tokens, list) and i < len(x_tokens) else None,
                       "y": y, "z": None,
                       "customdata": json.dumps(custom, ensure_ascii=False), "hotkey": None,
                       "layer": None, "metric_value": y, "source_url": source_url}


CSV_FIELDS = ["run", "chart_id", "trace", "series_index", "chunk_index", "point_index", "x",
              "x_time", "x_tokens", "y", "z",
              "customdata", "hotkey", "layer", "metric_value", "source_url"]


def write_normalized(output: Path, archive_manifest: dict, max_bytes: int) -> dict:
    target = output / "normalized" / "public_run_series.csv.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    archive_root = output / "http"
    count = 0
    parse_errors = []
    rows_by_chart: dict[str, int] = {}
    with tmp.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as gz:
        text = io.TextIOWrapper(gz, encoding="utf-8", newline="", write_through=True)
        writer = csv.DictWriter(text, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for entry in archive_manifest.get("entries", []):
            url = entry.get("url", "")
            if "/api/data/" not in url or entry.get("status") != 200 or not entry.get("path"):
                continue
            parts = url.split("/api/data/", 1)[1].split("?", 1)[0].split("/")
            if len(parts) < 2:
                continue
            run_name, chart_file = parts[0] if len(parts) == 2 else parts[-2], parts[-1]
            chart_id = chart_file.rsplit(".", 1)[0]
            if chart_id not in CHARTS or chart_id == "ov_config":
                continue
            try:
                value = json_resource(archive_root / entry["path"], chart_id)
                for row in source_rows(run_name, chart_id, value, url):
                    writer.writerow(row)
                    count += 1
                    rows_by_chart[chart_id] = rows_by_chart.get(chart_id, 0) + 1
            except (OSError, ValueError, TypeError, UnicodeError) as exc:
                parse_errors.append({"run": run_name, "chart_id": chart_id,
                                     "url": url, "error": str(exc)})
                continue
        text.flush()
    tmp.replace(target)
    size = target.stat().st_size
    if tree_bytes(output) > max_bytes:
        target.unlink(missing_ok=True)
        raise RuntimeError("normalized output exceeded --max-bytes; removed last derivative")
    return {"path": str(target.relative_to(output)), "bytes": size, "rows": count,
            "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
            "rows_by_chart": rows_by_chart, "parse_errors": parse_errors}


def failed_paths(output: Path, archive_manifest: dict | None) -> list[dict]:
    failures = []
    for entry in (archive_manifest or {}).get("entries", []):
        if not entry.get("path") or entry.get("status") != 200:
            failures.append({"source": "http", "url": entry.get("url"), "label": entry.get("label"),
                             "status": entry.get("status"), "error": entry.get("error")})
    for manifest_path in sorted(output.glob("dashboard_*/manifest.json")):
        try:
            capture = json.loads(manifest_path.read_text())
        except (OSError, ValueError):
            continue
        for entry in capture.get("entries", []):
            if entry.get("status") not in (None, 200) or entry.get("application_status") == "skipped":
                failures.append({"source": manifest_path.parent.name, "url": entry.get("url"),
                                 "status": entry.get("status"), "error": entry.get("reason")})
            if entry.get("kind") != "browser_logs" or not entry.get("path"):
                continue
            try:
                logs = json.loads((manifest_path.parent / entry["path"]).read_text())
            except (OSError, ValueError):
                continue
            for failure in logs.get("failed_requests", []) + logs.get("page_errors", []):
                failures.append({"source": manifest_path.parent.name, **failure})
    return failures


def inventory(output: Path, max_bytes: int, archive_manifest: dict | None):
    entries = []
    for p in sorted(output.rglob("*")):
        if p.is_file() and p.name != "PUBLIC_RUNS_INVENTORY.json":
            data_hash = hashlib.sha256(p.read_bytes()).hexdigest()
            entries.append({"path": str(p.relative_to(output)), "bytes": p.stat().st_size,
                            "sha256": data_hash, "status": "captured"})
    failed = failed_paths(output, archive_manifest)
    value = {"schema_version": 1, "project": "macrocosmos-iota", "max_bytes": max_bytes,
             "tree_bytes": tree_bytes(output), "files": entries, "failed_paths": failed}
    (output / "PUBLIC_RUNS_INVENTORY.json").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return value


def collect(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if tree_bytes(output) > args.max_bytes:
        raise RuntimeError("existing output already exceeds --max-bytes")
    verify_existing(output, require_manifest=args.analyze_only)
    archive_root = output / "http"
    archive_manifest = None
    run_manifest = None
    if not args.analyze_only:
        remaining = args.max_bytes - tree_bytes(output)
        raw_limit = min(raw_budget(args.max_bytes), max(0, remaining - 2_000_000))
        if raw_limit < 1:
            raise RuntimeError("no room for bounded HTTP archive under --max-bytes")
        arch = Archive(archive_root, max_bytes=raw_limit, resume=args.resume, delay=1.0)
        manifest_entry = arch.fetch(BASE + "/api/data/manifest.json", "dashboard-run-manifest")
        run_manifest = arch.json(manifest_entry)
        if not run_manifest:
            run_manifest = browser_run_manifest(output)
        for label, url in DOCS:
            arch.fetch(url, label)
        if isinstance(run_manifest, dict):
            baked = run_manifest.get("baked_at")
            runs = run_manifest.get("runs", [])
            projected = 0
            plan = []
            for run in runs:
                run_dir = run.get("dir")
                run_name = run.get("run") or run.get("name") or run_dir
                sizes = run.get("bytes", {})
                if not run_dir or not isinstance(sizes, dict):
                    continue
                run_expected = sum(int(sizes.get(chart_id, 0) or 0) for chart_id in CHARTS)
                if run_expected > 100_000_000 or projected + run_expected > min(raw_limit, RAW_CAP_BYTES):
                    continue
                projected += run_expected
                plan.append((run_name, run_dir, sizes))
            for run_name, run_dir, sizes in plan:
                for chart_id, ext in CHARTS.items():
                    expected = int(sizes.get(chart_id, 0) or 0)
                    if not expected or expected > 20_000_000:
                        continue
                    url = f"{BASE}/api/data/{run_dir}/{chart_id}.{ext}?{urlencode({'baked_at': baked})}"
                    arch.fetch(url, f"{run_name}:{chart_id}")
        archive_manifest = arch.manifest
    elif (archive_root / "manifest.json").exists():
        archive_manifest = json.loads((archive_root / "manifest.json").read_text())
    else:
        archive_manifest = {"entries": []}
    if not run_manifest:
        run_manifest = browser_run_manifest(output)
    if not run_manifest:
        raise FileNotFoundError("No valid public run manifest in verified dashboard snapshots or HTTP archive")
    derived = write_normalized(output, archive_manifest, args.max_bytes)
    current = next((r for r in run_manifest.get("runs", [])
                    if r.get("dir") == run_manifest.get("default") or r.get("run") == run_manifest.get("default")), None)
    observed_timestamps = []
    for manifest_path in list(output.glob("dashboard_*/manifest.json")) + ([archive_root / "manifest.json"] if (archive_root / "manifest.json").exists() else []):
        try:
            payload = json.loads(manifest_path.read_text())
        except (OSError, ValueError):
            continue
        for stamp in (payload.get("created_at"), payload.get("completed_at")):
            if stamp:
                try:
                    observed_timestamps.append(datetime.fromisoformat(stamp.replace("Z", "+00:00")))
                except ValueError:
                    pass
    collected_at = datetime.now(timezone.utc).isoformat()
    summary = {"project": "macrocosmos-iota", "snapshot_date": max(observed_timestamps).date().isoformat() if observed_timestamps else None,
               "collected_at_utc": collected_at,
               "dashboard_manifest_baked_at": run_manifest.get("baked_at"),
               "default_run": run_manifest.get("default"), "advertised_runs": len(run_manifest.get("runs", [])),
               "current_run_state_id": current.get("run_state_id") if current else None,
               "advertised_chart_types": len(run_manifest.get("charts", [])),
               "normalized_rows_by_chart": derived.get("rows_by_chart", {}),
               "successful_chart_responses": sum(1 for e in archive_manifest.get("entries", [])
                                                    if "/api/data/" in e.get("url", "")
                                                    and not e.get("url", "").endswith("/manifest.json")
                                                    and e.get("status") == 200 and e.get("path")),
               "failed_or_unavailable_paths": failed_paths(output, archive_manifest),
               "normalized": derived, "total_tree_bytes": tree_bytes(output), "max_bytes": args.max_bytes}
    (output / "PUBLIC_RUNS_SUMMARY.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    inventory_value = inventory(output, args.max_bytes, archive_manifest)
    if tree_bytes(output) > args.max_bytes:
        raise RuntimeError("project output exceeded --max-bytes")
    print(json.dumps({"tree_bytes": inventory_value["tree_bytes"], "files": len(inventory_value["files"]),
                      "successful_chart_responses": summary["successful_chart_responses"],
                      "failed_paths": len(summary["failed_or_unavailable_paths"]), "normalized": derived}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-bytes", type=int, default=MAX_PROJECT_BYTES)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    args = parser.parse_args()
    raw_budget(args.max_bytes)
    collect(args)


if __name__ == "__main__":
    main()
