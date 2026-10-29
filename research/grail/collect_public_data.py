#!/usr/bin/env python3
"""Archive official Grail metadata and normalize bounded public captures."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "research" / "public-networks"))
from archive_http import Archive, digest  # noqa: E402

RAW_CAP_DECIMAL = 1_000_000_000
OFFICIAL_GETS = (
    ("grafana_operations_definition", "https://grail-grafana.tplr.ai/api/dashboards/uid/grail-ops"),
    ("covenant_hf_grail_search", "https://huggingface.co/api/models?author=1Covenant&search=grail&limit=100"),
    ("qwen_1_5b_model_metadata", "https://huggingface.co/api/models/Qwen/Qwen2.5-1.5B-Instruct"),
    ("qwen_1_5b_model_card", "https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct/raw/main/README.md"),
)
GRAPHQL_ENDPOINTS = (
    ("forge", "https://forge.coreweave.com/api/wandb/graphql"),
    ("wandb", "https://api.wandb.ai/graphql"),
)
GRAPHQL_QUERY = """query PublicGrailProjectMetadata {
  project(name: \"grail\", entityName: \"tplr\") {
    name runCount
    runs(first: 10, order: \"createdAt\") {
      edges { node { name displayName state createdAt heartbeatAt historyLineCount } }
    }
  }
}"""
RUN_FIELDS = ["endpoint", "entity", "project", "run_name", "display_name", "state", "created_at", "heartbeat_at", "history_line_count"]


def total_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file()) if root.exists() else 0


def json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def bounded_write(root: Path, relative: str, body: bytes, limit: int) -> Path:
    target = root / relative
    if not target.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"Unsafe output path: {relative!r}")
    previous = target.stat().st_size if target.is_file() else 0
    if total_bytes(root) - previous + len(body) > limit:
        raise ValueError(f"Total output byte budget {limit} would be exceeded by {relative}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name("." + target.name + ".tmp")
    temp.write_bytes(body)
    temp.replace(target)
    return target


def csv_bytes(rows: list[dict[str, Any]], fields: list[str]) -> bytes:
    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def record_public_post(archive: Archive, label: str, endpoint: str, query: str) -> dict[str, Any]:
    """Make one unauthenticated, read-only GraphQL request and archive its body."""
    host = urllib.parse.urlsplit(endpoint).hostname
    if urllib.parse.urlsplit(endpoint).scheme != "https" or host not in {"forge.coreweave.com", "api.wandb.ai"}:
        raise ValueError("GraphQL endpoint is outside the explicit public-host allowlist")
    payload = json.dumps({"query": query}, separators=(",", ":")).encode("utf-8")
    request_hash = hashlib.sha256(payload).hexdigest()
    for old in reversed(archive.manifest.get("entries", [])):
        if old.get("method") != "POST" or old.get("url") != endpoint or old.get("label") != label or old.get("request_sha256") != request_hash:
            continue
        old_path = archive.output / old["path"] if old.get("path") else None
        if old_path and old_path.is_file() and old_path.stat().st_size == old.get("bytes") and digest(old_path) == old.get("sha256"):
            print(label, "reused", old.get("bytes", 0), flush=True)
            return old
        break
    if archive.used >= archive.limit:
        entry = {"url": endpoint, "label": label, "method": "POST", "error": "budget_exhausted", "bytes": 0}
        archive.manifest["entries"].append(entry)
        archive.flush()
        return entry
    request = urllib.request.Request(
        endpoint,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "PublicRunResearchArchive/1.0"},
    )
    record: dict[str, Any] = {
        "url": endpoint,
        "label": label,
        "method": "POST",
        "request_sha256": request_hash,
        "request_description": "Unauthenticated metadata-only public W&B GraphQL query; no config or history requested.",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "attempts": [],
    }
    time.sleep(0.5)
    try:
        try:
            response = urllib.request.urlopen(request, timeout=25)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            status = response.code
            final_url = response.url
            final_host = urllib.parse.urlsplit(final_url).hostname
            record.update({"status": status, "final_url": final_url, "attempts": [{"status": status, "at": datetime.now(timezone.utc).isoformat()}]})
            if final_host != host:
                record["error"] = "redirect_host_not_allowed"
            else:
                remaining = archive.limit - archive.used
                body = response.read(min(2_000_000, remaining) + 1)
                if len(body) > remaining:
                    record["error"] = "budget_exhausted"
                elif len(body) > 2_000_000:
                    record["error"] = "response_exceeds_2MB_capture_limit"
                else:
                    filename = hashlib.sha256((endpoint + label + request_hash).encode()).hexdigest() + ".json"
                    path = archive.output / "raw" / filename
                    if path.exists():
                        path = path.with_name(path.stem + "-" + str(len(archive.manifest["entries"])) + path.suffix)
                    path.write_bytes(body)
                    archive.used += len(body)
                    record.update({
                        "content_type": response.headers.get("Content-Type", ""),
                        "path": str(path.relative_to(archive.output)),
                        "bytes": len(body),
                        "sha256": digest(path),
                    })
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
        record["error"] = f"{type(error).__name__}: {error}"
        record["attempts"] = [{"error": type(error).__name__, "at": datetime.now(timezone.utc).isoformat()}]
    archive.manifest["entries"].append(record)
    archive.flush()
    print(label, record.get("status", record.get("error")), record.get("bytes", 0), flush=True)
    return record


def _snapshot_dirs(output: Path) -> list[Path]:
    found = []
    http_dir = (output / "http").resolve()
    for manifest_path in sorted(output.rglob("manifest.json")):
        if http_dir in manifest_path.resolve().parents:
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, UnicodeError):
            continue
        if manifest.get("capture_status") and any(e.get("kind") == "http_response" for e in manifest.get("entries", [])):
            found.append(manifest_path.parent)
    return found


def _verify_snapshot(snapshot: Path, manifest: dict[str, Any]) -> list[str]:
    issues = []
    for entry in manifest.get("entries", []):
        relative = entry.get("path")
        if not relative:
            continue
        path = (snapshot / relative).resolve()
        if not path.is_relative_to(snapshot.resolve()) or not path.is_file():
            issues.append(f"missing or unsafe path {relative}")
            continue
        body = path.read_bytes()
        if len(body) != entry.get("bytes") or hashlib.sha256(body).hexdigest() != entry.get("sha256"):
            issues.append(f"size/hash mismatch {relative}")
    return issues


def _public_graphql_result(archive: Archive) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    runs: list[dict[str, Any]] = []
    projects = []
    for entry in archive.manifest.get("entries", []):
        if entry.get("method") != "POST" or entry.get("label") not in {"wandb_forge_project_metadata", "wandb_api_project_metadata"}:
            continue
        data = None
        if entry.get("path"):
            body_path = (archive.output / entry["path"]).resolve()
            if body_path.is_relative_to(archive.output.resolve()) and body_path.is_file():
                body = body_path.read_bytes()
                if len(body) == entry.get("bytes") and hashlib.sha256(body).hexdigest() == entry.get("sha256"):
                    try:
                        data = json.loads(body)
                    except (ValueError, UnicodeError):
                        pass
        project = (data or {}).get("data", {}).get("project") if isinstance(data, dict) else None
        projects.append({"label": entry.get("label"), "url": entry.get("url"), "status": entry.get("status"), "project_resolved": isinstance(project, dict), "project_name": project.get("name") if isinstance(project, dict) else None, "run_count": project.get("runCount") if isinstance(project, dict) else None, "graphql_errors": (data or {}).get("errors", []) if isinstance(data, dict) else []})
        if isinstance(project, dict):
            edges = project.get("runs", {}).get("edges", [])
            for edge in edges if isinstance(edges, list) else []:
                node = edge.get("node", {}) if isinstance(edge, dict) else {}
                if isinstance(node, dict):
                    runs.append({"endpoint": entry.get("url"), "entity": "tplr", "project": "grail", "run_name": node.get("name"), "display_name": node.get("displayName"), "state": node.get("state"), "created_at": node.get("createdAt"), "heartbeat_at": node.get("heartbeatAt"), "history_line_count": node.get("historyLineCount")})
    return runs, projects


def normalize(output: Path, archive: Archive) -> dict[str, Any]:
    snapshots = _snapshot_dirs(output)
    blog_evidence: dict[str, Any] = {"captured": False}
    coverage = []
    grafana_queries = []
    for snapshot in snapshots:
        try:
            manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, ValueError, UnicodeError):
            continue
        integrity_issues = _verify_snapshot(snapshot, manifest)
        summary_path = snapshot / "artifacts/page_summary.json"
        visible_text = ""
        if summary_path.is_file() and not integrity_issues:
            try:
                visible_text = str(json.loads(summary_path.read_text(encoding="utf-8")).get("visible_text", ""))
            except (OSError, ValueError, UnicodeError):
                pass
        title = None
        if summary_path.is_file():
            try:
                title = json.loads(summary_path.read_text(encoding="utf-8")).get("title")
            except (OSError, ValueError, UnicodeError):
                pass
        coverage.append({"snapshot": str(snapshot.relative_to(output)), "capture_status": manifest.get("capture_status", "unknown"), "saved_bytes": manifest.get("total_saved_bytes"), "title": title, "integrity_verified": not integrity_issues, "integrity_issues": integrity_issues})
        lowered = visible_text.lower()
        if not integrity_issues:
            for entry in manifest.get("entries", []):
                if urllib.parse.urlsplit(entry.get("url", "")).path != "/api/ds/query" or not entry.get("path"):
                    continue
                payload = json.loads((snapshot / entry["path"]).read_text())
                for ref, result in payload.get("results", {}).items():
                    frames = result.get("frames", [])
                    grafana_queries.append({"snapshot": str(snapshot.relative_to(output)),
                        "url": entry["url"], "response_sha256": entry["sha256"],
                        "http_status": entry.get("status"), "query_status": result.get("status"),
                        "ref": ref, "frame_count": len(frames),
                        "frame_row_counts": [max((len(v) for v in f.get("data", {}).get("values", [])), default=0) for f in frames],
                        "request_evidence": str((snapshot / "artifacts/public_query_requests.json").relative_to(output))})
        if "grail" in snapshot.name.lower() and "blog" in snapshot.name.lower():
            blog_evidence = {
                "captured": bool(visible_text),
                "snapshot": str(snapshot.relative_to(output)),
                "title": title,
                "mentions_qwen_2_5_1_5b_instruct": "qwen2.5-1.5b-instruct" in lowered,
                "mentions_100_windows": bool(re.search(r"100 windows", visible_text, re.I)),
                "mentions_roughly_320_trainer_updates": bool(re.search(r"roughly 320 trainer update steps", visible_text, re.I)),
                "mentions_single_live_chain_training_run": bool(re.search(r"single training run on the live chain", visible_text, re.I)),
                "completion_status_terms_found": sorted(set(re.findall(r"\b(?:completed|finished)\b", visible_text, re.I))),
                "integrity_verified": not integrity_issues,
                "pdf_captured": (snapshot / "artifacts/page.pdf").is_file(),
                "mhtml_captured": (snapshot / "artifacts/page.mhtml").is_file(),
            }
    runs, projects = _public_graphql_result(archive)
    return {
        "dashboard_snapshots": coverage,
        "wandb_project_checks": projects,
        "wandb_run_rows": len(runs),
        "wandb_numerical_history_rows": 0,
        "grail_v0_blog": blog_evidence,
        "publisher_reported_results": {
            "training_pass_at_1": "approximately 3% to 41%",
            "training_pass_at_5": "approximately 10% to 63%",
            "external_benchmarks": {
                "GSM8K": "72.2% trained versus 57.9% 4-shot base",
                "MATH": "47.6% trained versus 12.7% 4-shot base",
                "AMC 2023": "25% trained versus 7.5% 4-shot base",
            },
            "qualification": "These are values described in the official article, not an independently recovered numerical history export; article states the results are from a single run.",
        },
        "public_run_metadata_available": bool(runs),
        "grafana_read_queries": grafana_queries,
        "wandb_numerical_history_verified": False,
        "source_gaps": [],
    }


def write_inventory(batch_root: Path) -> dict[str, Any]:
    files = []
    for path in sorted(p for p in batch_root.rglob("*") if p.is_file() and ".tmp" not in p.name):
        files.append({"path": path.relative_to(PROJECT_DIR).as_posix(), "bytes": path.stat().st_size, "sha256": digest(path)})
    result = {"schema_version": 1, "project": "grail", "batch_root": batch_root.relative_to(PROJECT_DIR).as_posix(), "files": files, "file_count": len(files), "total_bytes": sum(item["bytes"] for item in files)}
    (PROJECT_DIR / "INVENTORY.json").write_bytes(json_bytes(result))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Whole batch root: browser captures, http/, and derived files")
    parser.add_argument("--max-bytes", type=int, required=True, help="Maximum total bytes under --output")
    parser.add_argument("--resume", action="store_true", help="Verify and reuse existing HTTP evidence")
    parser.add_argument("--analyze-only", action="store_true", help="Regenerate local normalized artifacts without network access")
    args = parser.parse_args(argv)
    if not 4096 <= args.max_bytes <= 3_000_000_000:
        parser.error("--max-bytes must be between 4096 and 3000000000 (3 GB decimal)")
    output = args.output.resolve()
    if not output.is_relative_to((PROJECT_DIR / "data").resolve()):
        parser.error("--output must be a batch directory under research/grail/data/")
    output.mkdir(parents=True, exist_ok=True)
    http_output = output / "http"
    if args.analyze_only and not (http_output / "manifest.json").is_file():
        parser.error("--analyze-only requires an existing http/manifest.json")
    existing = total_bytes(output)
    if existing >= args.max_bytes:
        parser.error("Existing batch output already meets or exceeds --max-bytes")
    reserve = min(2_000_000, max(4096, args.max_bytes // 20))
    raw_cap = min(args.max_bytes // 2, RAW_CAP_DECIMAL, args.max_bytes - existing - reserve)
    if raw_cap < 1:
        parser.error("No remaining byte budget for raw HTTP responses")
    try:
        archive = Archive(http_output, max_bytes=raw_cap, resume=args.resume or args.analyze_only, delay=0.5)
        if not args.analyze_only:
            for label, url in OFFICIAL_GETS:
                archive.fetch(url, label)
            for name, endpoint in GRAPHQL_ENDPOINTS:
                label = "wandb_forge_project_metadata" if name == "forge" else "wandb_api_project_metadata"
                record_public_post(archive, label, endpoint, GRAPHQL_QUERY)
        summary = normalize(output, archive)
        if not summary["dashboard_snapshots"]:
            summary["source_gaps"].append(f"No browser snapshots discovered under {output}; collector does not read a different label.")
        if not summary["grail_v0_blog"].get("captured"):
            summary["source_gaps"].append("Official grail-v0 Substack article capture is absent or has no visible text.")
        if not any(p["project_resolved"] for p in summary["wandb_project_checks"]):
            summary["source_gaps"].append("Unauthenticated public W&B GraphQL checks did not resolve tplr/grail; run listing and numerical history are unavailable in this collection.")
        if not summary["wandb_run_rows"]:
            summary["source_gaps"].append("No run metadata rows were returned; no per-run numeric history query was made.")
        for relative, body in (
            ("normalized/grail_wandb_runs.csv", csv_bytes([], RUN_FIELDS)),
            ("normalized/grail_wandb_numeric_history.csv", b"endpoint,run_name,step,metric,value\n"),
        ):
            # Fill the metadata table if the public project endpoint returned runs.
            if relative.endswith("runs.csv"):
                rows, _ = _public_graphql_result(archive)
                body = csv_bytes(rows, RUN_FIELDS)
            bounded_write(output, relative, body, args.max_bytes)
        summary.update({"total_output_limit_bytes": args.max_bytes, "raw_limit_bytes": raw_cap, "output_before_summary_bytes": total_bytes(output)})
        bounded_write(output, "summary.json", json_bytes(summary), args.max_bytes)
        report = [
            "# Grail public data capture",
            "",
            "The official `grail-v0` article describes one live-chain Qwen2.5-1.5B-Instruct run over 100 windows (roughly 320 trainer update steps after evaluation-only windows). This establishes the reported plotted horizon; it does not provide a machine-readable run state in the archived article. The article itself cautions that its comparisons are a single run.",
            "",
            "In its narrative, the article reports training pass@1 rising from about 3% to 41% and pass@5 from about 10% to 63%; its external benchmark comparison reports 72.2% on GSM8K, 47.6% on MATH, and 25% on AMC 2023 for the trained model. These are publisher-reported results, not recovered per-step values, and the author says they come from one run.",
            "",
            "The official W&B project page rendered only a loading shell in the dated browser capture. The collector made unauthenticated, metadata-only GraphQL requests to the official W&B/API and CoreWeave Forge GraphQL endpoints, asking for at most ten run metadata rows and no config or history. When the public project object does not resolve, this collector stops there; it does not invent run IDs or attempt history queries.",
            "",
            f"- Public project checks: {len(summary['wandb_project_checks'])}",
            f"- Resolved public runs: {summary['wandb_run_rows']}",
            f"- Public numerical history rows: {summary['wandb_numerical_history_rows']}",
            f"- Browser snapshots under this batch: {len(summary['dashboard_snapshots'])}",
            f"- Grafana read-query responses: {len(summary['grafana_read_queries'])}; see summary for frame rows and archived request windows.",
            "The corrected Grafana capture allows the official dashboard WebSocket and a bounded read-only Loki query endpoint. An empty selected-window response does not establish absence of older history. Original restricted attempts are retained.",
            "",
            "`normalized/grail_wandb_runs.csv` and `normalized/grail_wandb_numeric_history.csv` are compact normalized outputs. The Qwen HF README/metadata responses describe the base model, not a Grail-trained checkpoint. No weights or datasets were requested. The official blog’s PDF and MHTML browser artifacts are retained in the dated batch when the capture succeeded.",
            "",
            "See `summary.json` for source coverage and gaps; see `http/manifest.json` for exact requests, responses, statuses, sizes, and hashes.",
            "",
        ]
        bounded_write(output, "reports/public-data.md", "\n".join(report).encode(), args.max_bytes)
        failures = [e for e in archive.manifest.get("entries", []) if e.get("status") != 200 or not e.get("path")]
        archive.manifest.update({"collection_status": "complete_with_source_gaps" if failures or summary["source_gaps"] else "complete", "raw_limit_bytes": raw_cap, "total_output_limit_bytes": args.max_bytes, "batch_root": str(output), "normalized_output": "summary.json"})
        archive.flush()
        if total_bytes(output) > args.max_bytes:
            raise ValueError("Final collector output exceeded --max-bytes")
        write_inventory(output)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    except Exception as error:
        print(f"collection failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
