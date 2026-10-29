#!/usr/bin/env python3
"""Collect bounded, public INTELLECT-1 release and training evidence."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESEARCH = HERE.parent
sys.path.insert(0, str(RESEARCH / "public-networks"))
from archive_http import Archive, digest, save_json  # noqa: E402

MAX_PROJECT_BYTES = 3_000_000_000
MAX_RAW_BYTES = 1_000_000_000
CAPTURES = (
    ("dashboard", "https://app.primeintellect.ai/intelligence", ()),
    ("release_page", "https://www.primeintellect.ai/blog/intellect-1-release", ("cdn.prod.website-files.com",)),
)
SOURCES = (
    ("arxiv_paper_html", "https://arxiv.org/html/2412.01152"),
    ("arxiv_paper_pdf", "https://arxiv.org/pdf/2412.01152"),
    ("release_blog_html", "https://www.primeintellect.ai/blog/intellect-1-release"),
    ("model_metadata", "https://huggingface.co/api/models/PrimeIntellect/INTELLECT-1"),
    ("model_card", "https://huggingface.co/PrimeIntellect/INTELLECT-1/raw/main/README.md"),
    ("instruct_model_metadata", "https://huggingface.co/api/models/PrimeIntellect/INTELLECT-1-Instruct"),
    ("instruct_model_card", "https://huggingface.co/PrimeIntellect/INTELLECT-1-Instruct/raw/main/README.md"),
    ("model_collection", "https://huggingface.co/collections/PrimeIntellect/intellect-1"),
    ("dataset_collection_metadata", "https://huggingface.co/collections/PrimeIntellect/intellect-1-dataset"),
)


def raw_budget_parts(max_bytes: int, capture_count: int = len(CAPTURES)) -> tuple[int, int, int]:
    """Return total raw cap, HTTP body cap, and per-browser-capture cap."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    raw_cap = min(max_bytes // 2, MAX_RAW_BYTES)
    browser_total = raw_cap // 2
    http_cap = raw_cap - browser_total
    each_capture = browser_total // max(1, capture_count)
    return raw_cap, http_cap, each_capture


def normalized_entries(manifest: dict) -> list[dict]:
    """Give every HTTP attempt the same path/size/hash/status fields."""
    rows = []
    for item in manifest.get("entries", []):
        row = dict(item)
        row.setdefault("path", None)
        row.setdefault("bytes", 0)
        row.setdefault("sha256", None)
        row.setdefault("status", None)
        rows.append(row)
    return rows


def file_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in Path(root).rglob("*") if path.is_file())


def enforce_budget(root: Path, max_bytes: int, raw_bytes: int) -> int:
    raw_cap = min(max_bytes // 2, MAX_RAW_BYTES)
    if raw_bytes > raw_cap:
        raise ValueError(f"Raw evidence exceeded its cap: {raw_bytes} > {raw_cap}")
    total = file_bytes(root)
    if total > max_bytes:
        raise ValueError(f"Total output exceeded --max-bytes: {total} > {max_bytes}")
    return total


def capture_bytes(root: Path) -> int:
    """Count browser evidence payloads, excluding their small manifests."""
    captures = Path(root) / "captures"
    return sum(path.stat().st_size for path in captures.rglob("*")
               if path.is_file() and path.name != "manifest.json") if captures.exists() else 0


def verify_captures(root: Path) -> None:
    captures = Path(root) / "captures"
    if not captures.exists():
        return
    for manifest_path in captures.glob("*/manifest.json"):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest.get("entries", []):
            if not entry.get("path"):
                continue
            evidence = manifest_path.parent / entry["path"]
            if (not evidence.is_file() or evidence.stat().st_size != entry.get("bytes")
                    or digest(evidence) != entry.get("sha256")):
                raise ValueError(f"Browser evidence hash/size mismatch: {evidence}")


def verify_raw_archive(output: Path, manifest: dict) -> int:
    total = 0
    for entry in normalized_entries(manifest):
        if entry["path"]:
            path = output / entry["path"]
            if not path.is_file() or path.stat().st_size != entry["bytes"] or digest(path) != entry["sha256"]:
                raise ValueError(f"Evidence hash/size mismatch: {entry['path']}")
            total += entry["bytes"]
    return total


def write_table(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["label", "url", "status", "bytes", "sha256", "path", "error", "final_url"]
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for entry in entries:
            writer.writerow({key: entry.get(key) for key in fields})


def run_captures(output: Path, max_bytes: int, resume: bool) -> list[dict]:
    _raw_cap, _http_cap, each_cap = raw_budget_parts(max_bytes)
    capture_script = RESEARCH / "public-networks" / "capture_dashboard.py"
    browser_python = RESEARCH / "public-networks" / ".capture-env" / "bin" / "python"
    results = []
    for label, url, hosts in CAPTURES:
        target = output / "captures" / label
        manifest_path = target / "manifest.json"
        if manifest_path.is_file():
            verify_captures(output)
            results.append({"label": label, "url": url, "status": "reused_existing_snapshot" if resume else "preserved_existing_snapshot"})
            continue
        if target.exists():
            results.append({"label": label, "url": url, "status": "existing_without_manifest_not_overwritten"})
            continue
        command = [str(browser_python), str(capture_script), "--url", url, "--output", str(target),
                   "--max-bytes", str(max(1, each_cap)), "--max-response-bytes", str(min(16_000_000, max(1, each_cap))),
                   "--wait-seconds", "5", "--chromium-path", "/usr/sbin/chromium"]
        for host in hosts:
            command.extend(("--allow-host", host))
        completed = subprocess.run(command, check=False, capture_output=True, text=True)
        results.append({"label": label, "url": url, "status": "complete" if completed.returncode == 0 else "failed",
                        "return_code": completed.returncode,
                        "stdout": completed.stdout[-2000:], "stderr": completed.stderr[-2000:]})
    return results


def read_capture_manifests(output: Path) -> list[dict]:
    records = []
    root = output / "captures"
    if not root.exists():
        return records
    for manifest_path in sorted(root.glob("*/manifest.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        document = next((entry for entry in manifest.get("entries", [])
                         if entry.get("path") == "artifacts/initial.html"), {})
        records.append({"label": manifest_path.parent.name, "url": manifest.get("url"),
                        "capture_status": manifest.get("capture_status"),
                        "document_status": document.get("status"),
                        "run_error": manifest.get("run_error"),
                        "saved_entries": sum(1 for e in manifest.get("entries", []) if e.get("path")),
                        "skipped_entries": sum(1 for e in manifest.get("entries", []) if not e.get("path")),
                        "manifest_path": str(manifest_path.relative_to(output))})
    return records


def build_report(entries: list[dict], captures: list[dict], raw_bytes: int, total_bytes: int, max_bytes: int) -> str:
    lines = [
        "# INTELLECT-1 public evidence snapshot",
        "",
        "This archive preserves public, operator-authored material. It is not an independent reproduction of the training run.",
        "",
        "## Source availability",
        "",
        "| Source | HTTP status | Bytes saved | SHA-256 |",
        "| --- | ---: | ---: | --- |",
    ]
    for entry in entries:
        lines.append(f"| [{entry.get('label','source')}]({entry.get('url','')}) | {entry.get('status')} | {entry.get('bytes',0)} | `{entry.get('sha256') or '—'}` |")
    lines.extend(("", "## Public page captures", "", "| Page | Capture result | Evidence manifest |", "| --- | --- | --- |"))
    for capture in captures:
        result = capture.get("capture_status", capture.get("status"))
        if capture.get("document_status") is not None:
            result = f"HTTP {capture['document_status']}; {result}"
        lines.append(f"| {capture['label']} — {capture.get('url')} | {result} | `{capture.get('manifest_path','')}` |")
    lines.extend((
        "", "## What the sources support", "",
        "The official technical report and release page report a 10B-parameter INTELLECT-1 trained on 1T tokens from 10 October through 22 November 2024. They report 30 independent compute providers, up to 14 concurrent nodes, and the paper’s figures show active-node counts and a falling perplexity curve. These are Prime Intellect’s reported measurements; the archived run-time series needed to independently recompute those totals was not found among the linked public sources.",
        "",
        "The report cites the public dashboard at `https://app.primeintellect.ai/intelligence`; the current capture records HTTP 404 for that route. This says nothing about whether the dashboard was public during training. The report figure remains paper evidence, not a substitute for raw event or metric logs.",
        "",
        "No official W&B project or run-history URL was linked in the paper, release post, or preserved historical repository refs. No guessed W&B organization/project endpoints were queried. Current source code and current `main` are not attributed as the exact deployed run version. The full repository clone preserves its history, including the `intellect-1-technical-report` branch; the report-upload commit is later than the run and is not asserted to be the run’s source SHA.",
        "",
        f"Raw HTTP and browser evidence counted here: {raw_bytes} bytes. Total output currently measured: {total_bytes} bytes of {max_bytes} allowed. Model weights and dataset contents were excluded.",
        "",
        "See `http_archive/manifest.json` for HTTP attempts, hashes, and unavailable sources; `captures/*/manifest.json` records browser artifacts, document status, and skipped responses.",
    ))
    return "\n".join(lines) + "\n"


def analyze(output: Path, max_bytes: int) -> dict:
    output = Path(output)
    snapshot = output / "public_sources"
    manifest_path = snapshot / "http_archive" / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Expected HTTP source manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = normalized_entries(manifest)
    raw_http_bytes = verify_raw_archive(snapshot / "http_archive", manifest)
    verify_captures(snapshot)
    browser_bytes = capture_bytes(snapshot)
    raw_bytes = raw_http_bytes + browser_bytes
    write_table(snapshot / "tables" / "sources.csv", entries)
    captures = read_capture_manifests(snapshot)
    attempts_path = snapshot / "capture_attempts.json"
    attempts = json.loads(attempts_path.read_text(encoding="utf-8")) if attempts_path.exists() else []
    preliminary_total = file_bytes(snapshot)
    summary = {"project": "INTELLECT-1", "sources": entries, "captures": captures,
               "capture_attempts": attempts,
               "raw_http_bytes": raw_http_bytes, "browser_evidence_bytes": browser_bytes,
               "raw_evidence_bytes": raw_bytes, "total_output_bytes_before_summary": preliminary_total,
               "max_bytes": max_bytes,
               "unavailable_sources": [e["url"] for e in entries if e.get("status") != 200 or not e.get("path")]}
    save_json(snapshot / "summary.json", summary)
    report_path = snapshot / "report.md"
    reported_total = file_bytes(snapshot)
    for _ in range(3):
        report_path.write_text(build_report(entries, captures, raw_bytes, reported_total, max_bytes), encoding="utf-8")
        observed_total = file_bytes(snapshot)
        if observed_total == reported_total:
            break
        reported_total = observed_total
    total = enforce_budget(snapshot, max_bytes, raw_bytes)
    return {"raw_bytes": raw_bytes, "total_bytes": total, "source_count": len(entries),
            "unavailable_sources": len(summary["unavailable_sources"]), "captures": captures}


def collect(output: Path, max_bytes: int, resume: bool) -> dict:
    output = Path(output)
    snapshot = output / "public_sources"
    snapshot.mkdir(parents=True, exist_ok=True)
    attempts = run_captures(snapshot, max_bytes, resume)
    save_json(snapshot / "capture_attempts.json", attempts)
    raw_cap, http_cap, _each_capture = raw_budget_parts(max_bytes)
    archive = Archive(snapshot / "http_archive", max_bytes=http_cap, resume=resume)
    for label, url in SOURCES:
        archive.fetch(url, label)
    # Failed requests have explicit fields too, so absence is machine-readable.
    archive.manifest["entries"] = normalized_entries(archive.manifest)
    archive.flush()
    result = analyze(output, max_bytes)
    result["raw_cap_bytes"] = raw_cap
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-bytes", type=int, default=MAX_PROJECT_BYTES,
                        help="Total output cap, including raw sources and browser captures")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.max_bytes <= MAX_PROJECT_BYTES:
        parser.error("--max-bytes must be between 1 and 3,000,000,000")
    if not args.analyze_only and args.max_bytes < 1_000_000:
        parser.error("--max-bytes must be at least 1,000,000 for bounded source capture")
    result = analyze(args.output, args.max_bytes) if args.analyze_only else collect(args.output, args.max_bytes, args.resume)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
