#!/usr/bin/env python3
"""Collect bounded, public INTELLECT-2 release and training evidence."""

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
    ("dashboard", "https://app.primeintellect.ai/intellect-2", ()),
    ("release_page", "https://www.primeintellect.ai/blog/intellect-2-release", ("cdn.prod.website-files.com",)),
)
SOURCES = (
    ("arxiv_paper_html", "https://arxiv.org/html/2505.07291"),
    ("arxiv_paper_pdf", "https://arxiv.org/pdf/2505.07291"),
    ("official_technical_report_pdf", "https://storage.googleapis.com/public-technical-paper/INTELLECT_2_Technical_Report.pdf"),
    ("release_blog_html", "https://www.primeintellect.ai/blog/intellect-2-release"),
    ("model_metadata", "https://huggingface.co/api/models/PrimeIntellect/INTELLECT-2"),
    ("model_card", "https://huggingface.co/PrimeIntellect/INTELLECT-2/raw/main/README.md"),
    ("training_dataset_metadata", "https://huggingface.co/api/datasets/PrimeIntellect/INTELLECT-2-RL-Dataset"),
    ("toploc_repo_page", "https://github.com/PrimeIntellect-ai/toploc"),
    ("protocol_repo_page", "https://github.com/PrimeIntellect-ai/protocol"),
    ("shardcast_repo_availability_check", "https://github.com/PrimeIntellect-ai/shardcast"),
)


def raw_budget_parts(max_bytes: int, capture_count: int = len(CAPTURES)) -> tuple[int, int, int]:
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    raw_cap = min(max_bytes // 2, MAX_RAW_BYTES)
    browser_total = raw_cap // 2
    return raw_cap, raw_cap - browser_total, browser_total // max(1, capture_count)


def normalized_entries(manifest: dict) -> list[dict]:
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
    captures = Path(root) / "captures"
    if not captures.exists():
        return 0
    return sum(path.stat().st_size for path in captures.rglob("*")
               if path.is_file() and path.name != "manifest.json")


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


def verified_http_bytes(archive_root: Path, manifest: dict) -> int:
    total = 0
    for entry in normalized_entries(manifest):
        if not entry["path"]:
            continue
        path = archive_root / entry["path"]
        if not path.is_file() or path.stat().st_size != entry["bytes"] or digest(path) != entry["sha256"]:
            raise ValueError(f"Evidence hash/size mismatch: {entry['path']}")
        total += entry["bytes"]
    return total


def run_captures(output: Path, max_bytes: int, resume: bool) -> list[dict]:
    _raw_cap, _http_cap, each_cap = raw_budget_parts(max_bytes)
    script = RESEARCH / "public-networks" / "capture_dashboard.py"
    browser_python = RESEARCH / "public-networks" / ".capture-env" / "bin" / "python"
    results = []
    for label, url, hosts in CAPTURES:
        target = output / "captures" / label
        manifest_path = target / "manifest.json"
        if manifest_path.is_file():
            verify_captures(output)
            results.append({"label": label, "url": url,
                            "status": "reused_existing_snapshot" if resume else "preserved_existing_snapshot"})
            continue
        if target.exists():
            results.append({"label": label, "url": url, "status": "existing_without_manifest_not_overwritten"})
            continue
        command = [str(browser_python), str(script), "--url", url, "--output", str(target),
                   "--max-bytes", str(max(1, each_cap)), "--max-response-bytes", str(min(16_000_000, max(1, each_cap))),
                   "--wait-seconds", "5", "--chromium-path", "/usr/sbin/chromium"]
        for host in hosts:
            command.extend(("--allow-host", host))
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        results.append({"label": label, "url": url, "status": "complete" if result.returncode == 0 else "failed",
                        "return_code": result.returncode, "stdout": result.stdout[-2000:],
                        "stderr": result.stderr[-2000:]})
    return results


def capture_summaries(output: Path) -> list[dict]:
    results = []
    captures = output / "captures"
    if not captures.exists():
        return results
    for path in sorted(captures.glob("*/manifest.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        document = next((entry for entry in manifest.get("entries", [])
                         if entry.get("path") == "artifacts/initial.html"), {})
        results.append({"label": path.parent.name, "url": manifest.get("url"),
                        "capture_status": manifest.get("capture_status"),
                        "document_status": document.get("status"), "run_error": manifest.get("run_error"),
                        "saved_entries": sum(bool(e.get("path")) for e in manifest.get("entries", [])),
                        "skipped_entries": sum(not e.get("path") for e in manifest.get("entries", [])),
                        "manifest_path": str(path.relative_to(output))})
    return results


def write_sources_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ("label", "url", "status", "bytes", "sha256", "path", "error", "final_url")
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: row.get(field) for field in fields} for row in rows)


def build_report(rows: list[dict], captures: list[dict], raw_bytes: int, total_bytes: int, max_bytes: int) -> str:
    lines = ["# INTELLECT-2 public evidence snapshot", "",
             "This archive preserves Prime Intellect’s public, operator-authored material. It is not an independent reproduction of the run.",
             "", "## Source availability", "", "| Source | HTTP status | Bytes saved | SHA-256 |",
             "| --- | ---: | ---: | --- |"]
    for row in rows:
        lines.append(f"| [{row.get('label','source')}]({row.get('url','')}) | {row.get('status')} | {row.get('bytes',0)} | `{row.get('sha256') or '—'}` |")
    lines += ["", "## Public page captures", "", "| Page | Capture result | Evidence manifest |", "| --- | --- | --- |"]
    for capture in captures:
        result = capture.get("capture_status", capture.get("status"))
        if capture.get("document_status") is not None:
            result = f"HTTP {capture['document_status']}; {result}"
        lines.append(f"| {capture['label']} — {capture.get('url')} | {result} | `{capture.get('manifest_path','')}` |")
    lines += [
        "", "## What the sources support", "",
        "The paper and official release post describe INTELLECT-2 as a 32B-parameter model trained through globally distributed asynchronous RL. They report two runs (TARGET-SHORT and TARGET-LONG), increasing task rewards, and falling length penalties. Figure 12 provides smoothed reward/penalty curves; the paper says length penalties did not converge within the run window. These are the authors’ reported results. No raw metric history or publicly linked W&B run export was identified in the paper, release post, model card, or linked source repositories.",
        "",
        "The release post and paper link `prime-rl`, TOPLOC, and the Prime protocol stack. The paper reports node timing, communication, verification, and reward metrics, but does not provide an independently auditable node-by-node contribution ledger in the saved public materials. The archived protocol repository’s current README names INTELLECT-2 as a powered run; this does not pin the source version used at training time.",
        "",
        "The preserved `prime-rl` clone is a full-history checkout of evolving `main`; its first stable `v0.1` tag is dated after the May 2025 report. Neither `main` nor that later release is claimed to be the exact training SHA. No public dashboard URL or W&B project/run URL is linked from the official paper or release post. The app route referenced by the protocol README currently returns HTTP 404 in the public-only capture below.",
        "",
        "The official release post mentions SHARDCAST as a component, but does not identify a public source repository. The guessed `PrimeIntellect-ai/shardcast` URL returned 404 in the single availability check; no repository was cloned and no endpoint scan was performed.",
        "",
        f"Raw HTTP and browser evidence counted here: {raw_bytes} bytes. Total output currently measured: {total_bytes} bytes of {max_bytes} allowed. Model weights and dataset contents were excluded; the HF dataset API request is metadata only.",
        "",
        "See `http_archive/manifest.json` for HTTP attempt status, saved bytes, and hashes; each browser capture under `captures/` has its own manifest with document status and skipped responses. The captured figures and paper are not raw training telemetry.",
    ]
    return "\n".join(lines) + "\n"


def analyze(output: Path, max_bytes: int) -> dict:
    output = Path(output)
    snapshot = output / "public_sources"
    http_root = snapshot / "http_archive"
    manifest_path = http_root / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Expected HTTP source manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = normalized_entries(manifest)
    http_bytes = verified_http_bytes(http_root, manifest)
    verify_captures(snapshot)
    browser_bytes = capture_bytes(snapshot)
    raw_bytes = http_bytes + browser_bytes
    write_sources_csv(snapshot / "tables" / "sources.csv", rows)
    captures = capture_summaries(snapshot)
    attempts_path = snapshot / "capture_attempts.json"
    attempts = json.loads(attempts_path.read_text(encoding="utf-8")) if attempts_path.exists() else []
    summary = {"project": "INTELLECT-2", "sources": rows, "captures": captures, "capture_attempts": attempts,
               "raw_http_bytes": http_bytes, "browser_evidence_bytes": browser_bytes,
               "raw_evidence_bytes": raw_bytes, "max_bytes": max_bytes,
               "unavailable_sources": [row["url"] for row in rows if row.get("status") != 200 or not row.get("path")]}
    save_json(snapshot / "summary.json", summary)
    report_path = snapshot / "report.md"
    reported_total = file_bytes(snapshot)
    for _ in range(3):
        report_path.write_text(build_report(rows, captures, raw_bytes, reported_total, max_bytes), encoding="utf-8")
        observed_total = file_bytes(snapshot)
        if observed_total == reported_total:
            break
        reported_total = observed_total
    total = enforce_budget(snapshot, max_bytes, raw_bytes)
    return {"raw_bytes": raw_bytes, "total_bytes": total, "source_count": len(rows),
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
