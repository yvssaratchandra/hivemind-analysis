#!/usr/bin/env python3
"""Bounded archive of official Psyche run references and observed dashboards."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

PROJECT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT.parent / "public-networks"))
from archive_http import Archive  # noqa: E402

MAX_PROJECT_BYTES = 3_000_000_000
RAW_CAP_BYTES = 1_000_000_000
SOURCES = [
    ("psyche-docs", "https://docs.psyche.network/"),
    ("psyche-announcement", "https://nousresearch.com/nous-psyche"),
    ("psyche-september-update", "https://nousresearch.tech/the-next-phase-of-psyche"),
    ("hermes-43-report", "https://nousresearch.tech/introducing-hermes-4-3"),
    ("consilience-40b-model-metadata", "https://huggingface.co/api/models/PsycheFoundation/consilience-40b-7Y9v38s5"),
    ("consilience-40b-model-card-readme", "https://huggingface.co/PsycheFoundation/consilience-40b-7Y9v38s5/raw/main/README.md"),
    ("hermes-43-model-metadata", "https://huggingface.co/api/models/NousResearch/Hermes-4.3-36B"),
    ("hermes-43-model-card-readme", "https://huggingface.co/NousResearch/Hermes-4.3-36B/raw/main/README.md"),
]


def tree_bytes(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) if root.exists() else 0


def raw_budget(max_bytes: int) -> int:
    if max_bytes <= 0 or max_bytes > MAX_PROJECT_BYTES:
        raise ValueError(f"max_bytes must be in 1..{MAX_PROJECT_BYTES}")
    return min(max_bytes // 2, RAW_CAP_BYTES)


def is_official_api_response(url: str) -> bool:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    path = parsed.path
    official_host = host == "psyche.network" or host.endswith(".psyche.network")
    known_path = (path in ("/api/status", "/api/runs", "/api/contributionInfo") or
                  path.startswith("/api/run/") or path.startswith("/api/contributionInfo/"))
    return official_host and known_path


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
            h = hashlib.sha256()
            with evidence.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    h.update(block)
            if h.hexdigest() != entry.get("sha256"):
                raise ValueError(f"Evidence hash mismatch: {evidence}")
            checked += 1
    return checked


def model_metadata(archive_root: Path, entries: list[dict]) -> list[dict]:
    records = []
    for e in entries:
        if "model-metadata" not in e.get("label", "") or e.get("status") != 200 or not e.get("path"):
            continue
        try:
            raw = json.loads((archive_root / e["path"]).read_text())
        except (OSError, ValueError, UnicodeError):
            continue
        records.append({k: raw.get(k) for k in ("_id", "id", "sha", "lastModified", "private", "gated",
                                                        "pipeline_tag", "downloads", "likes", "tags", "config", "cardData")
                        if k in raw})
    return records


def inventory(output: Path, max_bytes: int, archive_manifest: dict):
    files = []
    for p in sorted(output.rglob("*")):
        if p.is_file() and p.name != "PUBLIC_RUNS_INVENTORY.json":
            h = hashlib.sha256()
            with p.open("rb") as f:
                for block in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(block)
            files.append({"path": str(p.relative_to(output)), "bytes": p.stat().st_size,
                          "sha256": h.hexdigest(), "status": "captured"})
    failed = [{"source": "http", "url": e.get("url"), "label": e.get("label"),
               "status": e.get("status"), "error": e.get("error")}
              for e in archive_manifest.get("entries", []) if not e.get("path") or e.get("status") != 200]
    for observation in browser_observations(output):
        failed.extend({"source": observation.get("requested_url"), **entry}
                      for entry in observation.get("failed_requests", []))
        failed.extend({"source": observation.get("requested_url"), **entry}
                      for entry in observation.get("page_errors", []))
    value = {"schema_version": 1, "project": "nous-psyche", "max_bytes": max_bytes,
             "tree_bytes": tree_bytes(output), "files": files, "failed_paths": failed}
    (output / "PUBLIC_RUNS_INVENTORY.json").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return value


def browser_observations(output: Path) -> list[dict]:
    observations = []
    for manifest_path in sorted(output.glob("dashboard_*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, ValueError):
            continue
        page = {}
        logs = {}
        api_responses = []
        for entry in manifest.get("entries", []):
            url = entry.get("url") or ""
            if is_official_api_response(url) and entry.get("path") and entry.get("status") is not None:
                api_responses.append({"url": url, "status": entry.get("status"),
                                      "bytes": entry.get("bytes")})
            if not entry.get("path"):
                continue
            body_path = manifest_path.parent / entry["path"]
            if entry.get("kind") == "page_summary":
                try:
                    page = json.loads(body_path.read_text())
                except (OSError, ValueError):
                    pass
            if entry.get("kind") == "browser_logs":
                try:
                    logs = json.loads(body_path.read_text())
                except (OSError, ValueError):
                    pass
        observations.append({
            "requested_url": manifest.get("url"), "landed_url": page.get("url"),
            "title": page.get("title"), "visible_text": page.get("visible_text"),
            "capture_status": manifest.get("capture_status"), "run_error": manifest.get("run_error"),
            "created_at": manifest.get("created_at"), "completed_at": manifest.get("completed_at"),
            "failed_requests": logs.get("failed_requests", []), "page_errors": logs.get("page_errors", []),
            "api_responses": api_responses,
        })
    return observations


def collect(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if tree_bytes(output) > args.max_bytes:
        raise RuntimeError("existing output already exceeds --max-bytes")
    verify_existing(output, require_manifest=args.analyze_only)
    archive_root = output / "http"
    manifest = {"entries": []}
    if not args.analyze_only:
        remaining = args.max_bytes - tree_bytes(output)
        raw_limit = min(raw_budget(args.max_bytes), max(0, remaining - 1_000_000))
        if raw_limit < 1:
            raise RuntimeError("no room for bounded HTTP archive under --max-bytes")
        archive = Archive(archive_root, max_bytes=raw_limit, resume=args.resume, delay=1.0)
        for label, url in SOURCES:
            archive.fetch(url, label)
        manifest = archive.manifest
    elif (archive_root / "manifest.json").exists():
        manifest = json.loads((archive_root / "manifest.json").read_text())

    observations = browser_observations(output)
    observed_api = [response for observation in observations for response in observation.get("api_responses", [])]
    api_success = [response for response in observed_api if response.get("status") == 200]
    model_cards = model_metadata(archive_root, manifest.get("entries", []))
    (output / "model_metadata_summary.json").write_text(json.dumps(model_cards, indent=2, sort_keys=True) + "\n")
    summary = {
        "project": "nous-psyche", "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "dashboard_observations": observations,
        "source_verified_api_routes": ["/api/status", "/api/runs", "/api/run/:runId/:index",
                                       "/api/contributionInfo", "/api/contributionInfo/:address"],
        "deployed_api_status": ("some API response captured" if api_success else
                                 "unknown: no successful public API response captured"),
        "observed_api_responses": observed_api,
        "model_metadata_records": len(model_cards), "historical_run_claims_are_operator_reports": True,
        "browser_failed_paths": [failure for observation in observations
                                  for failure in observation.get("failed_requests", []) +
                                  observation.get("page_errors", [])],
        "archive_failures": [{"url": e.get("url"), "label": e.get("label"), "status": e.get("status"),
                              "error": e.get("error")} for e in manifest.get("entries", [])
                             if not e.get("path") or e.get("status") != 200],
        "total_tree_bytes": tree_bytes(output), "max_bytes": args.max_bytes,
    }
    (output / "PUBLIC_RUNS_SUMMARY.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    inv = inventory(output, args.max_bytes, manifest)
    if tree_bytes(output) > args.max_bytes:
        raise RuntimeError("project output exceeded --max-bytes")
    print(json.dumps({"tree_bytes": tree_bytes(output), "files": len(inv["files"]),
                      "metadata_records": len(model_cards), "failed_paths": len(inv["failed_paths"])}, indent=2))


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
