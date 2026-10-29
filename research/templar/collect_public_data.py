#!/usr/bin/env python3
"""Archive official Templar metadata and normalize bounded public dashboard snapshots."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlsplit

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "research" / "public-networks"))
from archive_http import Archive, digest  # noqa: E402

RAW_CAP_DECIMAL = 1_000_000_000
OFFICIAL_GETS = (
    ("covenant72b-paper-pdf", "https://arxiv.org/pdf/2603.08163"),
    ("qwen-pipeline-paper-pdf", "https://arxiv.org/pdf/2601.02360"),
    ("covenant72b-model-metadata", "https://huggingface.co/api/models/1Covenant/Covenant-72B"),
    ("covenant72b-chat-model-metadata", "https://huggingface.co/api/models/1Covenant/Covenant-72B-Chat"),
    ("covenant72b-model-card", "https://huggingface.co/1Covenant/Covenant-72B/raw/main/README.md"),
    ("covenant72b-chat-model-card", "https://huggingface.co/1Covenant/Covenant-72B-Chat/raw/main/README.md"),
)
CSV_FIELDS = {
    "series": ["run_label", "run_id", "snapshot", "source_url", "metric", "series", "x", "y"],
    "benchmarks": ["run_label", "snapshot", "source_url", "task", "score", "global_step", "window", "timestamp"],
    "participants": ["run_label", "run_id", "snapshot", "source_url", "replica", "stages", "tokens_per_second", "cost_training", "cost_round", "node_count"],
    "links": ["run_label", "run_id", "snapshot", "source_url", "participant", "boundary", "source_host", "destination_host", "cross_host", "forward_mbps", "backward_mbps"],
    "scalars": ["run_label", "run_id", "snapshot", "source_url", "metric", "value"],
}


def total_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def bounded_write(root: Path, relative: str, data: bytes, limit: int) -> Path:
    """Atomically write one derived artifact while enforcing total output bytes."""
    target = root / relative
    if not target.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"Unsafe output path: {relative!r}")
    old_size = target.stat().st_size if target.is_file() else 0
    if total_bytes(root) - old_size + len(data) > limit:
        raise ValueError(f"Total output byte budget {limit} would be exceeded by {relative}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name("." + target.name + ".tmp")
    temporary.write_bytes(data)
    temporary.replace(target)
    return target


def json_bytes(data: Any) -> bytes:
    return (json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def csv_bytes(fields: list[str], rows: Iterable[dict[str, Any]]) -> bytes:
    from io import StringIO

    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return stream.getvalue().encode("utf-8")


def finite_number(value: Any) -> float | int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    if number.is_integer() and abs(number) <= 2**53:
        return int(number)
    return number


def bpb_rows(payload: Any, run_label: str = "Covenant 72B", snapshot: str = "", source_url: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    series_list = payload.get("chart", {}).get("series", []) if isinstance(payload, dict) else []
    if not isinstance(series_list, list):
        return rows
    for series in series_list:
        if not isinstance(series, dict):
            continue
        name = str(series.get("name", ""))
        points = series.get("data", [])
        if not isinstance(points, list):
            continue
        for point in points:
            if not isinstance(point, dict):
                continue
            x, y = finite_number(point.get("x")), finite_number(point.get("y"))
            if x is None or y is None:
                continue
            rows.append({
                "run_label": run_label,
                "run_id": "",
                "snapshot": snapshot,
                "source_url": source_url,
                "metric": "bits_per_byte",
                "series": name,
                "x": x,
                "y": y,
            })
    return rows


def benchmark_rows(payload: Any, run_label: str = "Covenant 72B", snapshot: str = "", source_url: str = "") -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("benchmarks"), list):
        return []
    rows = []
    for item in payload["benchmarks"]:
        if not isinstance(item, dict):
            continue
        score = finite_number(item.get("score"))
        step = finite_number(item.get("global_step"))
        if score is None:
            continue
        rows.append({
            "run_label": run_label,
            "snapshot": snapshot,
            "source_url": source_url,
            "task": item.get("task", ""),
            "score": score,
            "global_step": step,
            "window": finite_number(item.get("window")),
            "timestamp": item.get("timestamp", ""),
        })
    return rows


def _series_rows(
    series_name: str,
    points: Any,
    *,
    run_label: str,
    run_id: str,
    snapshot: str,
    source_url: str,
    metric: str,
) -> list[dict[str, Any]]:
    if not isinstance(points, list):
        return []
    result = []
    for point in points:
        if not isinstance(point, dict):
            continue
        x, y = finite_number(point.get("x")), finite_number(point.get("y"))
        if x is None or y is None:
            continue
        result.append({
            "run_label": run_label,
            "run_id": run_id,
            "snapshot": snapshot,
            "source_url": source_url,
            "metric": metric,
            "series": series_name,
            "x": x,
            "y": y,
        })
    return result


def snapshot_records(snapshot: Path) -> tuple[str, dict[str, tuple[dict[str, Any], Any]], list[str]]:
    """Return page text, verified JSON responses keyed by exact URL, and gaps."""
    manifest_path = snapshot / "manifest.json"
    gaps: list[str] = []
    if not manifest_path.is_file():
        return "", {}, [f"missing snapshot manifest: {snapshot}"]
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError) as error:
        return "", {}, [f"unreadable snapshot manifest {snapshot}: {type(error).__name__}"]
    summary_path = snapshot / "artifacts/page_summary.json"
    page_text = ""
    if summary_path.is_file():
        try:
            page_text = str(json.loads(summary_path.read_text(encoding="utf-8")).get("visible_text", ""))
        except (OSError, ValueError, UnicodeError):
            gaps.append(f"unreadable page summary: {summary_path}")
    payloads: dict[str, tuple[dict[str, Any], Any]] = {}
    for entry in manifest.get("entries", []):
        url, relative = entry.get("url"), entry.get("path")
        if entry.get("kind") != "http_response" or not url or not relative:
            continue
        path = (snapshot / relative).resolve()
        if not path.is_relative_to(snapshot.resolve()) or not path.is_file():
            gaps.append(f"missing or unsafe response path in {snapshot.name}: {relative}")
            continue
        body = path.read_bytes()
        if len(body) != entry.get("bytes") or hashlib.sha256(body).hexdigest() != entry.get("sha256"):
            gaps.append(f"hash/size mismatch for {snapshot.name}:{relative}")
            continue
        content_type = str(entry.get("content_type", ""))
        if "json" not in content_type and not body.lstrip().startswith((b"{", b"[")):
            continue
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeError):
            continue
        # Keep the last response for a repeated polling URL; counts remain in the manifest.
        payloads[url] = (entry, payload)
    return page_text, payloads, gaps


def _run_context(snapshot: Path, page_text: str, url: str) -> tuple[str, str]:
    query = parse_qs(urlsplit(url).query)
    run_ids = query.get("run_id", [""])
    run_id = run_ids[0]
    if run_id:
        return ("QWEN30_MOE_003" if run_id.startswith("qwen") else run_id), run_id
    if "/api/model-training" in url or "/api/benchmarks" in url:
        # The open run-selector also contains the label "Covenant 72B" while
        # QWEN30_MOE_003 remains selected. Require the model-size panel rendered
        # by the selected 72B view, not merely a menu option.
        if "MODEL SIZE: 72B" in page_text:
            return "Covenant 72B", ""
    if "QWEN30_MOE_003" in page_text:
        return "QWEN30_MOE_003", ""
    return snapshot.name, ""


def normalize_snapshots(snapshots: list[Path]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    rows = {key: [] for key in CSV_FIELDS}
    gaps: list[str] = []
    coverage: list[dict[str, Any]] = []
    for snapshot in snapshots:
        page_text, payloads, snapshot_gaps = snapshot_records(snapshot)
        gaps.extend(snapshot_gaps)
        manifest_path = snapshot / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else {}
        coverage.append({
            "snapshot": str(snapshot),
            "capture_status": manifest.get("capture_status", "unknown"),
            "saved_bytes": manifest.get("total_saved_bytes"),
            "verified_json_responses": len(payloads),
            "blocked_requests": len(manifest.get("blocked_requests", [])),
            "page_title": json.loads((snapshot / "artifacts/page_summary.json").read_text(encoding="utf-8")).get("title") if (snapshot / "artifacts/page_summary.json").is_file() else None,
        })
        for url, (entry, payload) in payloads.items():
            route = urlsplit(url).path
            # The model-training API can return legacy 72B data even while the
            # Qwen pipeline page is selected. Only normalize it when the
            # rendered snapshot explicitly shows the 72B view.
            if route in ("/api/model-training", "/api/benchmarks") and "MODEL SIZE: 72B" not in page_text:
                continue
            run_label, run_id = _run_context(snapshot, page_text, url)
            common = {"run_label": run_label, "run_id": run_id, "snapshot": snapshot.name, "source_url": url}
            if route == "/api/model-training":
                rows["series"].extend(bpb_rows(payload, run_label, snapshot.name, url))
                continue
            if route == "/api/benchmarks":
                rows["benchmarks"].extend(benchmark_rows(payload, run_label, snapshot.name, url))
                continue
            if route in ("/api/pipeline/loss", "/api/pipeline/tokens") and isinstance(payload, dict):
                metric = "global_loss" if route.endswith("/loss") else "tokens_processed"
                for series in payload.get("series", []):
                    if isinstance(series, dict):
                        rows["series"].extend(_series_rows(
                            str(series.get("name", "")), series.get("data"), metric=metric, **common
                        ))
            elif route == "/api/pipeline/mfu" and isinstance(payload, dict):
                for name in ("training", "round"):
                    rows["series"].extend(_series_rows(name, payload.get(name), metric="mfu_percent", **common))
                if finite_number(payload.get("taxPct")) is not None:
                    rows["scalars"].append({**common, "metric": "mfu_tax_percent", "value": finite_number(payload["taxPct"])})
            elif route == "/api/pipeline/orchestrator" and isinstance(payload, dict):
                rows["series"].extend(_series_rows("round_duration", payload.get("roundDuration"), metric="round_duration_seconds", **common))
            elif route == "/api/pipeline/participants" and isinstance(payload, dict):
                for participant in payload.get("participants", []):
                    if not isinstance(participant, dict):
                        continue
                    rows["participants"].append({
                        **common,
                        "replica": participant.get("replica", ""),
                        "stages": finite_number(participant.get("stages")),
                        "tokens_per_second": finite_number(participant.get("tps")),
                        "cost_training": finite_number(participant.get("costTraining")),
                        "cost_round": finite_number(participant.get("costRound")),
                        "node_count": len(participant.get("nodes", [])) if isinstance(participant.get("nodes"), list) else None,
                    })
            elif route == "/api/pipeline/pp-links" and isinstance(payload, dict):
                for participant in payload.get("participants", []):
                    if not isinstance(participant, dict):
                        continue
                    for link in participant.get("links", []):
                        if not isinstance(link, dict):
                            continue
                        rows["links"].append({
                            **common,
                            "participant": participant.get("participant", ""),
                            "boundary": link.get("boundary", ""),
                            "source_host": link.get("srcHost", ""),
                            "destination_host": link.get("dstHost", ""),
                            "cross_host": link.get("crossHost"),
                            "forward_mbps": finite_number(link.get("fwd")),
                            "backward_mbps": finite_number(link.get("bwd")),
                        })
            elif route == "/api/pipeline/bandwidth" and isinstance(payload, dict):
                for node in payload.get("nodes", []):
                    if not isinstance(node, dict):
                        continue
                    rows["scalars"].append({**common, "metric": f"bandwidth_down_uid_{node.get('uid', '')}", "value": finite_number(node.get("down"))})
                    rows["scalars"].append({**common, "metric": f"bandwidth_up_uid_{node.get('uid', '')}", "value": finite_number(node.get("up"))})
            elif route == "/api/pipeline/overview" and isinstance(payload, dict):
                for key, value in payload.items():
                    numeric = finite_number(value)
                    if numeric is not None:
                        rows["scalars"].append({**common, "metric": key, "value": numeric})
            elif route == "/api/evaluated-miners" and isinstance(payload, dict):
                count = finite_number(payload.get("count"))
                if count is not None:
                    rows["scalars"].append({**common, "metric": "evaluated_miners_count_only", "value": count})
    duplicate_rows_collapsed = 0
    for key, category in rows.items():
        seen: set[str] = set()
        unique_rows = []
        for row in category:
            # The same completed run can appear in more than one public
            # dashboard capture. Preserve the first source location while
            # avoiding repeated values being counted as additional samples.
            identity = json.dumps(
                {field: value for field, value in row.items() if field not in ("snapshot", "source_url")},
                sort_keys=True,
                separators=(",", ":"),
            )
            if identity in seen:
                duplicate_rows_collapsed += 1
                continue
            seen.add(identity)
            unique_rows.append(row)
        rows[key] = unique_rows
    summary = {
        "snapshots": coverage,
        "series_rows": len(rows["series"]),
        "bpb_rows": sum(row["metric"] == "bits_per_byte" for row in rows["series"]),
        "benchmark_rows": len(rows["benchmarks"]),
        "participant_rows": len(rows["participants"]),
        "pipeline_link_rows": len(rows["links"]),
        "scalar_rows": len(rows["scalars"]),
        "duplicate_rows_collapsed_across_captures": duplicate_rows_collapsed,
        "source_gaps": gaps,
    }
    summary["benchmark_global_steps"] = sorted({
        row["global_step"] for row in rows["benchmarks"] if row.get("global_step") is not None
    })
    bpb = [row for row in rows["series"] if row["metric"] == "bits_per_byte"]
    if bpb:
        # Keep snapshot provenance in CSV while avoiding duplicate dashboard
        # captures inflating the derived endpoint summary.
        unique_points = {(row["series"], row["x"], row["y"]) for row in bpb}
        points = sorted(
            ({"series": series, "x": x, "y": y} for series, x, y in unique_points),
            key=lambda row: finite_number(row["x"]) or 0,
        )
        summary["covenant72b_bpb"] = {
            "run_label": "Covenant 72B",
            "points": len(points),
            "captured_rows_with_snapshot_provenance": len(bpb),
            "first_outer_step": points[0]["x"],
            "first_value": points[0]["y"],
            "last_outer_step": points[-1]["x"],
            "last_value": points[-1]["y"],
            "min_value": min(float(row["y"]) for row in points),
            "max_value": max(float(row["y"]) for row in points),
        }
    return rows, summary


def write_inventory(project_dir: Path, batch_root: Path) -> dict[str, Any]:
    files = []
    if batch_root.exists():
        for path in sorted(p for p in batch_root.rglob("*") if p.is_file() and ".tmp" not in p.name):
            rel = path.relative_to(project_dir).as_posix()
            files.append({"path": rel, "bytes": path.stat().st_size, "sha256": digest(path)})
    result = {"schema_version": 1, "project": "templar", "batch_root": batch_root.relative_to(project_dir).as_posix(), "files": files, "file_count": len(files), "total_bytes": sum(row["bytes"] for row in files)}
    target = project_dir / "INVENTORY.json"
    target.write_bytes(json_bytes(result))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Batch root (includes browser captures, http/, and derived files)")
    parser.add_argument("--max-bytes", type=int, required=True, help="Maximum total bytes in --output, raw and derived")
    parser.add_argument("--resume", action="store_true", help="Verify and reuse existing raw response files")
    parser.add_argument("--analyze-only", action="store_true", help="Regenerate normalized tables from existing snapshots without network access")
    parser.add_argument("--snapshot", action="append", type=Path, help="Override discovered dashboard snapshots; repeat to add snapshots")
    args = parser.parse_args(argv)
    if not 4096 <= args.max_bytes <= 3_000_000_000:
        parser.error("--max-bytes must be between 4096 and 3000000000 (3 GB decimal)")
    output = args.output.resolve()
    data_root = (PROJECT_DIR / "data").resolve()
    if not output.is_relative_to(data_root):
        parser.error("--output must be a batch directory under research/templar/data/")
    output.mkdir(parents=True, exist_ok=True)
    http_output = output / "http"
    if args.analyze_only and not (http_output / "manifest.json").is_file():
        parser.error("--analyze-only requires an existing http/manifest.json")
    existing_bytes = total_bytes(output)
    if existing_bytes >= args.max_bytes:
        parser.error("Existing batch output already meets or exceeds --max-bytes")
    reserve = min(2_000_000, max(4096, args.max_bytes // 20))
    raw_cap = min(args.max_bytes // 2, RAW_CAP_DECIMAL, args.max_bytes - existing_bytes - reserve)
    if raw_cap < 1:
        parser.error("No remaining byte budget for raw source responses")
    try:
        archive = Archive(http_output, max_bytes=raw_cap, resume=args.resume or args.analyze_only, delay=0.5)
        if args.analyze_only and not archive.manifest.get("entries"):
            raise ValueError("No saved HTTP evidence is available to analyze")
        if not args.analyze_only:
            for label, url in OFFICIAL_GETS:
                archive.fetch(url, label)
        if args.snapshot:
            snapshots = [path.resolve() for path in args.snapshot]
            if any(not path.is_relative_to(output) for path in snapshots):
                parser.error("Every --snapshot must be inside --output so its bytes count against the cap")
        else:
            snapshots = []
            for manifest_path in sorted(output.rglob("manifest.json")):
                if http_output in manifest_path.parents:
                    continue
                try:
                    manifest_doc = json.loads(manifest_path.read_text(encoding="utf-8"))
                except (OSError, ValueError, UnicodeError):
                    continue
                if manifest_doc.get("capture_status") and any(
                    item.get("kind") == "http_response" for item in manifest_doc.get("entries", [])
                ):
                    snapshots.append(manifest_path.parent.resolve())
        if not snapshots:
            snapshot_gap = f"No browser dashboard snapshots discovered under batch root {output}; capture the public dashboard into this batch before analysis."
        else:
            snapshot_gap = None
        normalized, summary = normalize_snapshots(snapshots)
        if snapshot_gap:
            summary["source_gaps"].append(snapshot_gap)
        for key, fields in CSV_FIELDS.items():
            bounded_write(output, f"normalized/templar_{key}.csv", csv_bytes(fields, normalized[key]), args.max_bytes)
        summary.update({"total_output_limit_bytes": args.max_bytes, "raw_limit_bytes": raw_cap, "output_before_summary_bytes": total_bytes(output)})
        bounded_write(output, "summary.json", json_bytes(summary), args.max_bytes)
        report = [
            "# Templar public data capture",
            "",
            "The `Covenant 72B` dashboard view and the current `QWEN30_MOE_003` pipeline view are separate. The normalized series file retains the source snapshot, exact URL, run label, and the observed Qwen `run_id`.",
            "",
            f"- Dashboard snapshots: {len(summary['snapshots'])}",
            f"- Covenant 72B BPB points: {summary['bpb_rows']}",
            f"- Benchmark rows: {summary['benchmark_rows']}",
            f"- Qwen pipeline time-series points: {summary['series_rows'] - summary['bpb_rows']}",
            f"- Current-pipeline participant summaries: {summary['participant_rows']}",
            f"- Current-pipeline stage-link rows: {summary['pipeline_link_rows']}",
            "",
            (f"The selected 72B dashboard capture contains {summary['covenant72b_bpb']['points']} distinct BPB-by-outer-step points: {summary['covenant72b_bpb']['first_value']} at step {summary['covenant72b_bpb']['first_outer_step']} through {summary['covenant72b_bpb']['last_value']} at step {summary['covenant72b_bpb']['last_outer_step']}. The selected dashboard returns {summary['benchmark_rows']} benchmark rows at global step(s) {', '.join(map(str, summary['benchmark_global_steps'])) or 'not reported'} in this capture." if summary.get("covenant72b_bpb") else "The selected 72B dashboard did not provide a parseable BPB series in this batch."),
            "",
            "The static Covenant-72B paper and the dashboard API are separate observations: the paper's plotted BPB curve ends around outer step 6190, while the dashboard response captured on 2026-10-29 ends at 6410. The report does not combine the two points or treat the dashboard endpoint as the paper's final step.",
            "",
            "The 72B dashboard reports `BPB` at outer steps. Its separate evaluated-miner API returns a count, while the visible incentive table has no miner rows in the captured state. These are not individual contribution measurements.",
            "",
            "The archived Covenant-72B paper includes static plots of aggregate contributing peers and cumulative unique participation over training. They are participation/contribution-count curves, not per-miner score exports; this archive preserves the PDF but does not digitize its plotted points.",
            "",
            "The Qwen pipeline APIs provide global loss/tokens/MFU/round-duration series plus replica and pipeline-link records. The dashboard API responses captured after selecting 72B still include a Qwen `run_id`; those records remain labeled Qwen.",
            "",
            "Identical rows repeated across captures are collapsed by run/metric/value, with the first source snapshot retained in each normalized row. Capture totals remain in `summary.json`; repeated browser views are not counted as independent training samples.",
            "",
            "Static official-source responses in `http/raw/` include the [Covenant-72B report](https://arxiv.org/pdf/2603.08163), the [separate Qwen pipeline paper](https://arxiv.org/pdf/2601.02360), and first-party Hugging Face model metadata/cards. The README identifies Crusades as a separate subnet activity; no Crusades performance is attributed to the 72B run. No model weights or datasets were requested.",
            "",
            "See `summary.json` for coverage and source gaps; see `manifest.json` for exact URLs, statuses, sizes, and hashes.",
            "",
        ]
        bounded_write(output, "reports/public-data.md", "\n".join(report).encode("utf-8"), args.max_bytes)
        failures = [entry for entry in archive.manifest.get("entries", []) if entry.get("status") != 200 or not entry.get("path")]
        archive.manifest.update({
            "collection_status": "complete_with_source_gaps" if failures or summary["source_gaps"] else "complete",
            "raw_limit_bytes": raw_cap,
            "total_output_limit_bytes": args.max_bytes,
            "batch_root": str(output),
            "http_archive_path": "http/",
            "normalized_snapshot_paths": [str(path) for path in snapshots],
            "normalized_output": "summary.json",
        })
        archive.flush()
        if total_bytes(output) > args.max_bytes:
            raise ValueError("Final collector output exceeded --max-bytes")
        write_inventory(PROJECT_DIR, output)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    except Exception as error:
        print(f"collection failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
