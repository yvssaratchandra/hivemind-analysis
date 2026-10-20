"""Experiment orchestration and reproducible evidence capture."""
from dataclasses import asdict
from datetime import datetime, timezone
import csv
import hashlib
import importlib.metadata
import json
from pathlib import Path
import resource
import subprocess
import tarfile
import time

from .config import make_labels, make_profiles, resolve
from .discovery import Discovery
from .engine import Engine
from .network import Network
from .observer import Observer
from .protocols.hivemind.adapter import HivemindAdapter
from .workload import Workload

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = ROOT.parents[1]


def write_json(path: Path, value) -> None:
    """Write an inspectable JSON artifact with finite values only."""
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False)+"\n")


def source_manifest() -> dict:
    paths = list((ROOT/"src").rglob("*.py"))+list((ROOT/"configs").glob("*.json"))
    paths += [ROOT/"pyproject.toml", ROOT/"requirements.lock",
              REPOSITORY/"research/hivemind/simulator_native_validation.py"]
    native = REPOSITORY/"hivemind/averaging"
    paths += [native/name for name in ("matchmaking.py", "key_manager.py", "allreduce.py", "partition.py", "averager.py")]
    hashes = {str(p.relative_to(REPOSITORY)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPOSITORY, capture_output=True, text=True, check=True)
    return dict(revision=revision.stdout.strip(), source_hashes=hashes,
                dependencies={name: importlib.metadata.version(name) for name in ("trainsim", "simpy")})


def _safe_label(label: str) -> str:
    if not label or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in label):
        raise ValueError("Label must contain only letters, digits, underscore and hyphen")
    return label


def run(config: dict, seed: int, label: str, results_root: Path | None = None) -> Path:
    """Run one scenario; preserve errors and never overwrite prior evidence."""
    c = resolve(config)
    if c["protocol"] == "agora":
        from .protocols.agora.runner import run as run_agora
        return run_agora(c, seed, label, results_root)
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    path = (results_root or ROOT/"results")/_safe_label(label)
    path.mkdir(parents=True, exist_ok=False)
    profiles = make_profiles(c, seed)
    labels = make_labels(profiles, seed)
    write_json(path/"resolved_config.json", dict(c, seed=seed))
    write_json(path/"profiles.json", {n: dict(asdict(p), peer_id=p.peer_id.hex()) for n, p in profiles.items()})
    manifest = dict(source_manifest(), seed=seed, label=label, status="running",
                    labels={k: sorted(v) for k, v in labels.items()},
                    started_utc=datetime.now(timezone.utc).isoformat(),
                    assumptions=["synthetic resources", "endpoint fluid network", "abstract discovery",
                                 "bounded chunk streams", "independent batch workload", "not convergence"])
    write_json(path/"manifest.json", manifest)
    with tarfile.open(path/"sources.tar.gz", "w:gz") as archive:
        for relative in manifest["source_hashes"]:
            archive.add(REPOSITORY/relative, arcname=relative, recursive=False)
    observer = Observer(path/"events.jsonl.gz", labels=labels)
    observer.target_work = c["population"]*c["target_per_node"]
    engine = Engine(seed=seed, sink=observer.emit)
    network = Network(engine, profiles, registry_latency_seconds=c["registry_latency_seconds"])
    discovery = Discovery(engine, network)
    workload = None
    adapter = HivemindAdapter(engine, network, discovery, profiles, c,
                             on_terminal=lambda node, outcome: workload.terminal(node, outcome))
    workload = Workload(engine, profiles, c, adapter)
    # Stop condition is checked after each visible event, without polling in time.
    def emit(t, kind, actor="", **data):
        observer.emit(t, kind, actor, **data)
        if observer.target_time is not None and t >= c["observation_seconds"]:
            engine.stop_requested = True
    engine.sink = emit
    started = time.perf_counter()
    error = None
    try:
        engine.emit("run_started", "", config=c, seed=seed,
                    target_work=observer.target_work,
                    labels={k: sorted(v) for k, v in labels.items()})
        workload.start()
        engine.schedule(c["observation_seconds"], engine.emit, "observation_boundary")
        engine.run(c["horizon_seconds"])
        status = "completed" if observer.target_time is not None else "censored"
    except KeyboardInterrupt:
        status = "interrupted"
    except Exception as exc:
        status = "error"
        error = dict(type=type(exc).__name__, message=str(exc))
    finally:
        engine.stop_requested = True
        try:
            workload.stop()
        except Exception as exc:
            status = "error"
            error = dict(type=type(exc).__name__, message=f"Cleanup: {exc}")
        try:
            engine.emit("run_finished", "", status=status, error=error)
        finally:
            observer.close()
    summary = observer.summary()
    summary.update(status=status, target_time=observer.target_time, final_time=engine.now,
                   population=c["population"], profile=c["profile"], seed=seed,
                   elapsed_wall_seconds=time.perf_counter()-started,
                   process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                   trace_bytes=(path/"events.jsonl.gz").stat().st_size)
    write_json(path/"summary.json", summary)
    manifest.update(status=status, finished_utc=datetime.now(timezone.utc).isoformat(), error=error)
    write_json(path/"manifest.json", manifest)
    analyze(path)
    if error:
        raise RuntimeError(f"Run failed; evidence retained at {path}: {error}")
    if status == "interrupted":
        raise KeyboardInterrupt
    return path


def _write_csv(path: Path, rows) -> None:
    if isinstance(rows, dict):
        rows = [dict(node_id=key, **value) for key, value in rows.items()]
    rows = list(rows)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list, tuple)) else v
                             for k, v in row.items()})


def analyze(path: Path) -> dict:
    """Replay evidence independently and write tabular and human reports."""
    if json.loads((path/"resolved_config.json").read_text()).get("protocol") == "agora":
        from .protocols.agora.runner import analyze as analyze_agora
        return analyze_agora(path)
    from .analysis.report import analyze_trace
    manifest = json.loads((path/"manifest.json").read_text())
    snapshot = path/"sources.tar.gz"
    if snapshot.exists():
        with tarfile.open(snapshot, "r:gz") as archive:
            for relative, expected in manifest["source_hashes"].items():
                entry = archive.extractfile(relative)
                if entry is None or hashlib.sha256(entry.read()).hexdigest() != expected:
                    raise RuntimeError(f"Source snapshot checksum mismatch: {relative}")
    c = json.loads((path/"resolved_config.json").read_text())
    data = analyze_trace(path/"events.jsonl.gz", labels=manifest["labels"],
                         target_work=c["population"]*c["target_per_node"])
    online = json.loads((path/"summary.json").read_text())
    offline = data["summary"]
    online_credit = online.get("accepted_work", online.get("accepted_work_units", 0))
    offline_credit = offline.get("accepted_work", offline.get("accepted_work_units", 0))
    if (online_credit != offline_credit or online.get("target_time") != offline.get("target_time")
            or online.get("accepted_work_at_observation") != offline.get("accepted_work_at_observation")):
        raise RuntimeError("Online/offline work accounting mismatch")
    _write_csv(path/"per_node.csv", data["per_node"])
    _write_csv(path/"groups.csv", data["groups"])
    _write_csv(path/"effective_sets.csv", data.get("effective_sets", []))
    write_json(path/"replay.json", data)
    report = ["# Simulated coordination workload", "",
              f"Status: {online['status']}. Population: {c['population']}; profile: {c['profile']}; seed: {c['seed']}.",
              f"Accepted work: {online_credit}; target time: {online.get('target_time')}; final time: {online['final_time']} s.",
              "", "This is an independent-batch simulation with synthetic resources. It does not measure convergence.",
              "Native validation status is recorded separately; this report does not assert protocol fidelity.",
              "", f"Wall runtime: {online['elapsed_wall_seconds']:.3f} s; peak process RSS: {online['process_peak_rss_kib']} KiB; trace: {online['trace_bytes']} bytes.",
              "", "Online/offline unique-work accounting agrees. See per_node.csv, groups.csv and replay.json for evidence."]
    (path/"report.md").write_text("\n".join(report)+"\n")
    return data


def sweep(config: dict, label: str, results_root: Path | None = None,
          validation: Path | None = None, diagnostic: bool = False) -> Path:
    """Serial paired baseline matrix, retaining each child run independently."""
    c = resolve(config)
    if c["protocol"] == "agora":
        raise ValueError("Agora seed expansion is pending review of the single-stage pilot; use run for an individual scenario")
    if not diagnostic:
        if c["policies"]:
            raise ValueError("Validated baseline sweeps require empty policies (all participants honest)")
        if validation is None:
            raise ValueError("Baseline sweep requires --validation from validate-native-suite; use --diagnostic for explicitly unvalidated work")
        report = json.loads(validation.read_text())
        if not report.get("passed") or report.get("kind") != "held_out_native_suite":
            raise ValueError("Native held-out validation gates have not passed")
        if report.get("source_hashes") != source_manifest()["source_hashes"]:
            raise ValueError("Validation source hashes differ from the current simulator")
    root = results_root or ROOT/"results"
    suite = root/_safe_label(label)
    suite.mkdir(parents=True, exist_ok=False)
    write_json(suite/"config.json", c)
    write_json(suite/"validation.json", dict(diagnostic=diagnostic,
                                           evidence=str(validation) if validation else None))
    runs = []
    for n in c["populations"]:
        for profile in c["profiles"]:
            for seed in c["seeds"]:
                child = run(dict(c, population=n, profile=profile), seed,
                            f"n{n}_{profile}_s{seed}", suite)
                summary = json.loads((child/"summary.json").read_text())
                runs.append(dict(path=str(child), **summary))
                write_json(suite/"runs.json", runs)
    _write_csv(suite/"runs.csv", runs)
    from .analysis.progress import bootstrap_summary
    contrasts = {}
    for n in c["populations"]:
        for profile in c["profiles"]:
            selected = [r for r in runs if r["population"] == n and r["profile"] == profile]
            # Censored completion times are not treated as observed or omitted silently.
            values = [r["target_time"] for r in selected if r["target_time"] is not None]
            contrasts[f"{n}_{profile}"] = dict(completed=len(values), censored=len(selected)-len(values),
                                              observed_target_times=bootstrap_summary(
                                                  [{"target_time": value} for value in values], metrics=["target_time"]) if values else None,
                                              fixed_window=bootstrap_summary(selected, metrics=[
                                                  "accepted_work_at_observation", "group_count", "zero_participation_nodes"]))
    write_json(suite/"statistics.json", contrasts)
    write_json(suite/"status.json", dict(status="completed", runs=len(runs),
                                        censored=sum(r["status"] == "censored" for r in runs)))
    return suite
