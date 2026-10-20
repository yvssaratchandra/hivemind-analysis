"""Read-only comparison of native and simulated grouping evidence."""
import gzip
import json
import math
from pathlib import Path
import statistics
import shutil
import hashlib
import time
import os


def events(path: Path):
    """Read newline-delimited trace records without importing native packages."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as file:
        for line in file:
            yield json.loads(line)


def prepare_native(repository: Path, installed_package: Path, destination: Path) -> Path:
    """Stage repository Python with existing generated protobuf/daemon assets.

    Installed native dependencies remain read-only. Never overwrite a staging
    directory: its hashes must correspond to one identifiable source snapshot.
    """
    if destination.exists():
        raise FileExistsError(destination)
    package = destination/"hivemind"
    shutil.copytree(installed_package, package, ignore=shutil.ignore_patterns("__pycache__"))
    for source in (repository/"hivemind").rglob("*.py"):
        target = package/source.relative_to(repository/"hivemind")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    return destination


def audit_native_exit(native: Path) -> dict:
    """Independently check native-owned PIDs after its parent process exits.

    Shared-memory helpers can outlive shutdown calls until interpreter exit.
    Conservatively fail on any still-live listed PID, including possible reuse.
    This helper observes processes; it never signals them.
    """
    manifest_path = native/"manifest.json"
    content = manifest_path.read_bytes()
    manifest = json.loads(content)
    namespace = os.readlink("/proc/self/ns/pid")
    matching_namespace = manifest.get("pid_namespace") == namespace
    pids = set(map(int, manifest.get("owned_children", {}))) | set(manifest.get("survivors") or [])
    def alive():
        remaining = []
        for pid in sorted(pids):
            try:
                raw = Path(f"/proc/{pid}/stat").read_text()
                state = raw.rsplit(")", 1)[1].strip().split()[0]
                if state != "Z":
                    remaining.append(pid)
            except FileNotFoundError:
                pass
        return remaining
    deadline = time.monotonic()+5
    remaining = alive() if matching_namespace else []
    while remaining and time.monotonic() < deadline:
        time.sleep(.05)
        remaining = alive()
    report = dict(passed=matching_namespace and not remaining, survivors=remaining,
                  checked_pids=sorted(pids) if matching_namespace else [],
                  unverified_pids=[] if matching_namespace else sorted(pids),
                  pid_namespace=namespace, namespace_matched=matching_namespace,
                  manifest_sha256=hashlib.sha256(content).hexdigest(),
                  scope="Independent post-parent-exit PID check; PID reuse is conservatively treated as alive")
    target = native/"shutdown_audit.json"
    if target.exists():
        raise FileExistsError(target)
    target.write_text(json.dumps(report, indent=2)+"\n")
    return report


def grouping_metrics(path: Path, cutoff: float) -> dict:
    work_ready, groups, completed, registered = {}, {}, set(), {}
    for e in events(path):
        data = e.get("data", {})
        if e["kind"] == "work_ready":
            w = data["work"]
            work_ready[w["work_id"]] = w["compute_finish"]
        if e["kind"] == "group_formed":
            roster = data["roster"]
            if roster["formation_time"] <= cutoff:
                groups.setdefault(roster["group_id"], roster)
                registered.setdefault((roster["group_id"], e["actor"]), e["time"])
        if e["kind"] == "node_phase" and data.get("phase") == "allreduce" and e["time"] <= cutoff:
            registered.setdefault((data["group_id"], e["actor"]), e["time"])
        if e["time"] <= cutoff and e["kind"] == "group_completed":
            completed.add(data["group_id"])
        if e["time"] <= cutoff and e["kind"] == "aggregate_result":
            result = data["result"]
            if result["status"] == "complete":
                completed.add(result["group_id"])
    waits = []
    for group in groups.values():
        for member, work in zip(group["members"], group["work_ids"]):
            key = (group["group_id"], member)
            if work in work_ready and key in registered:
                waits.append(max(0., registered[key]-work_ready[work]))
    waits.sort()
    sizes = sorted(len(g["members"]) for g in groups.values())
    return dict(group_count=len(groups), completed_group_count=len(completed), group_rate=len(completed)/cutoff,
                median=statistics.median(waits) if waits else None,
                p90=waits[max(0, math.ceil(.9*len(waits))-1)] if waits else None,
                sizes=sizes, waits=waits)


def compare(simulation: Path, native: Path) -> dict:
    """Apply predefined grouping gates; report failures rather than tune them."""
    sc = json.loads((simulation/"resolved_config.json").read_text())
    nm = json.loads((native/"manifest.json").read_text())
    cutoff = min(sc["horizon_seconds"], nm["duration_seconds"])
    sim = grouping_metrics(simulation/"events.jsonl.gz", cutoff)
    nat = grouping_metrics(native/"events.jsonl.gz", cutoff)
    checks = {}
    checks["enough_observations"] = sim["group_count"] >= 5 and nat["group_count"] >= 5
    for key in ("median", "p90"):
        checks[key] = sim[key] is not None and nat[key] is not None and abs(sim[key]-nat[key]) <= max(.05, .2*nat[key])
    checks["group_rate"] = nat["group_rate"] > 0 and abs(sim["group_rate"]-nat["group_rate"]) <= .2*nat["group_rate"]
    sizes = sorted(set(sim["sizes"]+nat["sizes"]))
    cdf = max((abs(sum(x <= k for x in sim["sizes"])/max(1, len(sim["sizes"]))-
                   sum(x <= k for x in nat["sizes"])/max(1, len(nat["sizes"]))) for k in sizes), default=1.)
    checks["group_size_cdf"] = cdf <= .15
    audit_path = native/"shutdown_audit.json"
    audit = json.loads(audit_path.read_text()) if audit_path.exists() else {}
    independent_clean = (audit.get("passed") and audit.get("namespace_matched")
                         and audit.get("pid_namespace") == nm.get("pid_namespace")
                         and audit.get("manifest_sha256") == hashlib.sha256((native/"manifest.json").read_bytes()).hexdigest())
    checks["native_clean_exit"] = (nm.get("status") == "completed" and independent_clean
                                   and not nm.get("forced_cleanup") and not nm.get("cleanup_errors"))
    checks["owned_processes_recorded"] = bool(nm.get("owned_children"))
    checks["configuration_matched"] = all(sc[key] == nm["config"][key] for key in (
        "population", "target_group_size", "min_group_size", "min_matchmaking_time", "request_timeout",
        "search_timeout", "allreduce_timeout", "sender_timeout", "reducer_timeout", "payload_bytes",
        "chunk_bytes", "compute_seconds", "initial_offset_seconds", "initial_bits", "latency_seconds"))
    checks["honest_validation"] = not sc.get("policies") and not nm["config"].get("policies")
    checks["profile_matched"] = sc["profile"] == nm["config"]["profile"] == "controlled"
    checks["registry_latency_matched"] = sc["registry_latency_seconds"] == nm["config"].get("registry_latency_seconds", 0.)
    checks["seed_matched"] = sc["seed"] == nm["seed"]
    checks["horizon_matched"] = sc["horizon_seconds"] == nm["duration_seconds"]
    repository = Path(__file__).resolve().parents[4]
    checks["native_source_matched"] = all(
        hashlib.sha256((repository/"hivemind"/name).read_bytes()).hexdigest() == sha
        for name, sha in nm["source_hashes"].items() if name.endswith(".py"))
    sm = json.loads((simulation/"manifest.json").read_text())
    checks["simulation_source_matched"] = all(
        (repository/name).is_file() and hashlib.sha256((repository/name).read_bytes()).hexdigest() == sha
        for name, sha in sm["source_hashes"].items())
    return dict(passed=all(checks.values()), checks=checks, group_size_cdf_difference=cdf,
                simulation=sim, native=nat,
                scope="Grouping only; not full DHT or bulk-bandwidth validation.")


def validate_suite(pairs_path: Path) -> dict:
    """Validate ten held-out pairs; missing evidence is a failed gate."""
    from .runner import source_manifest
    pairs = json.loads(pairs_path.read_text())
    results, conditions = [], set()
    for pair in pairs:
        simulation, native = Path(pair["simulation"]), Path(pair["native"])
        config = json.loads((simulation/"resolved_config.json").read_text())
        result = compare(simulation, native)
        condition = (config["latency_seconds"], config["seed"])
        if condition in conditions:
            raise ValueError(f"Duplicate held-out pair {condition}")
        conditions.add(condition)
        result.update(simulation_path=str(simulation), native_path=str(native))
        results.append(result)
    expected = {(delay, seed) for delay in (0., .025) for seed in range(101, 106)}
    calibration_path = pairs_path.parent/"calibration_pairs.json"
    calibration_pairs = json.loads(calibration_path.read_text()) if calibration_path.exists() else []
    calibration_conditions, calibration_results = set(), []
    for pair in calibration_pairs:
        sc = json.loads((Path(pair["simulation"])/"resolved_config.json").read_text())
        condition = (sc["latency_seconds"], sc["seed"])
        if condition in calibration_conditions:
            raise ValueError(f"Duplicate calibration pair {condition}")
        calibration_conditions.add(condition)
        calibration_results.append(compare(Path(pair["simulation"]), Path(pair["native"])))
    expected_calibration = {(delay, seed) for delay in (0., .025) for seed in range(1, 6)}
    fixture_path = pairs_path.parent/"fixtures/fixtures.json"
    fixtures = json.loads(fixture_path.read_text()) if fixture_path.exists() else {}
    repository = Path(__file__).resolve().parents[4]
    checks = dict(complete_held_out_matrix=conditions == expected,
                  complete_calibration_matrix=calibration_conditions == expected_calibration,
                  calibration_gates=bool(calibration_results) and all(r["passed"] for r in calibration_results),
                  individual_gates=bool(results) and all(r["passed"] for r in results),
                  deterministic_fixtures=fixtures.get("passed", False),
                  fixture_source_matched=fixtures.get("simulator_source_hashes") == source_manifest()["source_hashes"],
                  fixture_native_source_matched=bool(fixtures.get("native_source_hashes")) and all(
                      hashlib.sha256((repository/"hivemind/averaging"/name).read_bytes()).hexdigest() == digest
                      for name, digest in fixtures.get("native_source_hashes", {}).items()),
                  reference_workload=all(
                      all(json.loads((Path(p["simulation"])/"resolved_config.json").read_text())[key] == value
                          for key, value in dict(population=8, target_group_size=4, min_group_size=2,
                                                 payload_bytes=512, chunk_bytes=128, compute_seconds=1.5,
                                                 initial_bits=0).items()) for p in pairs),
                  full_duration=all(json.loads((Path(p["simulation"])/"resolved_config.json").read_text())["horizon_seconds"] == 120
                                    for p in pairs+calibration_pairs))
    return dict(kind="held_out_native_suite", passed=all(checks.values()), checks=checks,
                source_hashes=source_manifest()["source_hashes"], pairs=results, calibration=calibration_results,
                scope="Held-out grouping validation; bulk-network model remains a stated abstraction")
