"""Gate learning simulations on immutable native marker and SGD references."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import shutil

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research/simulator/src"))
from trainsim.policies.announcements import AnnouncementPolicy
from trainsim.protocols.hivemind.numerical import aggregate, announced_views
from announcement_artifacts import verify, inventory
from announcement_learning import NODES, load_problem
from announcement_reference import verify_native_round

COMPONENTS = (
    "research/simulator/src/trainsim/protocols/hivemind/numerical.py",
    "research/simulator/src/trainsim/policies/announcements.py",
    "research/simulator/src/trainsim/protocols/partitioning.py",
    "research/hivemind/announcement_learning.py",
)


def source_hashes():
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in COMPONENTS}


def numerical_case(outputs, condition, chunk_elements):
    import numpy as np
    inputs = {node: np.asarray(record["initial"], dtype=np.float32).reshape(-1)
              for node, record in outputs.items()}
    members = ("B", "D", "C") if condition == "control3" else NODES
    kind = "honest" if condition in ("baseline4", "control3") else condition
    views = announced_views(members, "B", AnnouncementPolicy(kind))
    return aggregate(inputs, views, chunk_elements=chunk_elements)


def validate(marker_suites, training_runs, destination):
    import numpy as np
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    checks, errors, references = [], [], []
    def check(name, value):
        checks.append({"name": name, "passed": bool(value)})
        if not value:
            errors.append(name)
    for root in [*marker_suites, *training_runs]:
        check(f"{root.name}: immutable inventory", not verify(root))
        references.append({"path": str(root.relative_to(ROOT)),
                           "artifact_inventory_sha256": hashlib.sha256((root / "artifact_inventory.json").read_bytes()).hexdigest()})
    marker_seeds = set()
    for suite in marker_suites:
        from analyze_announcement_probe import analyze_suite
        with tempfile.TemporaryDirectory(prefix="announcement-validation-") as temporary:
            copied = Path(temporary) / suite.name
            shutil.copytree(suite, copied)
            evidence = analyze_suite(copied)
            check(f"{suite.name}: complete native marker evidence", evidence["all_evidence_valid"])
        for condition in ("baseline4", "control3", "ordering", "membership"):
            case = suite / condition
            config = json.loads((case / "config.json").read_text())
            if config.get("bandwidth_profile", "equal") != "equal":
                raise ValueError("Unequal allocation is a retained native boundary, outside this backend")
            marker_seeds.add(config["seed"])
            outputs = json.loads((case / "outputs.json").read_text())
            result = numerical_case(outputs, condition, 36)
            for node, record in outputs.items():
                check(f"{suite.name}/{condition}/{node}: marker output",
                      np.allclose(result.values[node], np.asarray(record["final"]).reshape(-1), atol=1e-4, rtol=0))
                check(f"{suite.name}/{condition}/{node}: fallback size",
                      sum(b-a for a,b in result.fallback_ranges[node]) == record.get("failed_size", 0))
    check("all three fresh marker seeds included", marker_seeds.issuperset({2, 3, 4}))
    covered = set()
    for run in training_runs:
        config = json.loads((run / "config.json").read_text())
        condition = config["condition"]
        distribution = config["distribution"]
        covered.add((condition, distribution))
        problem = load_problem(run)
        check(f"{run.name}: pilot seed", problem.seed == 201)
        rounds = sorted(run.glob("round_[0-9][0-9][0-9]"))
        check(f"{run.name}: ten native rounds", len(rounds) == 10)
        models = {node: problem.initial_weights for node in NODES}
        for case in rounds:
            proof = verify_native_round(case)
            check(f"{run.name}/{case.name}: native proof", proof["valid"])
            if not proof["valid"]:
                errors.extend(proof["errors"])
            outputs = json.loads((case / "outputs.json").read_text())
            training = json.loads((case / "training.json").read_text())
            result = numerical_case(outputs, condition, 4)
            inputs = {node: problem.gradient(node, models[node]) for node in NODES}
            views = announced_views(NODES, "B", AnnouncementPolicy("honest" if condition == "baseline4" else condition))
            trajectory = aggregate(inputs, views, arrival_order=list(reversed(NODES)))
            for node, record in outputs.items():
                check(f"{run.name}/{case.name}/{node}: model before",
                      np.allclose(models[node], training["weights_before"][node], atol=1e-5, rtol=1e-4))
                check(f"{run.name}/{case.name}/{node}: returned gradient",
                      np.allclose(result.values[node], record["final"], atol=1e-5, rtol=1e-4))
                check(f"{run.name}/{case.name}/{node}: recurrent input gradient",
                      np.allclose(inputs[node], record["initial"], atol=1e-5, rtol=1e-4))
                check(f"{run.name}/{case.name}/{node}: recurrent output gradient",
                      np.allclose(trajectory.values[node], record["final"], atol=1e-5, rtol=1e-4))
                check(f"{run.name}/{case.name}/{node}: fallback size",
                      sum(b-a for a,b in result.fallback_ranges[node]) == record.get("failed_size", 0))
                models[node] = problem.update(models[node], trajectory.values[node])
                check(f"{run.name}/{case.name}/{node}: model after",
                      np.allclose(models[node], training["weights_after"][node], atol=1e-5, rtol=1e-4))
    check("six native training conditions included",
          covered == {(c,d) for c in ("baseline4","membership","ordering") for d in ("iid","distinctive")})
    report = {"schema": "announcement-numerical-validation-v1", "valid": not errors,
              "source_hashes": source_hashes(), "native_references": references,
              "checks": checks, "errors": errors,
              "scope": "Fixed four-node equal-bandwidth gradient averaging; no timing or leadership validation."}
    (destination / "validation.json").write_text(json.dumps(report, indent=2) + "\n")
    (destination / "REPORT.md").write_text(
        "# Numerical announcement validation\n\n"
        f"Passed: **{report['valid']}**. {sum(c['passed'] for c in checks)}/{len(checks)} checks.\n\n"
        + report["scope"] + "\n\n" + "\n".join(f"- {error}" for error in errors) + "\n")
    (destination / "artifact_inventory.json").write_text(json.dumps(inventory(destination), indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--marker-suite", action="append", type=Path, required=True)
    parser.add_argument("--training-run", action="append", type=Path, required=True)
    args = parser.parse_args()
    if Path(args.label).name != args.label:
        parser.error("Label must be a single new directory name")
    result = validate([p.resolve() for p in args.marker_suite],
                      [p.resolve() for p in args.training_run],
                      ROOT / "research/hivemind/results" / args.label)
    print(json.dumps({"valid": result["valid"], "checks": len(result["checks"]), "errors": result["errors"][:10]}))
    raise SystemExit(0 if result["valid"] else 2)
