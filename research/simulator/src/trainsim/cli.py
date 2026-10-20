"""Command-line entry points; commands do not contact public swarms."""
import argparse
import json
from pathlib import Path

from .config import load, resolve


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "run", "sweep"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--config", type=Path, required=True)
        if name != "validate":
            cmd.add_argument("--label", required=True)
            cmd.add_argument("--results-root", type=Path)
        if name == "sweep":
            cmd.add_argument("--validation", type=Path)
            cmd.add_argument("--diagnostic", action="store_true")
        if name == "run":
            cmd.add_argument("--seed", type=int, required=True)
            for field, converter in (("population", int), ("profile", str), ("horizon_seconds", float),
                                     ("observation_seconds", float), ("target_per_node", float),
                                     ("payload_bytes", int), ("chunk_bytes", int)):
                cmd.add_argument("--"+field.replace("_", "-"), type=converter)
    cmd = commands.add_parser("analyze")
    cmd.add_argument("--run", type=Path, required=True)
    cmd = commands.add_parser("compare-native")
    cmd.add_argument("--simulation", type=Path, required=True)
    cmd.add_argument("--native", type=Path, required=True)
    cmd.add_argument("--output", type=Path)
    cmd = commands.add_parser("validate-native-suite")
    cmd.add_argument("--pairs", type=Path, required=True,
                     help="JSON list of {simulation: directory, native: directory} held-out pairs")
    cmd.add_argument("--output", type=Path, required=True)
    cmd = commands.add_parser("native-suite")
    cmd.add_argument("--label", required=True)
    cmd.add_argument("--native-package-root", type=Path, required=True)
    cmd.add_argument("--native-dependencies", type=Path, required=True)
    cmd.add_argument("--results-root", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command in ("validate", "run", "sweep"):
            c = load(args.config)
            if args.command == "validate":
                print(json.dumps(c, indent=2, sort_keys=True))
                return 0
            from .runner import run, sweep
            if args.command == "sweep":
                result = sweep(c, args.label, args.results_root, args.validation, args.diagnostic)
            else:
                for field in ("population", "profile", "horizon_seconds", "observation_seconds",
                              "target_per_node", "payload_bytes", "chunk_bytes"):
                    value = getattr(args, field)
                    if value is not None:
                        c[field] = value
                result = run(resolve(c), args.seed, args.label, args.results_root)
            print(result)
            return 0
        if args.command == "analyze":
            from .runner import analyze
            data = analyze(args.run)
            print(json.dumps(data["summary"], indent=2))
            return 0
        if args.command == "native-suite":
            from .validation_runner import native_suite
            result = native_suite(args.label, args.native_package_root, args.native_dependencies, args.results_root)
            print(result)
            status = json.loads((result/"status.json").read_text())["status"]
            return 0 if status == "completed" else 2
        from .native_validation import compare, validate_suite
        if args.command == "validate-native-suite":
            result = validate_suite(args.pairs)
        else:
            result = compare(args.simulation, args.native)
        if args.output:
            from .runner import write_json
            if args.output.exists():
                raise FileExistsError(args.output)
            write_json(args.output, result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["passed"] else 2
    except (ValueError, FileExistsError, FileNotFoundError, RuntimeError) as exc:
        parser.exit(2, f"error: {exc}\n")
