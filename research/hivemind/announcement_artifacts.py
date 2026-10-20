"""Preserve and audit experiment files without placing bulky traces in Git."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def inventory(root: Path) -> dict:
    files = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "artifact_inventory.json" and "__pycache__" not in path.parts:
            files.append({"path": str(path.relative_to(root)), "bytes": path.stat().st_size,
                          "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    return {"description": "Frozen local evidence; paths relative to this experiment directory.",
            "files": files}


def verify(root: Path) -> list[str]:
    manifest = json.loads((root / "artifact_inventory.json").read_text())
    failures = []
    for item in manifest["files"]:
        path = root / item["path"]
        if (not path.is_file() or path.stat().st_size != item["bytes"]
                or hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]):
            failures.append(item["path"])
    return failures


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.verify:
        failed = verify(args.path)
        print(json.dumps({"valid": not failed, "failures": failed}))
        raise SystemExit(bool(failed))
    target = args.path / "artifact_inventory.json"
    if target.exists():
        raise SystemExit("Inventory already exists; use a distinct experiment directory.")
    result = inventory(args.path)
    target.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"files": len(result["files"]), "inventory": str(target)}))
