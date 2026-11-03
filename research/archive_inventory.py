"""Inventory and verify git-ignored local snapshots (code, data, papers, dependencies).

Writes, for each project:
  - one row per snapshot in ARCHIVE_INVENTORY.json / ARCHIVE_INVENTORY.md
  - a per-file SHA-256 manifest (archive_manifests/<project>__<snapshot>.tsv) for plain snapshots

Git clones are recorded by HEAD, commit count and shallowness; ``--verify`` also runs ``git fsck``.
Plain snapshots are recorded by file count, bytes and a tree hash (SHA-256 of their manifest).

Usage:
  python3 -I research/archive_inventory.py            # (re)build the inventory
  python3 -I research/archive_inventory.py --verify   # compare the disk against the inventory
"""

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RESEARCH = Path(__file__).resolve().parent
PROJECTS = ("node0", "agora", "gensyn", "distributed-training")
# Snapshot locations inside a project: a fixed directory, or every child of a directory.
SINGLE = ("upstream", "genrl", "papers", "contract")
CHILDREN = ("data", "deps")
SKIP_NAMES = {"__pycache__", ".git"}


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout.strip()


def discover(project_dir):
    for name in SINGLE:
        path = project_dir / name
        if path.exists():
            yield path
    for name in CHILDREN:
        parent = project_dir / name
        if parent.is_dir():
            yield from sorted(child for child in parent.iterdir() if child.name not in SKIP_NAMES)


def walk_files(root):
    if root.is_file():
        yield root
        return
    for path in sorted(root.rglob("*")):
        if path.is_file() and not any(part in SKIP_NAMES for part in path.relative_to(root).parts):
            yield path


def manifest_name(project, snapshot_rel):
    return f"{project}__{snapshot_rel.replace('/', '__')}.tsv"


def describe(project, path, manifests_dir):
    rel = path.relative_to(RESEARCH / project).as_posix()
    row = {"project": project, "snapshot": rel}
    if (path / ".git").exists():
        row.update(
            kind="git",
            remote=git(path, "remote", "get-url", "origin"),
            head=git(path, "rev-parse", "HEAD"),
            head_date=git(path, "log", "-1", "--format=%cI"),
            commits=int(git(path, "rev-list", "--count", "HEAD")),
            shallow=git(path, "rev-parse", "--is-shallow-repository") == "true",
            dirty=bool(git(path, "status", "--porcelain", "--untracked-files=no")),
        )
        return row
    lines, total = [], 0
    for file in walk_files(path):
        size = file.stat().st_size
        total += size
        name = file.name if path.is_file() else file.relative_to(path).as_posix()
        lines.append(f"{sha256_file(file)}\t{size}\t{name}")
    text = "".join(line + "\n" for line in lines)
    if manifests_dir is not None:
        (manifests_dir / manifest_name(project, rel)).write_text(text)
    row.update(kind="files", files=len(lines), bytes=total, tree_sha256=hashlib.sha256(text.encode()).hexdigest())
    return row


def build(projects, manifests_dir):
    rows = []
    for project in projects:
        for path in discover(RESEARCH / project):
            rows.append(describe(project, path, manifests_dir))
            print(f"  {project}/{rows[-1]['snapshot']}", file=sys.stderr)
    return rows


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def write_markdown(rows, path, generated):
    out = [
        "# Local archive inventory",
        "",
        f"Generated {generated} by `research/archive_inventory.py`. These snapshots are git-ignored and backed up",
        "outside GitHub. Verify a restored copy with `python3 -I research/archive_inventory.py --verify`.",
        "Provenance for each source is in the project's `SOURCE.json`, `DEPS_SOURCE.json`, `GENRL_SOURCE.json`",
        "or `PAPERS.json`; per-file hashes are in `research/archive_manifests/`.",
        "",
        "| Project | Snapshot | Kind | Contents | Identity |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        if r["kind"] == "git":
            contents = f"{r['commits']} commits" + (", shallow" if r["shallow"] else "") + (", DIRTY" if r["dirty"] else "")
            identity = f"HEAD `{r['head'][:12]}` ({r['head_date'][:10]})"
        else:
            contents = f"{r['files']} files, {human(r['bytes'])}"
            identity = f"tree `{r['tree_sha256'][:12]}`"
        out.append(f"| {r['project']} | `{r['snapshot']}` | {r['kind']} | {contents} | {identity} |")
    path.write_text("\n".join(out) + "\n")


def verify(inventory_path):
    recorded = json.loads(inventory_path.read_text())
    projects = sorted({r["project"] for r in recorded["snapshots"]})
    current = {(r["project"], r["snapshot"]): r for r in build(projects, None)}
    problems = 0
    for r in recorded["snapshots"]:
        key = (r["project"], r["snapshot"])
        now = current.pop(key, None)
        label = f"{r['project']}/{r['snapshot']}"
        if now is None:
            print(f"MISSING  {label}")
            problems += 1
            continue
        fields = ("head", "commits", "shallow") if r["kind"] == "git" else ("files", "bytes", "tree_sha256")
        changed = [f for f in fields if now.get(f) != r.get(f)]
        if r["kind"] == "git":
            fsck = subprocess.run(["git", "-C", str(RESEARCH / r["project"] / r["snapshot"]), "fsck", "--no-progress"],
                                  capture_output=True, text=True)
            if fsck.returncode != 0:
                changed.append("fsck")
        print(("CHANGED  " if changed else "OK       ") + label + (f"  ({', '.join(changed)})" if changed else ""))
        problems += bool(changed)
    for project, snapshot in current:
        print(f"NEW      {project}/{snapshot}")
    print(f"{len(recorded['snapshots'])} recorded snapshots, {problems} problem(s)")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--projects", nargs="+", default=list(PROJECTS))
    parser.add_argument("--verify", action="store_true", help="compare the disk against ARCHIVE_INVENTORY.json")
    args = parser.parse_args(argv)
    inventory_path = RESEARCH / "ARCHIVE_INVENTORY.json"
    if args.verify:
        return 1 if verify(inventory_path) else 0
    manifests_dir = RESEARCH / "archive_manifests"
    manifests_dir.mkdir(exist_ok=True)
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = build(args.projects, manifests_dir)
    inventory_path.write_text(json.dumps({"generated_utc": generated, "snapshots": rows}, indent=1) + "\n")
    write_markdown(rows, RESEARCH / "ARCHIVE_INVENTORY.md", generated)
    print(f"{len(rows)} snapshots inventoried", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
