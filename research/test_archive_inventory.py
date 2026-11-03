import json
import subprocess

import archive_inventory as inv


def make_tree(root):
    project = root / "proj"
    (project / "data" / "run1").mkdir(parents=True)
    (project / "data" / "run1" / "a.txt").write_text("alpha")
    (project / "data" / "run1" / "sub").mkdir()
    (project / "data" / "run1" / "sub" / "b.bin").write_bytes(b"\x00\x01")
    (project / "data" / "single.json").write_text("{}")
    repo = project / "upstream"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", "https://example.invalid/r.git"], check=True)
    (repo / "f.py").write_text("x = 1\n")
    subprocess.run(["git", "-C", str(repo), "add", "f.py"], check=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "init"], check=True)
    return project


def test_build_and_verify(tmp_path, monkeypatch, capsys):
    project = make_tree(tmp_path)
    monkeypatch.setattr(inv, "RESEARCH", tmp_path)
    assert inv.main(["--projects", "proj"]) == 0

    rows = {r["snapshot"]: r for r in json.loads((tmp_path / "ARCHIVE_INVENTORY.json").read_text())["snapshots"]}
    assert set(rows) == {"upstream", "data/run1", "data/single.json"}
    assert rows["upstream"]["kind"] == "git" and rows["upstream"]["commits"] == 1
    assert rows["data/run1"]["files"] == 2 and rows["data/run1"]["bytes"] == 7
    manifest = (tmp_path / "archive_manifests" / "proj__data__run1.tsv").read_text().splitlines()
    assert [line.split("\t")[2] for line in manifest] == ["a.txt", "sub/b.bin"]

    assert inv.main(["--verify"]) == 0
    assert "0 problem(s)" in capsys.readouterr().out

    (project / "data" / "run1" / "a.txt").write_text("tampered")
    (project / "data" / "single.json").unlink()
    assert inv.main(["--verify"]) == 1
    out = capsys.readouterr().out
    assert "CHANGED  proj/data/run1" in out
    assert "MISSING  proj/data/single.json" in out
