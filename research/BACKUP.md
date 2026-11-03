# Backing up the local archive

GitHub holds the scripts, reports and provenance records. It does not hold the project checkouts, their dependencies, the papers or the public data. Those are git-ignored and exist only on this machine, so they need a separate backup. This file explains how to make one (a zip on OneDrive) and how to check a restored copy.

## What lives where

| Location | Contents | In GitHub? |
|---|---|---|
| Tracked files | Scripts, reports, `SOURCE.json` / `DEPS_SOURCE.json` / `GENRL_SOURCE.json` / `PAPERS.json`, the Gensyn contract source, `ARCHIVE_INVENTORY.*`, `archive_manifests/` | Yes |
| `research/<project>/upstream/` | Project source, full git history | No |
| `research/<project>/data/` | Dashboard captures and public run data. SN38's is ~13 GB | No |
| `research/<project>/deps/`, `research/gensyn/genrl/` | Pinned dependencies: Hivemind forks, genrl, Bittensor SDK and CLI, subtensor | No |
| `research/<project>/papers/` | Cited papers | No |

[ARCHIVE_INVENTORY.md](ARCHIVE_INVENTORY.md) lists every snapshot for Node0, Agora, Gensyn RL Swarm and Distributed Training (SN38), with its hash. Other projects under `research/` are included in the zip, but they are not in the inventory and are not hash-checked.

## Making a backup

1. If you added or changed a snapshot, refresh the inventory and commit it first:

   ```bash
   python3 -I research/archive_inventory.py
   git add research/ARCHIVE_INVENTORY.* research/archive_manifests && git commit -m "Refresh archive inventory"
   git push
   ```

2. Check that the disk matches the inventory. The result should be `0 problem(s)`.

   ```bash
   python3 -I research/archive_inventory.py --verify
   ```

3. Zip the whole repository from its parent folder. Include `.git` and the ignored folders. Leave out the rebuildable Python environments: about 1.4 GB, recreated from their requirement files (see [public-networks/README.md](public-networks/README.md) and [agora/README.md](agora/README.md)).

   ```bash
   cd ~/Projects
   NAME="hivemind-analysis_$(date +%Y%m%d).zip"
   bsdtar -a -cf "$NAME" \
     --exclude 'hivemind-analysis/research/public-networks/.capture-env' \
     --exclude 'hivemind-analysis/research/public-networks/.plot-env' \
     --exclude 'hivemind-analysis/research/agora/.runtime' \
     --exclude 'hivemind-analysis/research/simulator/.venv' \
     --exclude '__pycache__' \
     hivemind-analysis
   sha256sum "$NAME" > "$NAME.sha256"
   ```

   - **Why `bsdtar`:** `zip` is not installed on this machine. `bsdtar` writes a standard `.zip` (`-a` picks the format from the extension) that OneDrive, Windows and macOS can open. It handles files over 4 GB; a 4.4 GB SN38 file was round-tripped with an identical hash on 2026-11-03.
   - **Where to write it:** keep the zip outside the repository and outside `/tmp`. `/tmp` is memory-backed here and too small.
   - **Size:** expect a zip of roughly 25–30 GB. Most of it is SN38 data, which is already compressed.

4. Upload the zip and its `.sha256` file to OneDrive. Keep at least the previous backup until the new one has been checked.

## Restoring

1. Download the zip and its `.sha256` file, check the zip, and unpack it:

   ```bash
   sha256sum -c hivemind-analysis_YYYYMMDD.zip.sha256
   bsdtar -xf hivemind-analysis_YYYYMMDD.zip -C ~/Projects
   ```

2. Verify the snapshots. Every line should read `OK`, ending with `0 problem(s)`. This also runs `git fsck` on every clone.

   ```bash
   cd ~/Projects/hivemind-analysis
   python3 -I research/archive_inventory.py --verify
   ```

   - `CHANGED` means the files differ from the recorded hashes. Compare against the snapshot's manifest in `research/archive_manifests/` to find which files.
   - `MISSING` means the snapshot isn't on disk.
   - `NEW` means a snapshot exists on disk but isn't in the inventory.

3. If the zip itself is lost, the git history is still on GitHub. The checkouts and dependencies can be re-cloned or re-downloaded at the commits and hashes recorded in the provenance files. Dashboards and public data **cannot** be recaptured once the sites are gone.

## What the backup cannot contain

Some things were never captured, so no backup holds them:
- the Pluralis auth server and trainers
- the Gensyn proposer and judge services
- model weights
- the SN38 Notion blog post: see `not_archived` in [distributed-training/DEPS_SOURCE.json](distributed-training/DEPS_SOURCE.json)
