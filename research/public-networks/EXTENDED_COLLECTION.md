# Extended public-run archive

Batch: `extended_20261029`. Approved additional storage: 20 GB, excluding source clones. Reserve 3 GB per project and 2 GB for shared retries. Earlier collections are preserved.

## Implementation status

| Responsibility | Owner | Status |
| --- | --- | --- |
| Psyche and IOTA sources, snapshots, histories | psyche_iota_sources | Collected and reviewed |
| Templar and Grail sources, snapshots, histories | covenant_sources | Collected and reviewed |
| INTELLECT-1 and INTELLECT-2 sources and run evidence | intellect_sources | Collected and reviewed |
| Registry, independent verification, gallery and review | Main agent | Verified |

## Approach and acceptance

Each project owns its collector, source revision inventory, report and ignored bulk archive. Public HTTP responses flow into immutable hash manifests; bounded normalization retains source attribution. Browser captures preserve rendered appearances separately from API exports. No training, public swarm participation, authenticated access, model weights or dataset contents are requested.

Collectors expose `--output`, `--max-bytes`, `--resume`, and `--analyze-only`. The shared runner exposes explicit project names and `extended`; legacy `all` continues to mean the original four projects. Source clones retain full history where available and omit LFS payloads explicitly.

Implementation order: source identification and cloning, public snapshots, bounded histories, project tests and report, independent integration checks. Acceptance requires hashes and sizes to verify, total additional storage within budget, source revisions recorded, and unavailable histories clearly reported. Review source/run attribution and representative raw data and screenshots before accepting the work. An intact archive does not imply complete historical coverage.

## Reuse

List the six projects without network access or writes:

```sh
python research/public-networks/collect_public_runs.py --project extended --discover-only
```

Collect another dated HTTP snapshot (browser captures are separately documented by each project):

```sh
python research/public-networks/collect_public_runs.py --project extended --label extended_NEW_DATE
```

Verify this batch and regenerate its local screenshot gallery:

```sh
python research/public-networks/verify_extended.py --label extended_20261029
```

Source checkouts and captured bodies are local, ignored bulk files. Pushing the parent repository preserves inventories and tooling, not the bulk archive. An independent backup must include all six `upstream/`, applicable `supporting/`, and `data/extended_20261029/` directories as well as the tracked reports and inventories.

## Final verification

- Additional archive: 305,659,445 bytes (about 306 MB), excluding source clones.
- 1,847 unique local files inventoried; 1,797 manifest payloads verified across 25 manifests.
- All 15 fetched repository/submodule trees passed Git integrity checks. Explicit submodule/LFS omissions remain in project source inventories.
- All six collectors completed offline analysis through the shared runner.
- 68 focused tests passed. All 107 local gallery references resolve.
- Independent raw-to-normalized checks passed for 113,008 IOTA overview-loss points, 1,415 IOTA leaderboard rows and 600 Templar BPB points.

See `EXTENDED_PUBLIC_RUNS_REPORT.md` for coverage, `EXTENDED_VERIFICATION.json` for file integrity, and `EXTENDED_TEST_RESULTS.json` for test evidence.
