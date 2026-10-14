# Step30 final comparison: stock 2s versus pre-expiry renewal

**Completed:** six five-minute trials, three paired seeds, eight fresh local peers per trial. All raw, exported-data and frozen-source checks pass. All peers/controllers have exited.

**Measured conclusion:** renewal consistently increased first contacts with X and renewed-pair retries. It added a small recurring grouping delay (paired mean **0.293s**). No predeclared material grouping-harm threshold was met. This is a control-plane result with an intentionally disbanding X; reliable leadership success and training impact were not measured.

## Combined results

Each policy has 15 fixed observation minutes. Waits and choice percentages below are pooled by honest attempt; a failed attempt scores 12s. Successful-group evidence is each peer's own native local assembly, not membership claimed in a leader roster.

| Measure | Stock 2s | Renewal (1,2] |
|---|---:|---:|
| Honest submitted attempts | 1076 | 1022 |
| Capped mean grouping wait | 3.416896s | 3.709940s |
| Success-only mean grouping wait | 3.376826s | 3.693685s |
| Group failures | 5/1076 (0.465%) | 2/1022 (0.196%) |
| Groups within observation windows | 316 | 313 |
| Groups formed during drain | 4 | 1 |
| Total native leader-assembled groups | 320 | 314 |
| Groups per fixed minute | 21.066667 | 20.866667 |
| Mean observed group roster size | 3.354430 | 3.255591 |
| Honest first local assemblies per minute | 70.600000 | 67.800000 |
| Honest attempts first trying X | 431/1076 (40.06%) | 577/1022 (56.46%) |
| Honest attempts ever trying X | 721/1076 (67.01%) | 756/1022 (73.97%) |
| Client ACCEPTED receipts from X | 885 | 1140 |
| Same-search renewed-pair retry witnesses | 268 | 745 |

A retry witness requires a previously attempted exact `(PeerID, expiration)` in the native past set, then selection of the same peer with a strictly newer expiration in the same search. Retry and ACCEPTED counts are join events, not independent honest attempts or successful groups. Stock itself republishes at expiry; these counts show the additional effect of earlier renewal against that native control.

## Paired results

| Seed | Stock wait | Renewal wait | Added wait | Stock / renewal failures | Stock / renewal observed groups | First X choice, stock / renewal | Retries, stock / renewal |
|---|---:|---:|---:|---:|---:|---:|---:|
| 3001 | 3.324828s | 3.598925s | +0.274097s | 1/362 / 1/343 | 111 / 104 | 42.82% / 60.35% | 90 / 263 |
| 3002 | 3.542730s | 3.832674s | +0.289943s | 2/354 / 1/338 | 105 / 104 | 37.57% / 54.14% | 92 / 263 |
| 3003 | 3.385739s | 3.699954s | +0.314214s | 2/360 / 0/341 | 100 / 105 | 39.72% / 54.84% | 86 / 219 |

## Decision and scope

- Added wait was positive in all three seeds: +0.274s, +0.290s, +0.314s. Its equally weighted paired mean was 0.292751s; zero seeds reached the predeclared +0.5s threshold.
- No paired seed reached +5 percentage points of failures or a 15% group-rate reduction. Group rate changed in both directions; renewal had fewer failures overall.
- The small wait increase persisted in 4/4, 3/4, 4/4 later minute windows for seeds 3001–3003. This establishes a recurring measured delay under this setup, while its magnitude remains below the predeclared material cutoff.
- Every trial met the exposure gates: >=100 honest starts, >=20 client ACCEPTED-from-X, and >=10 X searches with multiple accepted publications. Native publication intervals were about 2s, renewal about 1s; all paired ratios were <=0.75.
- Three seeds support a descriptive local result, not a deployment success rate or significance claim. Uniform independently seeded 100–200ms per remote control-RPC leg is WAN-delay emulation on loopback, without loss, churn, clock skew, tensor traffic, or model training.
- The seed 3002 pair spans the requested overnight pause. Its renewal trace was closed before suspension; all peers were shut down, and its stock counterpart used fresh peers and identical frozen configuration after resume. OS scheduling remains nondeterministic across trials.
- X used recruit-only admission in both arms, forwarded ACCEPTED, held 0.5s, then disbanded. It assembled zero groups by design. First-choice-X percentages are not leadership-success percentages.

The useful finding is a verified attraction/retry mechanism with a modest persistent delay. It does not justify a claim of substantial grouping denial or reliable leadership advantage. This final bounded study is complete; no further trials were started.

## Evidence and validation

- Per-trial folders retain original configs, seeds, timestamps, raw events, native summaries, derived analysis, independent certificates and completed-file hashes.
- [Generated report](REPORT.md), [trial metrics](trial_summary.csv), [paired contrasts](paired_contrasts.csv), [per-attempt choices](choices.csv), [exact retries](retries.csv), [join deadlines and receipts](joins.csv), [group IDs, leaders and rosters](groups.csv), [late windows](windows.csv), [source provenance](provenance.json), and [independent export verification](raw_verification_exports.json) remain inspectable.
- The active experiment uses 31 frozen harness files and 111 native source snapshots. Core source was unchanged. All 34 focused/replay checks passed before the final smoke; both fresh smoke gates and all six raw/CSV audits passed.
- [Offline final checkpoint validation](../step30_final_resume_validation_20261013_a.json): 6 completed, 0 pending, 0 interrupted. [Final shutdown audit](../step30_final_shutdown_20261013_b.json): zero processes, safe to suspend. [Preservation audit](../step30_preservation_verification_20261013_a.json): 2,831 baseline files and both earlier smoke revisions unchanged.
- Earlier rejected smoke evidence is preserved. Separately versioned checking fixes covered native event schema, cancelled zero-response joins, and repeated expirations at the fixed search deadline; the measurement workload stayed frozen.
