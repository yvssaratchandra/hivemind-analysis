# Step 26: preliminary lifetime × renewal × delay screen

All 14 jobs completed; zero pending jobs. Independent raw counting and CSV checks pass:
**319 distinct groups, 120 X-led groups, 129 submitted X attempts**. Eight peers per trial;
every X-led group has four members. Tensor results do not filter the counts.

The goal is X-led assemblies per submitted X attempt, whether renewal improves that result
beyond a short initial deadline and refusal to follow, and why honest peers choose or reject X.
These are one-seed loopback measurements with application-level delay emulation, not deployment rates.

| Arm | One-way delay (ms) | All groups | X-led groups | X led / X attempts | Led within 2s / attempts | Successful median (s) | X selected / eligible first choices |
|---|---:|---:|---:|---:|---:|---:|---:|
| Passive stock (native ad, X refuses outbound joins) | 0 | 33 | 12 | 12/12 | 11/12 | 0.895 | 38/57 |
| Single ad U=1s | 0 | 31 | 10 | 10/11 | 10/11 | 0.323 | 25/25 |
| Single ad U=3s | 0 | 33 | 15 | 15/15 | 15/15 | 0.258 | 38/38 |
| Refresh U=1s, f=0.5/s | 0 | 33 | 15 | 15/15 | 14/15 | 0.252 | 39/39 |
| Refresh U=1s, f=2/s | 0 | 33 | 15 | 15/15 | 15/15 | 0.250 | 36/36 |
| Refresh U=3s, f=0.5/s | 0 | 33 | 15 | 15/15 | 15/15 | 0.269 | 36/36 |
| Refresh U=3s, f=2/s | 0 | 33 | 14 | 14/14 | 14/14 | 0.388 | 38/38 |
| Passive stock (native ad, X refuses outbound joins) | 150 | 14 | 5 | 5/5 | 0/5 | 3.554 | 5/25 |
| Single ad U=1s | 150 | 11 | 0 | 0/3 | 0/3 | — | 1/1 |
| Single ad U=3s | 150 | 14 | 6 | 6/6 | 2/6 | 2.230 | 29/29 |
| Refresh U=1s, f=0.5/s | 150 | 12 | 0 | 0/3 | 0/3 | — | 5/5 |
| Refresh U=1s, f=2/s | 150 | 11 | 1 | 1/3 | 0/3 | 6.352 | 8/8 |
| Refresh U=3s, f=0.5/s | 150 | 14 | 6 | 6/6 | 2/6 | 2.233 | 30/33 |
| Refresh U=3s, f=2/s | 150 | 14 | 6 | 6/6 | 1/6 | 2.374 | 21/30 |

The attempt denominator includes failures and no-group outcomes. Multiple native searches inside
one submitted attempt are separate diagnostics. Choice counts use the first native decision per
honest search at which X is live, unattempted and no later in priority than that peer's own deadline.
This is a conditional selection measure, not the fraction of all groups captured. Other groups
forming while X searches do not automatically count as X losses. Total groups differ because the
same 30-second window contains different search waits, failures, group sizes and training delays.

The strongest observed pattern is lifetime interacting with delay. Without added delay, all four
renewal cells lead every submitted attempt; the 1-second single-ad control leads 10/11 (its first
attempt fails), and the 3-second single-ad control leads 15/15. At 150ms, the 1-second single ad and
0.5/s renewal each lead 0/3, while 2/s renewal leads 1/3 after 6.352s of searching. All 3-second arms
lead 6/6; their successful medians are 2.230s single-ad, 2.233s at 0.5/s, and 2.374s at 2/s.
Renewal does not show an advantage over the 3-second single-ad control here.

The frequency effect on eventual leadership is 0 percentage points locally; under delay it is
+33.3 points for U=1s and 0 for U=3s. The resulting three-factor difference in differences is
+33.3 points. This estimate has only three attempts in its smallest cell: the predefined screen
marks it **insufficient observations**, and it is not evidence of a reliable success probability.

There is also a priority cost. For U=3s under delay, X wins 29/29 eligible first choices with a single
ad, 30/33 at 0.5/s, and 21/30 at 2/s. For the nine non-X decisions in the fast-renewal arm, the chosen
peer's expiration is earlier than X's by a median 0.174s (range 0.004–1.101s). These are recorded native
queue decisions, not inferred losses from unrelated group formation. The 30-point matched reduction
and the 20.9-point frequency × delay choice contrast trigger exploratory screening flags. The local
U=3s fast-renewal median is also 50.5% slower than its single-ad control. Flags identify follow-up
questions; they do not give significance tests, and repeated choices by seven honest peers are not
independent population samples.

[Mechanism witnesses](mechanism_evidence.json) contain exact raw references and input hashes.
Across the seven delayed jobs, six honest searches contain **seven additional selections of X**
before the same native X SEARCH span closes. The newer expiration is absent from `past_attempts`, while
the older X expiration remains there. The witnesses occur in U=3s at 0.5/s (two extra selections),
U=3s at 2/s (two), and U=1s at 2/s (three). Four of the seven re-selections occur after X has already assembled its group but before native search
cleanup finishes; three occur before assembly. They are recorded retries, not further group wins. This demonstrates bounded renewal-enabled retries locally;
it does not demonstrate indefinite retry, sustained denial, or harm.

Three joins receive `BAD_EXPIRATION_TIME`: X's admission-time expiration exceeds the request's
expiration by 0.174s, 0.896s and 0.913s. The selected X deadline had been earlier than the request's.
In the two U=1s cases, X publishes the later expiration about 70ms after the honest selection and
before admission; the U=3s case selects a stale read-view deadline after that renewal was already
published. This directly records the freshness/priority/admission tradeoff.

For U=1s at 0.5/s under delay, all nine observed X join replies accept then disband; the single-ad
control has one such join. For U=1s at 2/s, eleven joins are accepted, eight get disband replies,
and three get the begin message for the sole X-led group. Seeing or selecting X therefore does
not imply that X gathers three followers in time.

The timing model is explicit. At publication start p_n:

    T = 1/f
    E_n = min(search_end, max(nextafter(E_(n-1), +infinity), p_n + U))
    achieved send interval ≈ max(T, serialized write latency) + scheduling overhead
    lifetime remaining at holder acceptance A_n = E_n - A_n

In uninterrupted steady state, continuous holder availability requires A_(n+1) < E_n.
U=1s at 0.5/s has nominal remaining lifetime (-1,1], so it includes availability gaps even
without network latency. The other nominal intervals are (0.5,1], (1,3], and (2.5,3].
Search cutoff can clamp the upper bound. These local send-time ranges are not guaranteed DHT views.
Remote find/store request handling is delayed by d and its response by d; each join request and
streamed leader response is delayed by d. The deadline remains an absolute timestamp and is not
extended in transit. A follower's request expiration is fixed for that join. Native admission rejects
when the leader's current expiration exceeds it; renewal can create that mismatch.

Requested 2 writes/sec achieves about **1.08/sec under delay**, versus 2.00/sec locally. Requested
0.5/sec achieves about 0.50/sec when repeated publications occur. Local U=3s at 0.5/sec usually
finishes after its first publication, so no achieved repeated-publication interval is measurable.
Honest logical ad-read interval medians across arms are **8.84–11.07ms locally** and
**622.52–623.69ms under delay**; read-start rates per enabled honest-peer second are 73.79–96.75
and 1.05–1.16 respectively. These are instrumented function calls, not packet rates or a fixed
victim polling schedule. Cached reads are not artificially delayed.

Configuration: one paired seed 2601; deterministic order seed 260026; 30-second submission windows
plus bounded drain; fresh eight-peer networks; X minimum/target four, honest minimum two/target four;
honest ad lifetime 5s; request timeout 3s; step timeout 12s; tensor size 128. All peers use the same
seeded training-delay distribution (pre-clipping mean 1.5s, CV 0.6, clipped to 0.1–4s) and seeded
initial offsets. Fresh PeerIDs and native scheduling remain nondeterministic. Delay is applied to
all peers' remote DHT find/store and join paths; routing/pings, cached reads and tensor transfer
remain native. This is application-level emulation, not a full WAN emulator.

Source and recovery: the eleven original experiment/dependency files and native/runtime provenance
remain frozen. Two audit-only corrections recovered closed trials without rerunning them: seven
start-only DHT RPCs in trial10 and six incomplete unary prefixes in trial11 were strictly after
control-plane work closed. Completion and response outcomes for those thirteen streams are unobserved;
all original prefix records remain in the raw traces. The first correction required final step-result
closure; the second correctly uses control-span/assembly/ad closure while still requiring paired
step results and passing native reconstruction. In-measurement incomplete RPCs and incomplete joins
remain failures. Original failed analyses, halt records, correction source snapshots/hashes and
pre-correction evidence are preserved. The original raw-verifier report is also retained: it rejected
the empty peer on the final non-participant closure marker. A separate verifier checks exactly one
valid final marker and corrects only that identity error. The corrected verifier passes all 14 jobs
and all **410 Step25 preservation hashes**. No core Hivemind source was edited. Focused checks:
108 original checks plus 9 first-audit, 4 second-audit and 3 closure-verifier checks pass.

All experiment peers and controllers have exited; [shutdown proof](../step26_shutdown_20261010_a.json)
records no remaining Step26 controller or runtime daemon. It is safe to suspend. No additional
experiments were started after this screen. A useful next small comparison would be the U=2s
single-ad and matched renewal control under delay with a few fresh seeds, plus repeat the U=1s
fast-renewal cell to check whether 1/3 is reproducible; that follow-up has not run.

Inspect [scalar table](leadership_summary.csv), [all group leaders/PeerIDs/rosters and winner/X deadlines](groups.csv),
[attempts](attempts.csv), [eligible native choices and read snapshots](choices.csv),
[join admission expirations and reply codes](rpc_admissions.csv), [interaction estimates](interactions.csv),
[complete summary](summary.json), [corrected independent raw verification](raw_verification_corrected.json),
[original verifier failure](raw_verification.json), [v1 audit provenance](audit_provenance.json),
[v2 audit provenance](audit_provenance_v2.json), and [read-only resume validation](../step26_final_resume_validation_20261010_a.json).
Group snapshots contain the latest local X declaration; `active` does not imply unexpired or remotely visible.
Choice records contain the actual follower read/queue view; admission records contain the leader's current state.
