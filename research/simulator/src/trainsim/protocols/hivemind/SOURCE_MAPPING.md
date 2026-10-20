# Hivemind adapter source map

## Optional numerical backend

`numerical.py` is a separate fixed-group backend for the announcement learning
study. Each participant has its own ordered roster and coordinate allocation.
It applies reducer mean-minus-sender-input corrections at each recipient's
own coordinate ranges, and preserves local inputs for failed remote reducers.
Absent expected senders expire before the reducer completes. NumPy is optional;
the existing independent-work timing adapter below retains its original model.

This numerical backend assumes reliable delivery before deadlines, equal
unspecified bandwidths and compatible stream lengths. It explicitly rejects
incompatible lengths rather than guessing the native error sequence. Leadership,
the fixed grouping and attack schedule are scenario inputs. It makes no
wall-clock prediction; the timed adapter's unresolved calibration is not used
to estimate convergence speed. Differential evidence is required before running
the learning suite, including native membership fallback and ordering fixtures.

The adapter implements a timing and control-flow model of the checked-out
Hivemind sources. The simulator does not import Hivemind, PyTorch, or NumPy at
runtime. The table identifies the primary code used to derive each behavior.

| Simulated behavior | Hivemind source |
|---|---|
| Candidate ordering, DHT refresh, declarations, join requests, expiry-time assembly, follower waiting and disband handling | `hivemind/averaging/matchmaking.py` (`Matchmaking.look_for_group`, `_request_join_potential_leaders`, `_request_join_group`, `PotentialLeaders`) |
| Group-key bucket update after a roster is formed | `hivemind/averaging/key_manager.py` (`GroupKeyManager.update_key_on_group_assembled`) |
| Equal-share allocation and assignment of contiguous tensor chunks to reducers | `hivemind/averaging/load_balancing.py` (`load_balance_peers`) and `hivemind/averaging/partition.py` (`TensorPartContainer`) |
| Per-sender input stream, per-reducer result stream, timeout, sender exclusion and reducer fallback | `hivemind/averaging/allreduce.py` (`AllReduceRunner`, `_communicate_with_peer`, `rpc_aggregate_part`, `_accumulate_parts_streaming`, `_handle_missing_senders`) and `hivemind/averaging/partition.py` (`TensorPartReducer`) |
| Public configuration defaults and timeout constraints | `hivemind/averaging/averager.py` (`DecentralizedAverager.__init__`) |

## Fidelity and approximations

The adapter follows the source's candidate priority `(expiration, PeerID
bytes)`, keeps node matchmaking state separate, accepts joins while a node is
still looking for a group, assembles a minimum-size group at declaration
expiry, and streams each configured tensor chunk through its assigned
reducer. Sender and reducer inactivity timers reset on chunk progress. A
timed-out recipient uses its own chunk for the remaining part of that reducer's
stream. All-reduce receipts name contributing **work IDs**, not node IDs, so
independent work items remain auditable.

The simulation uses a coherent registry clock. It does not model Hivemind's
maximum DHT clock discrepancy, `PotentialLeaders.max_assured_time` background
refresh trigger, or its exact DHT RPC scheduling. Each reducer starts a
sender-timeout clock for every expected sender when its local all-reduce starts;
the first received chunk and each later chunk reset that sender's inactivity
timer. This captures the native missing-sender startup bound, but does not
model its distinct `all_senders_started` event/barrier. Input buffering is
capped to one chunk beyond the reducer's current position, while native
buffering is controlled by its asynchronous stream prefetch setting. The
simulator models equal contributor weights for this baseline and does not
serialize/compress tensor values. If every contributor to a reducer chunk
fails, the simulator leaves that output unavailable and the recipient falls
back; this avoids assigning semantics to the native zero-denominator case.
Group membership and roster work IDs are treated as committed at assembly; a
follower that never receives its announcement is not modeled as a separate
local RPC failure state. The search-deadline and lease-expiry timers tie in
favor of the already scheduled search-deadline callback; this boundary has
not been differentially validated against asyncio scheduling.
