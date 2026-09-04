"""Track D experiment: do the prototype defenses actually change the measured outcomes?

Re-runs Tracks A/B/C with each defense enabled, on the same seeds and axes as the
undefended runs, so the before/after numbers are directly comparable.

* **D1 identity binding** -- vs. Track A. Eclipse stops being free; the attacker must
  grind keypairs, and the cost is quantified in keypair generations.
* **D2 signed declarations** -- vs. Track B. Fabricated declarations are rejected
  outright; what remains is the (still real) cost of minting genuine keypairs.
* **D3 robust aggregation** -- vs. Track C. Already measured in Track C; summarised
  here for the combined picture.
* **D4 weight bounding** -- vs. Track C's weight attack, the cheapest fix of the four.

Run:  .venv/bin/python -m research.experiments.track_d_defenses
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path

import torch

from research.attacks.poison_averaging import AttackConfig, LeastSquaresTask, run_training
from research.attacks.sybil_matchmaking import fabricate_peer_id
from research.common import plotting
from research.common.metrics import Recorder
from research.common.sybil import capture_count, closest_ids, key_to_dhtid, random_ids
from research.defenses.id_binding import (
    BindingPolicy,
    derive_node_id,
    eclipse_cost_under_binding,
    expected_keypairs_for_prefix,
)
from research.defenses.signed_declare import SignedAveragingValidator, signed_subkey
from research.defenses.weight_bound import WeightPolicy, sanitize_weight

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
KEY = "hivemind_run_alpha.all_averagers"
NUM_REPLICAS = 5
SEEDS = (0, 1, 2)


def run_d1_identity_binding(seed: int) -> None:
    """D1 vs. Track A: crafted routing addresses stop being admissible."""
    recorder = Recorder("track_d_id_binding", params={"key": KEY, "num_replicas": NUM_REPLICAS, "seed": seed})
    target = key_to_dhtid(KEY)

    # 1. the Track A attacker's addresses no longer pass admission
    strict = BindingPolicy(strict=True)
    attacker_peer_ids = [fabricate_peer_id(i, salt="d1") for i in range(NUM_REPLICAS)]
    crafted = closest_ids(target, NUM_REPLICAS)
    for node_id, peer_id in zip(crafted, attacker_peer_ids):
        strict.admit(node_id, peer_id)

    # 2. honest peers, whose addresses are derived correctly, are unaffected
    honest = BindingPolicy(strict=True)
    for index in range(50):
        peer_id = fabricate_peer_id(index, salt="honest")
        honest.admit(derive_node_id(peer_id), peer_id)

    print(f"  crafted attacker addresses rejected: {strict.rejected}/{strict.checked}")
    print(f"  honest derived addresses rejected:   {honest.rejected}/{honest.checked}")
    recorder.record(
        case="crafted_attacker", checked=strict.checked, rejected=strict.rejected, rejection_rate=strict.rejection_rate
    )
    recorder.record(
        case="honest_derived", checked=honest.checked, rejected=honest.rejected, rejection_rate=honest.rejection_rate
    )

    # 3. what the attack now costs: how close can a grinding attacker actually get?
    print("  cost of getting near the target key by grinding keypairs:")
    xs, ys = [], []
    for prefix_bits in (8, 16, 24, 32, 40, 48):
        keypairs = expected_keypairs_for_prefix(prefix_bits)
        eclipse = eclipse_cost_under_binding(prefix_bits, NUM_REPLICAS)
        xs.append(prefix_bits)
        ys.append(eclipse)
        recorder.record(
            case="grinding_cost",
            prefix_bits=prefix_bits,
            expected_keypairs_per_identity=keypairs,
            keypairs_for_total_eclipse=eclipse,
        )
        print(f"    {prefix_bits:>3}-bit prefix: {keypairs:.3e} keypairs each, {eclipse:.3e} for a total eclipse")

    # 4. does grinding at a realistic budget even reach the replica set?
    budget = 1_000_000  # ~2^20 keypairs, a generous single-machine budget
    rng = random.Random(seed)
    honest_ids = random_ids(200, rng=rng)
    ground = []
    for index in range(64):  # sample of what a grinder might obtain
        peer_id = fabricate_peer_id(index, salt=f"grind{seed}")
        ground.append(derive_node_id(peer_id))
    captured = capture_count(target, honest_ids, ground, NUM_REPLICAS)
    print(f"  64 legitimately-derived attacker addresses capture {captured}/{NUM_REPLICAS} replica slots")
    recorder.record(
        case="derived_capture", num_attacker_identities=64, captured=captured, num_replicas=NUM_REPLICAS, budget=budget
    )

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    plotting.line_plot(
        {"keypairs for a total eclipse": (xs, ys)},
        path=RESULTS_DIR / "track_d_id_binding_cost.png",
        title="D1: eclipse cost once node_id is bound to the public key",
        xlabel="prefix bits the attacker must match",
        ylabel="expected keypair generations",
        logx=False,
    )
    print(f"wrote {RESULTS_DIR / 'track_d_id_binding_cost.png'}")


def run_d2_signed_declarations() -> None:
    """D2 vs. Track B: unsigned fabricated declarations are rejected."""
    from hivemind.dht.crypto import RSASignatureValidator
    from hivemind.dht.validation import DHTRecord
    from hivemind.utils.timed_storage import get_dht_time

    recorder = Recorder("track_d_signed_declare", params={})
    validator = RSASignatureValidator()
    guard = SignedAveragingValidator(prefixes=["run"])
    group_key = b"run.0b"
    expiration = get_dht_time() + 60

    # Track B's attack: bare peer_id subkeys, no owner marker, no signature
    fabricated_accepted = 0
    for index in range(200):
        record = DHTRecord(
            key=group_key,
            subkey=fabricate_peer_id(index, salt="d2").to_bytes(),
            value=b"\xc3",  # msgpack-ish payload stand-in
            expiration_time=expiration,
        )
        if guard.validate(record) and validator.validate(record):
            fabricated_accepted += 1

    # an honest peer declaring itself, owner-tagged and signed
    honest_peer = fabricate_peer_id(0, salt="honest-d2")
    subkey = signed_subkey(honest_peer, validator)
    honest_record = DHTRecord(key=group_key, subkey=subkey, value=b"\xc3", expiration_time=expiration)
    signed_value = validator.sign_value(honest_record)
    honest_signed = DHTRecord(key=group_key, subkey=subkey, value=signed_value, expiration_time=expiration)
    honest_accepted = guard.validate(honest_signed) and validator.validate(honest_signed)

    # and the impersonation case: attacker signs, but claims someone else's marker is absent
    tampered = DHTRecord(key=group_key, subkey=subkey, value=b"\xc3", expiration_time=expiration)
    tampered_accepted = guard.validate(tampered) and validator.validate(tampered)

    print(f"  fabricated unsigned declarations accepted: {fabricated_accepted}/200 (was 200/200 in Track B)")
    print(f"  honest owner-tagged signed declaration accepted: {honest_accepted}")
    print(f"  owner-tagged but unsigned (forged) declaration accepted: {tampered_accepted}")

    recorder.record(case="fabricated_unsigned", attempted=200, accepted=fabricated_accepted)
    recorder.record(case="honest_signed", attempted=1, accepted=int(honest_accepted))
    recorder.record(case="ownertagged_unsigned", attempted=1, accepted=int(tampered_accepted))

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")


def run_d4_weight_bounding() -> None:
    """D4 vs. Track C's weight attack: the cheapest fix, measured on the same axis."""
    recorder = Recorder("track_d_weight_bound", params={"seeds": list(SEEDS)})
    weights = (1, 10, 100, 1_000, 10_000, 100_000, 1_000_000)
    num_peers = 16
    series, policies = (
        {},
        {
            "no validation (Hivemind today)": None,
            "D4 sanitize only": WeightPolicy(sanitize=True, clip=False),
            "D4 sanitize + clip": WeightPolicy(sanitize=True, clip=True),
        },
    )

    for label, policy in policies.items():
        means = []
        for claimed in weights:
            distances = []
            for seed in SEEDS:
                task = LeastSquaresTask(num_peers=num_peers, seed=seed)
                effective = float(claimed)
                if policy is not None:
                    honest_weights = [1.0] * (num_peers - 1)
                    accepted = policy.apply(float(claimed), honest_weights)
                    effective = 0.0 if accepted is None else accepted
                result = run_training(
                    task,
                    aggregator="weighted_mean (Hivemind today)",
                    attack=AttackConfig(strategy="sign_flip", local_fraction=1 / num_peers, weight=effective),
                )
                distances.append(min(result.final_distance, 1e4))
                recorder.record(
                    policy=label,
                    claimed_weight=claimed,
                    effective_weight=effective,
                    seed=seed,
                    final_distance=result.final_distance,
                )
            means.append(sum(distances) / len(distances))
        series[label] = (weights, means)
        print(f"  {label:<32} weight=1: {means[0]:.4f} -> weight=1e6: {means[-1]:.4f}")

    # non-finite weights, the case that currently poisons the denominator outright
    print(
        f"  sanitize_weight(nan) -> {sanitize_weight(float('nan'))}, "
        f"sanitize_weight(inf) -> {sanitize_weight(float('inf'))}, "
        f"sanitize_weight(-5) -> {sanitize_weight(-5.0)}"
    )

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    plotting.line_plot(
        series,
        path=RESULTS_DIR / "track_d_weight_bound.png",
        title="D4: bounding the sender-supplied averaging weight",
        xlabel="weight claimed by one malicious peer in 16",
        ylabel="final distance to optimum (clipped)",
        logx=True,
    )
    print(f"wrote {RESULTS_DIR / 'track_d_weight_bound.png'}")


def run_d3_summary() -> None:
    """D3 recap: robust aggregation against the same attacks, on one axis."""
    from research.defenses.robust_agg import aggregate

    torch.manual_seed(0)
    honest = torch.randn(15, 32)
    poisoned = torch.cat([honest, torch.full((1, 32), 1e6)])
    weights = torch.cat([torch.ones(15), torch.tensor([1e6])])
    truth = honest.mean(dim=0)

    print("  displacement from the honest mean with one extreme peer in 16:")
    for name in ("weighted_mean (Hivemind today)", "krum", "multi_krum", "coordinate_median", "trimmed_mean"):
        result = aggregate(name, poisoned, weights=weights, num_byzantine=4)
        print(f"    {name:<32} {float(torch.norm(result - truth)):.4f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1234)
    args = parser.parse_args()

    print("== D1: bind node_id to the cryptographic identity (vs Track A) ==")
    run_d1_identity_binding(args.seed)

    print("\n== D2: signed averager declarations (vs Track B) ==")
    run_d2_signed_declarations()

    print("\n== D3: robust aggregation (vs Track C) ==")
    run_d3_summary()

    print("\n== D4: bound the averaging weight (vs Track C) ==")
    run_d4_weight_bounding()


if __name__ == "__main__":
    main()
