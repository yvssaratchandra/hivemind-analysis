"""Track C experiment: what does a captured group cost the model?

Joins Track B's local malicious fraction ``f_l`` to actual learning damage, and
compares Hivemind's current aggregation against Byzantine-robust alternatives.

Result sets:

* ``track_c_damage``  -- distance to the known optimum vs. ``f_l``, per aggregator
  and attack. Shows both how little is needed to break plain averaging and where
  the robust rules themselves give out.
* ``track_c_weight``  -- the Hivemind-specific one: a *single* malicious peer that
  simply claims a large ``weight``. That field crosses the wire unvalidated
  (``allreduce.py:339-347`` -> ``partition.py:242``), so it is leverage no amount of
  group-composition defense addresses.
* ``track_c_stealth`` -- targeted poisoning: damage to chosen coordinates alongside
  the attacker's contribution norm, showing a norm filter would not fire.

Run:  .venv/bin/python -m research.experiments.track_c_learning_harm
"""

from __future__ import annotations

import argparse
from pathlib import Path

from research.attacks.poison_averaging import AttackConfig, LeastSquaresTask, run_training
from research.common import plotting
from research.common.metrics import Recorder, summarize

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

NUM_PEERS = 16
LOCAL_FRACTIONS = tuple(index / NUM_PEERS for index in range(0, 9))  # 0 .. 0.5
AGGREGATORS = ("weighted_mean (Hivemind today)", "krum", "multi_krum", "coordinate_median", "trimmed_mean")
ATTACKS = ("sign_flip", "large_norm", "non_finite", "targeted_stealth")
TARGET_COORDINATES = tuple(range(8))
SEEDS = (0, 1, 2)
CLIP = 1e4  # cap for plotting: divergence is unbounded and would flatten every other curve


def run_damage_sweep() -> None:
    """Damage vs. local malicious fraction, for every aggregator x attack pair."""
    recorder = Recorder(
        "track_c_damage",
        params={"num_peers": NUM_PEERS, "seeds": list(SEEDS), "target_coordinates": list(TARGET_COORDINATES)},
    )

    for attack_name in ATTACKS:
        series, errors = {}, {}
        for aggregator in AGGREGATORS:
            means, stds = [], []
            for local_fraction in LOCAL_FRACTIONS:
                distances = []
                for seed in SEEDS:
                    task = LeastSquaresTask(num_peers=NUM_PEERS, seed=seed)
                    result = run_training(
                        task,
                        aggregator=aggregator,
                        attack=AttackConfig(
                            strategy=attack_name,
                            local_fraction=local_fraction,
                            target_coordinates=TARGET_COORDINATES,
                        ),
                    )
                    distances.append(min(result.final_distance, CLIP))
                    recorder.record(
                        attack=attack_name,
                        aggregator=aggregator,
                        local_malicious_fraction=local_fraction,
                        num_malicious=int(round(local_fraction * NUM_PEERS)),
                        seed=seed,
                        final_distance=result.final_distance,
                        final_loss=result.final_loss,
                        final_targeted_error=result.final_targeted_error,
                        diverged=result.diverged,
                        stealth_ratio=result.stealth_ratio,
                    )
                stats = summarize(distances)
                means.append(stats["mean"])
                stds.append(stats["std"])
            series[aggregator] = (LOCAL_FRACTIONS, means)
            errors[aggregator] = stds

        plotting.line_plot(
            series,
            path=RESULTS_DIR / f"track_c_damage_{attack_name}.png",
            title=f"Learning damage vs. captured share of the group ({attack_name})",
            xlabel="local malicious fraction f_l (from Track B)",
            ylabel="final distance to optimum (clipped)",
            errorbars=errors,
            reference_line=None,
        )
        clean = series["weighted_mean (Hivemind today)"][1][0]
        breaking = next(
            (
                fraction
                for fraction, value in zip(LOCAL_FRACTIONS, series["weighted_mean (Hivemind today)"][1])
                if value > 100 * max(clean, 1e-6)
            ),
            None,
        )
        print(f"  {attack_name:<17} plain averaging breaks at f_l = {breaking}")

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")


def run_weight_attack() -> None:
    """One malicious peer in sixteen, varying only the ``weight`` it claims.

    Hivemind derives the honest weight from a peer's self-reported sample count
    (``grad_averager.py:196``) and the receiver never checks it, so this is a
    one-line attack that no group-composition defense touches.
    """
    recorder = Recorder("track_c_weight", params={"num_peers": NUM_PEERS, "seeds": list(SEEDS)})
    weights = (1, 10, 100, 1_000, 10_000, 100_000, 1_000_000)
    series, errors = {}, {}

    for aggregator in ("weighted_mean (Hivemind today)", "krum", "coordinate_median"):
        means, stds = [], []
        for weight in weights:
            distances = []
            for seed in SEEDS:
                task = LeastSquaresTask(num_peers=NUM_PEERS, seed=seed)
                result = run_training(
                    task,
                    aggregator=aggregator,
                    attack=AttackConfig(
                        strategy="sign_flip",
                        local_fraction=1 / NUM_PEERS,  # a single malicious peer
                        weight=float(weight),
                        target_coordinates=TARGET_COORDINATES,
                    ),
                )
                distances.append(min(result.final_distance, CLIP))
                recorder.record(
                    aggregator=aggregator,
                    claimed_weight=weight,
                    num_malicious=1,
                    local_malicious_fraction=1 / NUM_PEERS,
                    seed=seed,
                    final_distance=result.final_distance,
                    diverged=result.diverged,
                )
            stats = summarize(distances)
            means.append(stats["mean"])
            stds.append(stats["std"])
        series[aggregator] = (weights, means)
        errors[aggregator] = stds
        print(f"  {aggregator:<30} weight=1: {means[0]:.4f} -> weight=1e6: {means[-1]:.4f}")

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    plotting.line_plot(
        series,
        path=RESULTS_DIR / "track_c_weight_attack.png",
        title="One peer in 16 simply claims a larger weight",
        xlabel="weight claimed by the malicious peer (unvalidated on receipt)",
        ylabel="final distance to optimum (clipped)",
        errorbars=errors,
        logx=True,
    )
    print(f"wrote {RESULTS_DIR / 'track_c_weight_attack.png'}")


def run_stealth_analysis() -> None:
    """Targeted poisoning: damage to chosen coordinates, and how visible the attacker is."""
    recorder = Recorder("track_c_stealth", params={"num_peers": NUM_PEERS, "seeds": list(SEEDS)})
    series, norm_series = {}, {}

    for aggregator in ("weighted_mean (Hivemind today)", "krum", "multi_krum", "coordinate_median"):
        targeted, norms = [], []
        for local_fraction in LOCAL_FRACTIONS:
            errors_, ratios = [], []
            for seed in SEEDS:
                task = LeastSquaresTask(num_peers=NUM_PEERS, seed=seed)
                result = run_training(
                    task,
                    aggregator=aggregator,
                    attack=AttackConfig(
                        strategy="targeted_stealth",
                        local_fraction=local_fraction,
                        target_coordinates=TARGET_COORDINATES,
                    ),
                )
                errors_.append(min(result.final_targeted_error, CLIP))
                ratios.append(result.stealth_ratio if result.stealth_ratio != float("inf") else 0.0)
                recorder.record(
                    aggregator=aggregator,
                    local_malicious_fraction=local_fraction,
                    seed=seed,
                    targeted_error=result.final_targeted_error,
                    overall_distance=result.final_distance,
                    stealth_ratio=result.stealth_ratio,
                )
            targeted.append(summarize(errors_)["mean"])
            norms.append(summarize(ratios)["mean"])
        series[aggregator] = (LOCAL_FRACTIONS, targeted)
        norm_series[aggregator] = norms

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    plain = series["weighted_mean (Hivemind today)"]
    plain_norms = norm_series["weighted_mean (Hivemind today)"]
    print("  plain averaging, targeted attack -- damage rises while the attacker stays norm-indistinguishable:")
    for fraction, error, ratio in zip(plain[0], plain[1], plain_norms):
        if fraction:
            print(f"    f_l={fraction:.3f}  targeted error {error:>7.3f}  attacker/honest norm {ratio:.2f}")

    plotting.line_plot(
        series,
        path=RESULTS_DIR / "track_c_targeted.png",
        title="Stealthy targeted poisoning of 8 of 64 coordinates",
        xlabel="local malicious fraction f_l",
        ylabel="error on the attacker's target coordinates",
    )
    print(f"wrote {RESULTS_DIR / 'track_c_targeted.png'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    print("== Track C: damage vs local malicious fraction ==")
    run_damage_sweep()

    print("\n== Track C: unvalidated averaging weight ==")
    run_weight_attack()

    print("\n== Track C: stealthy targeted poisoning ==")
    run_stealth_analysis()


if __name__ == "__main__":
    main()
