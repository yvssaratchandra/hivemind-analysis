"""Stdlib-only validation tests for the Exo local publisher."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from probe import (  # noqa: E402
    Control,
    deterministic_event_id,
    effective_election_clock,
    make_publisher_node_id,
    strict_json_loads,
    validate_control,
)


PUBLISHER = "a123456789abcdef"
HONEST_A = "b123456789abcdef"
HONEST_B = "c123456789abcdef"
MASTER = HONEST_A
SEED = 37


def control_value(arm: str = "self") -> dict[str, object]:
    target = HONEST_B if arm == "forged" else PUBLISHER
    return {
        "session": {"master_node_id": MASTER, "election_clock": 4},
        "node_ids": {"honest-a": HONEST_A, "honest-b": HONEST_B},
        "target_node_id": target,
        "available_bytes": 1024 * 1024,
        "origin": "audit-system-37",
        "origin_idx": 0,
        "arm": arm,
        "seed": SEED,
    }


class ProbeValidationTests(unittest.TestCase):
    def validate(self, value: dict[str, object], arm: str = "self") -> Control:
        return validate_control(
            value,
            configured_arm=arm,
            configured_seed=SEED,
            publisher_node_id=PUBLISHER,
        )

    def test_self_control_accepts_only_bounded_expected_target(self) -> None:
        control = self.validate(control_value("self"))
        self.assertEqual(control.target_node_id, PUBLISHER)
        self.assertEqual(control.origin_idx, 0)
        self.assertEqual(control.available_bytes, 1024 * 1024)

    def test_forged_arm_targets_honest_b(self) -> None:
        control = self.validate(control_value("forged"), arm="forged")
        self.assertEqual(control.target_node_id, HONEST_B)

    def test_rejects_other_node_target_for_self_arm(self) -> None:
        value = control_value("self")
        value["target_node_id"] = HONEST_B
        with self.assertRaisesRegex(ValueError, "target_node_id"):
            self.validate(value)

    def test_rejects_mismatched_arm_or_seed(self) -> None:
        value = control_value("self")
        value["arm"] = "forged"
        with self.assertRaisesRegex(ValueError, "does not match configured arm"):
            self.validate(value)
        value = control_value("self")
        value["seed"] = SEED + 1
        with self.assertRaisesRegex(ValueError, "does not match configured seed"):
            self.validate(value)

    def test_rejects_boolean_and_over_limit_memory_values(self) -> None:
        value = control_value("self")
        value["available_bytes"] = True
        with self.assertRaisesRegex(ValueError, "available_bytes"):
            self.validate(value)
        value = control_value("self")
        value["available_bytes"] = 2 * 1024**3 + 1
        with self.assertRaisesRegex(ValueError, "available_bytes"):
            self.validate(value)

    def test_rejects_non_finite_json(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-finite"):
            strict_json_loads('{"available_bytes": NaN}')

    def test_stale_session_decrements_even_zero_clock(self) -> None:
        value = control_value("stale-session")
        value["session"] = {"master_node_id": MASTER, "election_clock": 0}
        control = self.validate(value, arm="stale-session")
        self.assertEqual(effective_election_clock(control), -1)

    def test_marker_id_is_stable_for_seed_and_arm(self) -> None:
        first = deterministic_event_id(SEED, "self")
        self.assertEqual(first, deterministic_event_id(SEED, "self"))
        self.assertNotEqual(first, deterministic_event_id(SEED + 1, "self"))
        self.assertNotEqual(first, deterministic_event_id(SEED, "forged"))

    def test_generated_identity_is_valid_hex_and_handles_zero_bytes(self) -> None:
        self.assertEqual(make_publisher_node_id(bytes(16)), "1")
        self.assertTrue(make_publisher_node_id(bytes.fromhex("0001" + "22" * 14)))


if __name__ == "__main__":
    unittest.main()
