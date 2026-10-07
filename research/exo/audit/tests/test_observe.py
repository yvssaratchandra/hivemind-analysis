from __future__ import annotations

import io
import json
import unittest
from unittest.mock import patch

from research.exo.audit.observe import (
    assess_probe,
    main,
    memory_available,
    session_consensus,
)


def state_with_memory(node_id: str, available_bytes: int) -> dict[str, object]:
    return {
        "nodeMemory": {
            node_id: {
                "ramAvailable": {"inBytes": available_bytes},
            }
        }
    }


def ready(publisher: str = "cafe01") -> dict[str, object]:
    return {"type": "ready", "publisher_node_id": publisher}


def published(
    *,
    event_id: str = "marker-1",
    claimed_node: str = "beef02",
    publisher: str = "cafe01",
    origin_idx: int = 7,
    session: dict[str, object] | None = None,
    available_bytes: int = 987654,
    payload: object | None = None,
) -> dict[str, object]:
    if session is None:
        session = {"master_node_id": "beef02", "election_clock": 3}
    if payload is None:
        payload = {
            "event_id": event_id,
            "node_id": claimed_node,
            "available_bytes": available_bytes,
            "origin": publisher,
            "marker": "synthetic-audit-value",
        }
    return {
        "type": "published",
        "event_id": event_id,
        "node_id": claimed_node,
        "origin_idx": origin_idx,
        "session": session,
        "available_bytes": available_bytes,
        "payload": payload,
        "publisher_node_id": publisher,
    }


def received(
    service: str = "attacker",
    *,
    event_id: str = "marker-1",
    origin_idx: int | None = 21,
    session: dict[str, object] | None = None,
    payload: object | None = None,
    observer_node_id: str = "cafe01",
    claimed_node: str = "beef02",
    available_bytes: int = 987654,
) -> dict[str, object]:
    if session is None:
        session = {"master_node_id": "beef02", "election_clock": 3}
    if payload is None:
        payload = {
            "event_id": event_id,
            "origin": "beef02",
            "event": {
                "NodeGatheredInfo": {
                    "eventId": event_id,
                    "nodeId": claimed_node,
                    "when": "master-overwritten-time",
                    "info": {"ram_available": {"in_bytes": available_bytes}},
                }
            },
        }
    return {
        "type": "received",
        "observer": service,
        "observer_node_id": observer_node_id,
        "event_id": event_id,
        "origin_idx": origin_idx,
        "session": session,
        "payload": payload,
    }


def snapshots(
    target: str = "beef02", value: int = 987654
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
    before = {
        "honest-a": state_with_memory(target, 111),
        "honest-b": state_with_memory(target, 222),
    }
    after = {
        "honest-a": state_with_memory(target, value),
        "honest-b": state_with_memory(target, value),
    }
    return before, after


class SessionConsensusTests(unittest.TestCase):
    def test_candidate_queue_and_solo_elections_do_not_fake_convergence(self) -> None:
        logs = {
            "honest-a": """honest-a-1 | 2026-10-06T16:15:43.872Z [ \x1b[1mINFO    \x1b[0m ] \x1b[1mStarting node cafe01\x1b[0m
honest-a-1 | 2026-10-06T16:15:44.105Z [ \x1b[34m\x1b[1mDEBUG   \x1b[0m ] \x1b[34m\x1b[1mElection finished, new SessionId(master_node_id='cafe01' election_clock=0) with queue [ElectionMessage(clock=2, proposed_session=SessionId(master_node_id='beef02' election_clock=9))]\x1b[0m
""",
            "honest-b": """honest-b-1 | 2026-10-06T16:15:44.009Z [ \x1b[1mINFO    \x1b[0m ] \x1b[1mStarting node beef02\x1b[0m
honest-b-1 | 2026-10-06T16:15:44.164Z [ \x1b[34m\x1b[1mDEBUG   \x1b[0m ] \x1b[34m\x1b[1mElection finished, new SessionId(master_node_id='beef02' election_clock=0) with queue [ElectionMessage(clock=2, proposed_session=SessionId(master_node_id='cafe01' election_clock=9))]\x1b[0m
""",
        }

        result = session_consensus(logs)

        self.assertFalse(result["converged"])
        self.assertIsNone(result["session"])
        self.assertIn(
            "latest committed sessions do not match across services", result["reasons"]
        )

    def test_matching_post_connection_sessions_converge(self) -> None:
        logs = {
            "honest-a": """Starting node cafe01
Current session: master_node_id='cafe01' election_clock=0
Election finished, new SessionId(master_node_id='beef02' election_clock=2) with queue [ElectionMessage(clock=7, proposed_session=SessionId(master_node_id='cafe01' election_clock=8))]
""",
            "honest-b": """Starting node beef02
Current session: master_node_id='beef02' election_clock=0
Current session: master_node_id='beef02' election_clock=2
""",
        }

        result = session_consensus(logs)

        self.assertTrue(result["converged"])
        self.assertEqual(
            result["node_ids"], {"honest-a": "cafe01", "honest-b": "beef02"}
        )
        self.assertEqual(
            result["session"], {"master_node_id": "beef02", "election_clock": 2}
        )

    def test_ansi_prefixed_startup_logs_preserve_ids_and_committed_session(self) -> None:
        logs = {
            "honest-a": """honest-a-1 | 2026-10-06T16:15:43.872Z [ 2026-10-06 16:15:43.854 | \x1b[1mINFO    \x1b[0m | exo.main:create:75 ] \x1b[1mStarting node b88da74f76f33a993fa135e86fb62206\x1b[0m
honest-a-1 | 2026-10-06T16:15:47.393Z [ 2026-10-06 16:15:47.389 | \x1b[34m\x1b[1mDEBUG   \x1b[0m | exo.shared.election:_campaign:238 ] \x1b[34m\x1b[1mElection finished, new SessionId(master_node_id='b88da74f76f33a993fa135e86fb62206' election_clock=0) with queue [ElectionMessage(proposed_session=SessionId(master_node_id='69c8b34f72e5b64fc0b501c0d5e8a531', election_clock=99))]\x1b[0m
honest-a-1 | 2026-10-06T16:15:47.395Z [ 2026-10-06 16:15:47.390 | \x1b[34m\x1b[1mDEBUG   \x1b[0m | exo.shared.election:elect:115 ] \x1b[34m\x1b[1mCurrent session: master_node_id='b88da74f76f33a993fa135e86fb62206' election_clock=0\x1b[0m
""",
            "honest-b": """honest-b-1 | 2026-10-06T16:15:44.009Z [ 2026-10-06 16:15:43.965 | \x1b[1mINFO    \x1b[0m | exo.main:create:75 ] \x1b[1mStarting node 69c8b34f72e5b64fc0b501c0d5e8a531\x1b[0m
honest-b-1 | 2026-10-06T16:15:47.380Z [ 2026-10-06 16:15:47.376 | \x1b[34m\x1b[1mDEBUG   \x1b[0m | exo.shared.election:_campaign:238 ] \x1b[34m\x1b[1mElection finished, new SessionId(master_node_id='b88da74f76f33a993fa135e86fb62206' election_clock=0) with queue [ElectionMessage(proposed_session=SessionId(master_node_id='69c8b34f72e5b64fc0b501c0d5e8a531', election_clock=0))]\x1b[0m
honest-b-1 | 2026-10-06T16:15:47.383Z [ 2026-10-06 16:15:47.377 | \x1b[34m\x1b[1mDEBUG   \x1b[0m | exo.shared.election:elect:115 ] \x1b[34m\x1b[1mCurrent session: master_node_id='b88da74f76f33a993fa135e86fb62206' election_clock=0\x1b[0m
""",
        }

        result = session_consensus(logs)

        self.assertTrue(result["converged"])
        self.assertEqual(
            result["node_ids"],
            {
                "honest-a": "b88da74f76f33a993fa135e86fb62206",
                "honest-b": "69c8b34f72e5b64fc0b501c0d5e8a531",
            },
        )
        self.assertEqual(
            result["session"],
            {
                "master_node_id": "b88da74f76f33a993fa135e86fb62206",
                "election_clock": 0,
            },
        )

    def test_one_self_elected_peer_is_not_multi_peer_convergence(self) -> None:
        result = session_consensus(
            {
                "honest-a": "Starting node cafe01\nCurrent session: master_node_id='cafe01' election_clock=0\n"
            }
        )

        self.assertFalse(result["converged"])
        self.assertIn("exactly two peer logs are required", result["reasons"])

    def test_campaign_started_after_committed_session_invalidates_convergence(self) -> None:
        logs = {
            "honest-a": """Starting node cafe01
Current session: master_node_id='beef02' election_clock=2
Starting new campaign
Campaign started
Election 3 started
""",
            "honest-b": """Starting node beef02
Current session: master_node_id='beef02' election_clock=2
""",
        }

        result = session_consensus(logs)

        self.assertFalse(result["converged"])
        self.assertIn(
            "honest-a: election campaign remains in flight after latest committed session",
            result["reasons"],
        )

    def test_cancelled_campaign_after_session_is_not_left_in_flight(self) -> None:
        logs = {
            "honest-a": """Starting node cafe01
Current session: master_node_id='beef02' election_clock=2
Starting new campaign
Campaign started
Election 3 started
Election 3 cancelled
Election 3 finally
""",
            "honest-b": """Starting node beef02
Current session: master_node_id='beef02' election_clock=2
""",
        }

        self.assertTrue(session_consensus(logs)["converged"])

    def test_latest_session_after_restart_is_required(self) -> None:
        logs = {
            "honest-a": """Starting node cafe01
Current session: master_node_id='cafe01' election_clock=0
Starting node cafe02
Current session: master_node_id='cafe02' election_clock=1
""",
            "honest-b": """Starting node beef02
Current session: master_node_id='cafe02' election_clock=1
""",
        }

        result = session_consensus(logs)

        self.assertTrue(result["converged"])
        self.assertEqual(result["node_ids"]["honest-a"], "cafe02")
        self.assertEqual(result["session"]["master_node_id"], "cafe02")

    def test_mismatched_clock_or_unknown_master_fails(self) -> None:
        logs = {
            "honest-a": "Starting node cafe01\nCurrent session: master_node_id='beef02' election_clock=2\n",
            "honest-b": "Starting node beef02\nCurrent session: master_node_id='beef02' election_clock=3\n",
        }
        result = session_consensus(logs)
        self.assertFalse(result["converged"])
        self.assertIsNone(result["session"])

        logs["honest-a"] = (
            "Starting node cafe01\nCurrent session: master_node_id='dead99' election_clock=2\n"
        )
        logs["honest-b"] = (
            "Starting node beef02\nCurrent session: master_node_id='dead99' election_clock=2\n"
        )
        result = session_consensus(logs)
        self.assertFalse(result["converged"])
        self.assertIn(
            "common session master is not one of the current node IDs",
            result["reasons"],
        )

    def test_malformed_latest_session_does_not_fall_back(self) -> None:
        result = session_consensus(
            {
                "honest-a": "Starting node cafe01\nCurrent session: master_node_id='cafe01' election_clock=0\nCurrent session: no fields\n",
                "honest-b": "Starting node beef02\nCurrent session: master_node_id='beef02' election_clock=0\n",
            }
        )
        self.assertFalse(result["converged"])
        self.assertIsNone(result["session"])
        self.assertIn(
            "honest-a: latest committed session is malformed", result["reasons"]
        )


class MemoryAvailableTests(unittest.TestCase):
    def test_reads_camel_and_snake_case_schemas(self) -> None:
        camel = {
            "nodeMemory": {
                "cafe01": {"ramAvailable": {"inBytes": 12345}},
            }
        }
        snake = {
            "node_memory": {
                "cafe01": {"ram_available": {"in_bytes": 67890}},
            }
        }
        self.assertEqual(memory_available(camel, "cafe01"), 12345)
        self.assertEqual(memory_available(snake, "cafe01"), 67890)
        self.assertIsNone(memory_available(snake, "missing"))
        self.assertIsNone(memory_available({"nodeMemory": []}, "cafe01"))


class ProbeAssessmentTests(unittest.TestCase):
    def test_forged_arm_requires_ack_and_both_post_snapshots(self) -> None:
        before, after = snapshots()
        records = [
            ready(),
            published(),
            received(
                payload=json.dumps(
                    {
                        "origin": "beef02",
                        "event": {
                            "NodeGatheredInfo": {
                                "event_id": "marker-1",
                                "node_id": "beef02",
                                "when": "master-overwritten-time",
                                "info": {
                                    "ram_available": {"in_bytes": 987654}
                                },
                            }
                        },
                    }
                )
            ),
        ]

        result = assess_probe("forged/1", records, before, after)

        self.assertEqual(result["outcome"], "accepted")
        self.assertEqual(result["indexed_occurrences"], [21])
        self.assertEqual(
            result["per_service_marker_matches"],
            {"attacker": [21], "honest-a": [], "honest-b": []},
        )
        self.assertTrue(result["marker_is_synthetic"])
        self.assertTrue(
            all(item["matches_after"] for item in result["state_observations"].values())
        )

    def test_missing_ack_or_state_is_inconclusive(self) -> None:
        before, after = snapshots()
        no_ack = assess_probe("forged/1", [ready(), published()], before, after)
        self.assertEqual(no_ack["outcome"], "inconclusive")
        self.assertIn("matching indexed acknowledgement is missing", no_ack["reasons"])

        partial_after = {"honest-a": after["honest-a"]}
        missing_state = assess_probe(
            "forged/1",
            [ready(), published(), received()],
            before,
            partial_after,
        )
        self.assertEqual(missing_state["outcome"], "inconclusive")
        self.assertEqual(missing_state["indexed_occurrences"], [21])

    def test_false_acknowledgements_are_excluded(self) -> None:
        before, after = snapshots()
        records = [
            ready(),
            published(),
            received(event_id="other-marker"),
            received(session={"master_node_id": "beef02", "election_clock": 4}),
            received(payload={"event_id": "marker-1", "origin": "cafe01"}),
            received(observer_node_id="dead99"),
        ]

        result = assess_probe("forged/1", records, before, after)

        self.assertEqual(result["outcome"], "inconclusive")
        self.assertEqual(result["indexed_occurrences"], [])
        self.assertEqual(result["per_service_marker_matches"]["attacker"], [])
        self.assertIn("matching indexed acknowledgement is missing", result["reasons"])

    def test_payload_for_different_node_or_marker_value_is_not_an_ack(self) -> None:
        before, after = snapshots()
        wrong_node = assess_probe(
            "forged/1",
            [
                ready(),
                published(),
                received(claimed_node="cafe01", available_bytes=987654),
            ],
            before,
            after,
        )
        wrong_memory = assess_probe(
            "forged/1",
            [
                ready(),
                published(),
                received(available_bytes=987655),
            ],
            before,
            after,
        )

        for result in (wrong_node, wrong_memory):
            self.assertEqual(result["outcome"], "inconclusive")
            self.assertEqual(result["indexed_occurrences"], [])
        self.assertIn(
            "received NodeGatheredInfo does not match published marker",
            wrong_node["reasons"],
        )
        self.assertIn(
            "received NodeGatheredInfo does not match published marker",
            wrong_memory["reasons"],
        )

    def test_invalid_global_index_is_not_a_valid_acknowledgement(self) -> None:
        before, after = snapshots()
        result = assess_probe(
            "forged/1",
            [ready(), published(), received(origin_idx=None)],
            before,
            after,
        )

        self.assertEqual(result["outcome"], "inconclusive")
        self.assertEqual(result["indexed_occurrences"], [])

    def test_stale_absence_is_not_observed_not_rejected(self) -> None:
        before, after = snapshots()
        stale = published(
            claimed_node="cafe01",
            session={"master_node_id": "dead99", "election_clock": 99}
        )

        result = assess_probe("stale-session/1", [ready(), stale], before, after)

        self.assertEqual(result["outcome"], "not_observed")
        self.assertIsNone(result["state_observations"]["honest-a"]["after_bytes"])
        self.assertIsNone(result["state_observations"]["honest-b"]["after_bytes"])
        self.assertTrue(any("not observed" in reason for reason in result["reasons"]))
        self.assertFalse(any("reject" in reason.lower() for reason in result["reasons"]))

    def test_stale_absence_still_needs_both_snapshot_objects(self) -> None:
        before, after = snapshots(target="cafe01")
        stale = published(
            claimed_node="cafe01",
            session={"master_node_id": "dead99", "election_clock": 99},
        )

        result = assess_probe(
            "stale-session/1",
            [ready(), stale],
            before,
            {"honest-a": after["honest-a"]},
        )

        self.assertEqual(result["outcome"], "inconclusive")

    def test_replay_counts_unique_global_indices_not_duplicate_acks(self) -> None:
        envelope = published(claimed_node="cafe01", publisher="cafe01")
        once = assess_probe(
            "replay",
            [
                ready("cafe01"),
                envelope,
                dict(envelope),
                received(origin_idx=31, claimed_node="cafe01"),
                received(origin_idx=31, claimed_node="cafe01"),
                received(origin_idx=31, claimed_node="cafe01"),
            ],
            *snapshots(target="cafe01"),
        )
        self.assertEqual(once["outcome"], "indexed_once")
        self.assertEqual(once["indexed_occurrences"], [31])

        reindexed = assess_probe(
            "replay/same-envelope-twice",
            [
                ready("cafe01"),
                envelope,
                dict(envelope),
                received(origin_idx=31, claimed_node="cafe01"),
                received(origin_idx=31, claimed_node="cafe01"),
                received(origin_idx=32, claimed_node="cafe01"),
            ],
            *snapshots(target="cafe01"),
        )
        self.assertEqual(reindexed["outcome"], "reindexed")
        self.assertEqual(reindexed["indexed_occurrences"], [31, 32])

    def test_replay_different_envelopes_is_inconclusive(self) -> None:
        before, after = snapshots(target="cafe01")
        first = published(claimed_node="cafe01", publisher="cafe01")
        second = dict(first, origin_idx=8)
        result = assess_probe(
            "replay",
            [
                ready("cafe01"),
                first,
                second,
                received(origin_idx=31, claimed_node="cafe01"),
            ],
            before,
            after,
        )
        self.assertEqual(result["outcome"], "inconclusive")
        self.assertIn("replay publications do not contain the same envelope", result["reasons"])

    def test_baseline_is_a_no_send_control(self) -> None:
        result = assess_probe("baseline/no-send", [], {}, {})
        self.assertEqual(result["outcome"], "no_send_control")
        self.assertEqual(result["indexed_occurrences"], [])


class SnapshotCliTests(unittest.TestCase):
    def test_snapshot_cli_reads_only_local_endpoints_and_prints_json(self) -> None:
        class FakeResponse(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *_args: object) -> None:
                self.close()

        responses = [FakeResponse(b'"cafe01"'), FakeResponse(b'{"instances":{}}')]
        stdout = io.StringIO()
        with patch("research.exo.audit.observe.urlopen", side_effect=responses) as open_url:
            with patch("sys.stdout", stdout):
                code = main(["--snapshot"])

        self.assertEqual(code, 0)
        open_url.assert_any_call("http://localhost:52415/node_id", timeout=3.0)
        open_url.assert_any_call("http://localhost:52415/state", timeout=3.0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"node_id": "cafe01", "state": {"instances": {}}},
        )


if __name__ == "__main__":
    unittest.main()
