"""Focused state reconstruction checks for Step 24 X search timelines."""

from pathlib import Path
import tempfile
import unittest

from analyze_step24_x_states import analyze_to_output, reconstruct_trial


START = 100.0
DURATION = 60.0
END = START + DURATION
IDS = {"X": "x", "H0": "h0", "H1": "h1"}


def config(*, duration=DURATION):
    return {
        "case": {"name": "range_1_2", "mode": "refresh"},
        "seed": 2401,
        "peer_ids": IDS.copy(),
        "started_monotonic": START,
        "duration": duration,
    }


def peer_pid(peer):
    return {"x": 1, "h0": 2, "h1": 3}.get(peer, 4)


def event(kind, at, *, peer="x", other="", number=None, pid=None, **detail):
    if number is None:
        number = int(round((at - START) * 100)) + 1
    if pid is None:
        pid = peer_pid(peer)
    return {
        "kind": kind,
        "peer": peer,
        "other": other,
        "pid": pid,
        "event_number": number,
        "monotonic": float(at),
        "wall_time": 1_000.0 + float(at - START),
        "detail": detail,
    }


def election(group_id, at, *, leader="H0", leader_peer=None, step_id=None,
             expiration=1_200.0, x_expiration=1_210.0, active=True,
             roster=None, event_number=None):
    if leader_peer is None:
        leader_peer = IDS[leader]
    if step_id is None:
        step_id = f"{leader}:1"
    if roster is None:
        roster = [leader_peer, "h1"]
    return {
        "case": "range_1_2",
        "seed": 2401,
        "group_id": group_id,
        "leader_name": leader,
        "leader_peer_id": leader_peer,
        "leader_step_id": step_id,
        "assembly_wall": 1_000.0 + (at - START),
        "leader_expiration": expiration,
        "x_expiration": x_expiration,
        "expiration_gap_x_minus_leader": (
            None if x_expiration is None else x_expiration - expiration
        ),
        "x_ad_active": active,
        "x_remaining_seconds": (
            None if x_expiration is None else x_expiration - (1_000.0 + at - START)
        ),
        "leader_remaining_seconds": expiration - (1_000.0 + at - START),
        "leader_x_same_group_key": True,
        "follower_views": [],
        "roster": list(roster),
        "group_size": len(roster),
        "assembly_event_ref": (
            f"{peer_pid(leader_peer)}:{event_number or int(round((at - START) * 100) + 1)}"
        ),
        "source": "synthetic-test",
    }


def base_step(*, step_id="X:1", start=101.0):
    return [
        event("step_start", start - 0.01, peer="x", name="X", sequence=1,
              step_id=step_id),
        event("search_start", start, peer="x", step_id=step_id,
              group_key="x-key", deadline=start + 8.0),
    ]


def intervals_for(result, state, *, period="submission"):
    return [
        row for row in result["timeline"]
        if row["state"] == state and row.get("period", period) == period
    ]


def state_seconds(result, state, *, period="submission"):
    return sum(row["duration"] for row in intervals_for(result, state, period=period))


def assembly(at, group_id, *, peer="x", step_id="X:1", roster=None,
             event_number=None):
    if roster is None:
        roster = ["x", "h0"]
    if event_number is None:
        event_number = int(round((at - START) * 100)) + 1
    return event("assembled", at, peer=peer, step_id=step_id, group_id=group_id,
                 roster=roster, event_number=event_number)


def training(at, step_id, *, finished, ready):
    return event("training_scheduled", at, peer="x", step_id=step_id,
                 finished=finished, delay=ready-finished, ready=ready)


class Step24XStateTests(unittest.TestCase):
    def reconstruct(self, events, elections=(), *, trial_config=None):
        return reconstruct_trial(trial_config or config(), events, list(elections))

    def test_ad_withdrawal_and_search_end_do_not_clear_leading(self):
        events = base_step()
        events.extend([
            event("ad_started", 102.0, peer="x", other="x-key", active=True,
                  expiration=1_030.0),
            assembly(103.0, "gx", step_id="X:1"),
            event("ad_started", 104.0, peer="x", other="x-key", active=False,
                  expiration=1_030.0),
            event("search_end", 105.0, peer="x", step_id="X:1"),
            event("step_result", 120.0, peer="x", step_id="X:1", finished=120.0,
                  status="completed"),
            training(120.1, "X:1", finished=120.0, ready=125.0),
        ])
        row = election("gx", 103.0, leader="X", leader_peer="x", step_id="X:1",
                       roster=["x", "h0"], event_number=301)
        result = self.reconstruct(events, [row])

        self.assertAlmostEqual(state_seconds(result, "leading"), 17.0)
        self.assertEqual(result["searches"][0]["outcome"], "led")
        self.assertEqual(result["searches"][0]["led_group_ids"], ["gx"])

    def test_accepted_then_begin_allreduce_stays_following_through_finish(self):
        events = base_step()
        events.extend([
            event("native_client_reply", 103.0, peer="x", other="h0",
                  step_id="X:1", code="ACCEPTED", phase="handshake", group_id="g1"),
            event("native_client_reply", 104.0, peer="x", other="h0",
                  step_id="X:1", code="BEGIN_ALLREDUCE", phase="waiting_for_group",
                  group_id="g1"),
            assembly(104.0, "g1", peer="h0", step_id="H0:1", roster=["x", "h0"],
                     event_number=401),
            event("allreduce_start", 105.0, peer="x", step_id="X:1", group_id="g1"),
            event("allreduce_end", 106.0, peer="x", step_id="X:1", group_id="g1",
                  error=None),
            event("search_end", 117.0, peer="x", step_id="X:1"),
            event("step_result", 120.0, peer="x", step_id="X:1", finished=120.0,
                  status="completed", correct=False, actual=[999.0]),
            training(120.1, "X:1", finished=120.0, ready=125.0),
        ])
        row = election("g1", 104.0, leader="H0", roster=["x", "h0"],
                       event_number=401)
        result = self.reconstruct(events, [row])

        self.assertAlmostEqual(state_seconds(result, "following"), 17.0)
        self.assertEqual(result["searches"][0]["outcome"], "followed")
        self.assertEqual(result["searches"][0]["followed_group_ids"], ["g1"])
        self.assertEqual(result["other_groups"][0]["group_id"], "g1")

    def test_disband_and_waiting_exception_resume_an_open_search(self):
        cases = (
            event("native_client_reply", 105.0, peer="x", other="h0",
                  step_id="X:1", code="GROUP_DISBANDED", phase="waiting_for_group",
                  group_id="g1"),
            event("native_client_exception", 105.0, peer="x", other="h0",
                  step_id="X:1", phase="waiting_for_group", group_id="g1"),
        )
        for reset in cases:
            with self.subTest(reset=reset["kind"]):
                events = base_step()
                events.extend([
                    event("native_client_reply", 103.0, peer="x", other="h0",
                          step_id="X:1", code="ACCEPTED", phase="handshake", group_id="g1"),
                    reset,
                    event("search_end", 110.0, peer="x", step_id="X:1"),
                    event("step_result", 112.0, peer="x", step_id="X:1",
                          finished=112.0, status="no_group"),
                    training(112.1, "X:1", finished=112.0, ready=120.0),
                ])
                result = self.reconstruct(events)

                self.assertGreaterEqual(state_seconds(result, "searching"), 7.0)
                self.assertEqual(result["searches"][0]["outcome"], "no_group")

    def test_retries_with_same_step_id_get_distinct_search_ordinals(self):
        events = base_step()
        events.extend([
            event("search_end", 108.0, peer="x", step_id="X:1"),
            event("search_start", 109.0, peer="x", step_id="X:1",
                  group_key="retry-key", deadline=117.0),
            event("search_end", 117.0, peer="x", step_id="X:1"),
            event("step_result", 118.0, peer="x", step_id="X:1",
                  finished=118.0, status="no_group"),
            training(118.1, "X:1", finished=118.0, ready=125.0),
        ])
        result = self.reconstruct(events)

        self.assertEqual(len(result["searches"]), 2)
        first, second = result["searches"]
        self.assertEqual((first["ordinal"], second["ordinal"]), (1, 2))
        self.assertEqual((first["step_id"], second["step_id"]), ("X:1", "X:1"))
        self.assertNotEqual(first["search_id"], second["search_id"])

    def test_no_group_search_end_has_transition_until_step_finish(self):
        events = base_step()
        events.extend([
            event("search_end", 110.0, peer="x", step_id="X:1"),
            event("step_result", 115.0, peer="x", step_id="X:1",
                  finished=115.0, status="no_group"),
            training(115.1, "X:1", finished=115.0, ready=125.0),
        ])
        result = self.reconstruct(events)

        transition = [
            row for row in intervals_for(result, "transition")
            if row["start"] < 115.0 and row["end"] > 110.0
        ]
        self.assertTrue(transition)
        self.assertEqual(result["searches"][0]["outcome"], "no_group")

    def test_missing_and_overlapping_search_edges_are_rejected(self):
        missing = [event("search_end", 105.0, peer="x", step_id="X:1")]
        with self.assertRaises(ValueError):
            self.reconstruct(missing)

        overlapping = base_step()
        overlapping.append(event("search_start", 102.0, peer="x", step_id="X:1",
                                 group_key="second", deadline=109.0))
        with self.assertRaises(ValueError):
            self.reconstruct(overlapping)

    def test_trial_coverage_is_clipped_and_drain_keeps_late_assembly_separate(self):
        events = base_step(start=150.0)
        events.extend([
            event("search_end", 165.0, peer="x", step_id="X:1"),
            event("step_result", 170.0, peer="x", step_id="X:1", finished=170.0,
                  status="no_group"),
            training(170.1, "X:1", finished=170.0, ready=180.0),
            assembly(175.0, "late", peer="h0", step_id="H0:4", roster=["h0", "h1"],
                     event_number=751),
            event("step_result", 180.0, peer="h0", step_id="H0:4", finished=180.0,
                  status="completed", event_number=752),
        ])
        late = election("late", 175.0, leader="H0", step_id="H0:4",
                        expiration=1_260.0, x_expiration=1_280.0, event_number=751)
        result = self.reconstruct(events, [late])

        self.assertAlmostEqual(sum(result["summary"]["submission_state_seconds"].values()), 60.0)
        self.assertAlmostEqual(sum(result["summary"]["drain_state_seconds"].values()), 20.0)
        self.assertEqual(result["other_groups"][0]["period"], "drain")
        self.assertEqual(result["other_groups"][0]["x_state"], "between_rounds")

    def test_absent_x_expiration_stays_null(self):
        events = base_step()
        events.extend([
            assembly(105.0, "g-no-x-exp", peer="h0", step_id="H0:1",
                     roster=["h0", "h1"], event_number=501),
            event("search_end", 106.0, peer="x", step_id="X:1"),
            event("step_result", 110.0, peer="x", step_id="X:1", finished=110.0,
                  status="no_group"),
            training(110.1, "X:1", finished=110.0, ready=120.0),
        ])
        row = election("g-no-x-exp", 105.0, leader="H0", step_id="H0:1",
                       x_expiration=None, active=False, event_number=501)
        result = self.reconstruct(events, [row])

        self.assertIsNone(result["other_groups"][0]["x_expiration"])
        self.assertIsNone(result["other_groups"][0]["x_remaining_seconds"])

    def test_other_group_phase_is_classified_at_assembly_time(self):
        events = [
            *base_step(start=101.0),
            assembly(103.0, "x-leads", peer="x", step_id="X:1", event_number=301),
            assembly(110.0, "during-lead", peer="h0", step_id="H0:1",
                     roster=["h0", "h1"], event_number=302),
            event("step_result", 120.0, peer="x", step_id="X:1", finished=120.0,
                  status="completed"),
            event("search_end", 119.0, peer="x", step_id="X:1"),
            event("training_scheduled", 120.1, peer="x", step_id="X:1",
                  finished=120.0, delay=10.0, ready=130.0),
            assembly(125.0, "during-gap", peer="h0", step_id="H0:2",
                     roster=["h0", "h1"], event_number=303),
            event("step_start", 130.1, peer="x", name="X", sequence=2,
                  step_id="X:2"),
            event("search_start", 131.0, peer="x", step_id="X:2",
                  group_key="x-key-2", deadline=140.0),
            assembly(135.0, "during-search", peer="h0", step_id="H0:3",
                     roster=["h0", "h1"], event_number=304),
            event("search_end", 140.0, peer="x", step_id="X:2"),
            event("step_result", 141.0, peer="x", step_id="X:2", finished=141.0,
                  status="no_group"),
            training(141.1, "X:2", finished=141.0, ready=151.0),
        ]
        rows = [
            election("x-leads", 103.0, leader="X", leader_peer="x", step_id="X:1",
                     roster=["x", "h0"], event_number=301),
            election("during-lead", 110.0, step_id="H0:1", event_number=302),
            election("during-gap", 125.0, step_id="H0:2", event_number=303),
            election("during-search", 135.0, step_id="H0:3", event_number=304),
        ]
        result = self.reconstruct(events, rows)
        by_group = {row["group_id"]: row for row in result["other_groups"]}

        self.assertEqual(by_group["during-lead"]["x_state"], "leading")
        self.assertEqual(by_group["during-gap"]["x_state"], "between_rounds")
        self.assertEqual(by_group["during-search"]["x_state"], "searching")
        self.assertEqual(by_group["during-search"]["period"], "submission")

    def test_cross_process_event_order_does_not_change_reconstruction(self):
        events = base_step()
        events.extend([
            assembly(102.0, "external", peer="h0", step_id="H0:1",
                     roster=["h0", "x"], event_number=201),
            assembly(102.5, "external-open", peer="h0", step_id="H0:2",
                     roster=["h0", "h1"], event_number=251),
            assembly(103.0, "x-led", peer="x", step_id="X:1", event_number=301),
            event("search_end", 110.0, peer="x", step_id="X:1"),
            event("step_result", 120.0, peer="x", step_id="X:1", finished=120.0,
                  status="completed"),
            training(120.1, "X:1", finished=120.0, ready=130.0),
        ])
        rows = [
            election("external", 102.0, leader="H0", step_id="H0:1",
                     roster=["h0", "x"], event_number=201),
            election("external-open", 102.5, leader="H0", step_id="H0:2",
                     roster=["h0", "h1"], event_number=251),
            election("x-led", 103.0, leader="X", leader_peer="x", step_id="X:1",
                     roster=["x", "h0"], event_number=301),
        ]
        forward = self.reconstruct(events, rows)
        shuffled = self.reconstruct(list(reversed(events)), rows)

        def state_signature(result):
            return [(r["state"], r["start"], r["end"], r["group_id"], r["search_id"])
                    for r in result["timeline"]]

        def search_signature(result):
            return [(r["search_id"], r["outcome"], r["led_group_ids"], r["followed_group_ids"])
                    for r in result["searches"]]

        self.assertEqual(state_signature(forward), state_signature(shuffled))
        self.assertEqual(search_signature(forward), search_signature(shuffled))
        self.assertEqual([r["x_state"] for r in forward["other_groups"]],
                         [r["x_state"] for r in shuffled["other_groups"]])
        member_group, open_group = forward["other_groups"]
        self.assertEqual(member_group["x_state"], "searching")
        self.assertTrue(member_group["x_is_member_of_other_group"])
        self.assertFalse(member_group["potential_deadline_competition"])
        self.assertEqual(open_group["x_state"], "searching")
        self.assertFalse(open_group["x_is_member_of_other_group"])
        self.assertTrue(open_group["potential_deadline_competition"])

    def test_raw_export_mismatch_and_missing_training_schedule_are_rejected(self):
        raw_group = [assembly(103.0, "unexported", peer="h0", step_id="H0:1",
                              roster=["h0", "x"], event_number=301)]
        with self.assertRaisesRegex(ValueError, "election export and raw assemblies differ"):
            self.reconstruct(raw_group)

        missing_schedule = base_step()
        missing_schedule.extend([
            event("search_end", 110.0, peer="x", step_id="X:1"),
            event("step_result", 115.0, peer="x", step_id="X:1", finished=115.0,
                  status="no_group"),
        ])
        with self.assertRaisesRegex(ValueError, "step result lacks training schedule"):
            self.reconstruct(missing_schedule)

    def test_output_path_safety_checks_precede_suite_validation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            suite = root / "invalid-suite"
            suite.mkdir()

            existing = root / "already-created"
            existing.mkdir()
            with self.assertRaisesRegex(ValueError, "output already exists"):
                analyze_to_output(suite, existing)

            nested = suite / "nested-output"
            with self.assertRaisesRegex(ValueError, "output must be outside input suite"):
                analyze_to_output(suite, nested)
            self.assertFalse(nested.exists())

    def test_between_rounds_splits_at_planned_ready_time(self):
        events = [
            *base_step(start=101.0),
            event("search_end", 105.0, peer="x", step_id="X:1"),
            event("step_result", 110.0, peer="x", step_id="X:1", finished=110.0,
                  status="no_group"),
            training(110.1, "X:1", finished=110.0, ready=120.0),
            event("step_start", 130.1, peer="x", name="X", sequence=2,
                  step_id="X:2"),
            event("search_start", 131.0, peer="x", step_id="X:2",
                  group_key="x-key-2", deadline=139.0),
            event("search_end", 135.0, peer="x", step_id="X:2"),
            event("step_result", 140.0, peer="x", step_id="X:2", finished=140.0,
                  status="no_group"),
            training(140.1, "X:2", finished=140.0, ready=150.0),
        ]
        result = self.reconstruct(events)

        self.assertAlmostEqual(result["summary"]["submission_between_seconds"]["training_delay"],
                               20.0)
        self.assertAlmostEqual(result["summary"]["submission_between_seconds"]["ready_wait"],
                               20.1)

    def test_group_completed_in_drain_is_credited_to_submitted_search(self):
        events = [
            event("step_start", 150.0, peer="x", name="X", sequence=1, step_id="X:1"),
            event("search_start", 151.0, peer="x", step_id="X:1",
                  group_key="x-key", deadline=159.0),
            assembly(162.0, "drain-lead", peer="x", step_id="X:1", event_number=6201),
            event("search_end", 163.0, peer="x", step_id="X:1"),
            event("step_result", 164.0, peer="x", step_id="X:1", finished=164.0,
                  status="completed"),
            training(164.1, "X:1", finished=164.0, ready=165.0),
            event("step_start", 166.0, peer="x", name="X", sequence=2, step_id="X:2"),
            event("search_start", 167.0, peer="x", step_id="X:2",
                  group_key="retry-key", deadline=175.0),
            event("search_end", 170.0, peer="x", step_id="X:2"),
            event("step_result", 175.0, peer="x", step_id="X:2", finished=175.0,
                  status="no_group"),
            training(175.1, "X:2", finished=175.0, ready=180.0),
        ]
        row = election("drain-lead", 162.0, leader="X", leader_peer="x", step_id="X:1",
                       roster=["x", "h0"], event_number=6201)
        result = self.reconstruct(events, [row])
        led_search, drain_search = result["searches"]

        self.assertTrue(led_search["started_in_submission"])
        self.assertTrue(led_search["completed_in_drain"])
        self.assertEqual(led_search["outcome"], "led")
        self.assertFalse(drain_search["started_in_submission"])
        self.assertEqual(drain_search["outcome"], "no_group")
        self.assertEqual(result["summary"]["searches_led"], 1)
        self.assertEqual(result["summary"]["submission_searches_started"], 1)
        self.assertEqual(result["summary"]["submission_searches_led"], 1)
        self.assertEqual(result["summary"]["drain_searches_started"], 1)
        self.assertEqual(result["summary"]["drain_searches_no_group"], 1)

    def test_retry_started_in_drain_is_counted_in_drain_search_totals(self):
        events = [
            event("step_start", 150.0, peer="x", name="X", sequence=1, step_id="X:1"),
            event("search_start", 159.0, peer="x", step_id="X:1",
                  group_key="first-key", deadline=167.0),
            event("search_end", 160.5, peer="x", step_id="X:1"),
            event("search_start", 161.0, peer="x", step_id="X:1",
                  group_key="retry-key", deadline=169.0),
            event("search_end", 163.0, peer="x", step_id="X:1"),
            event("step_result", 164.0, peer="x", step_id="X:1", finished=164.0,
                  status="no_group"),
            training(164.1, "X:1", finished=164.0, ready=170.0),
        ]
        result = self.reconstruct(events)

        self.assertEqual(result["summary"]["searches_started"], 2)
        self.assertEqual(result["summary"]["retry_searches"], 1)
        self.assertEqual(result["summary"]["submission_searches_started"], 1)
        self.assertEqual(result["summary"]["submission_searches_no_group"], 1)
        self.assertEqual(result["summary"]["drain_searches_started"], 1)
        self.assertEqual(result["summary"]["drain_searches_no_group"], 1)


if __name__ == "__main__":
    unittest.main()
