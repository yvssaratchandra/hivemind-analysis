"""Focused checks for reconstructing Step 24 group winner declarations."""

import json
from pathlib import Path
import tempfile
import unittest

from export_step24_elections import export_suite


IDS = {"X": "peer-x", "H0": "peer-h0", "H1": "peer-h1"}


def make_event(kind, peer, pid, event_number, wall_time, *, other="", **detail):
    return {
        "kind": kind,
        "peer": peer,
        "other": other,
        "pid": pid,
        "event_number": event_number,
        "monotonic": 10.0 + event_number,
        "wall_time": wall_time,
        "detail": detail,
    }


def trial_events(group_id="g1", leader="peer-x", assembly_number=5,
                 assembly_wall=120.0, assembly_monotonic=None, roster=None):
    if roster is None:
        roster = ["peer-x", "peer-h0"]
    if assembly_monotonic is None:
        assembly_monotonic = 10.0 + assembly_number
    leader_name = next(name for name, peer in IDS.items() if peer == leader)
    declarations = [
        make_event("ad_started", leader, 10, 1, 1.0, other="group-key",
                   active=True, expiration=100.0),
        make_event("ad_finished", leader, 10, 2, 2.0, other="group-key",
                   active=True, accepted=True, expiration=100.0),
        make_event("ad_started", leader, 10, 3, 3.0, other="group-key",
                   active=True, expiration=110.0),
        make_event("ad_finished", leader, 10, 4, 4.0, other="group-key",
                   active=True, accepted=False, expiration=110.0),
        make_event("assembled", leader, 10, assembly_number, assembly_wall,
                   step_id=f"{leader_name}:1", group_id=group_id, roster=roster),
        # Its wall time is earlier, but its process sequence is after assembly.
        make_event("ad_started", leader, 10, assembly_number + 1, 5.0,
                   other="group-key", active=True, expiration=999.0),
    ]
    interleaved = make_event("step_start", "peer-h0", 20, 1, 2.5,
                             name="H0", sequence=1, step_id="H0:1")
    # Deliberately scramble queue order and include a second process. Rebuild
    # by PID/event_number, not file order, wall time, or monotonic time.
    return [declarations[3], interleaved, declarations[5], declarations[1],
            declarations[4], declarations[0], declarations[2]]


def x_events(*, expiration=120.0, group_key="group-key", active=True,
             renewal_after_assembly=False, assembly_monotonic=15.0):
    rows = [
        make_event("step_start", "peer-x", 30, 1, 2.0,
                   name="X", sequence=1, step_id="X:1"),
        make_event("step_start", "peer-x", 30, 2, 3.0,
                   name="X", sequence=2, step_id="X:2"),
        make_event("ad_started", "peer-x", 30, 3, 4.0, other=group_key,
                   active=active, expiration=expiration),
    ]
    if renewal_after_assembly:
        rows.append(make_event("ad_started", "peer-x", 30, 4, 5.0,
                               other=group_key, active=True, expiration=300.0))
        rows[-1]["monotonic"] = assembly_monotonic + 0.01
    return rows


def follower_join_events(*, group_id="g1", include_view=True, include_x=True):
    events = []
    if include_view:
        entries = [{"peer": "peer-h0", "expiration": 90.0}]
        if include_x:
            entries.append({"peer": "peer-x", "expiration": 100.0})
        view = make_event("ad_view", "peer-h1", 21, 1, 6.0,
                          other="group-key", step_id="H1:1", entries=entries)
        view["monotonic"] = 14.0
        events.append(view)
    wait_number = 2 if include_view else 1
    wait = make_event("native_client_wait", "peer-h1", 21, wait_number, 7.0,
                      other="peer-h0", step_id="H1:1", phase="handshake", timeout=2.0)
    wait["monotonic"] = 14.8
    events.append(wait)
    reply_number = 3 if include_view else 2
    reply = make_event("native_client_reply", "peer-h1", 21, reply_number, 8.0,
                       other="peer-h0", step_id="H1:1", code="BEGIN_ALLREDUCE",
                       group_id=group_id)
    reply["monotonic"] = 15.2
    events.append(reply)
    return events


def write_trial(root: Path, name: str, events, *, case="passive_stock", seed=2400):
    path = root / name
    path.mkdir(parents=True)
    config = {
        "case": {"name": case},
        "seed": seed,
        "peer_ids": IDS,
    }
    (path / "config.json").write_text(json.dumps(config))
    (path / "summary.json").write_text(json.dumps({"outcomes": []}))
    (path / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))
    return path


class Step24ElectionExportTests(unittest.TestCase):
    def export_events(self, events):
        with tempfile.TemporaryDirectory() as folder:
            suite = Path(folder)
            write_trial(suite, "trial_s2400", events)
            return export_suite(suite)

    def test_uses_process_order_latest_prior_renewal_and_keeps_expired_deadline(self):
        row, = self.export_events(trial_events())
        self.assertEqual(row["leader_name"], "X")
        self.assertEqual(row["absolute_declared_expiration"], 110.0)
        self.assertEqual(row["remaining_seconds"], -10.0)
        self.assertEqual(row["declaration_event_ref"], "10:3")
        self.assertEqual(row["assembly_event_ref"], "10:5")
        self.assertEqual(row["last_active_ad_finished_expiration"], 100.0)
        self.assertIs(row["last_active_ad_finished_accepted"], True)
        self.assertEqual(row["last_active_ad_finished_event_ref"], "10:2")
        self.assertEqual(row["x_expiration"], 110.0)
        self.assertEqual(row["expiration_comparison"], "x_is_leader")

    def test_latest_inactive_declaration_rejects_the_assembly(self):
        events = trial_events()
        for event in events:
            if event["kind"] == "ad_started" and event["event_number"] == 3:
                event["detail"]["active"] = False
        with self.assertRaisesRegex(ValueError, "latest publisher declaration.*inactive"):
            self.export_events(events)

    def test_missing_prior_declaration_rejects_the_assembly(self):
        events = trial_events()
        for event in events:
            if event["kind"] == "ad_started" and event["event_number"] in (1, 3):
                event["kind"] = "other_event"
        with self.assertRaisesRegex(ValueError, "no prior publisher declaration"):
            self.export_events(events)

    def test_conflicting_group_id_leaders_or_rosters_reject_suite(self):
        with tempfile.TemporaryDirectory() as folder:
            suite = Path(folder)
            first = trial_events(group_id="collision")
            second = trial_events(group_id="collision", leader="peer-h0",
                                  roster=["peer-h0", "peer-h1"])
            # Give the second winner its own process-local declaration history.
            second = [event for event in second if event["pid"] != 10]
            second.extend([
                make_event("ad_started", "peer-h0", 11, 1, 1.0, other="other-key",
                           active=True, expiration=100.0),
                make_event("assembled", "peer-h0", 11, 2, 120.0,
                           step_id="H0:1", group_id="collision",
                           roster=["peer-h0", "peer-h1"]),
            ])
            write_trial(suite, "a", first, seed=2400)
            write_trial(suite, "b", second, seed=2401)
            with self.assertRaisesRegex(ValueError, "conflicting group_id collision"):
                export_suite(suite)

    def test_same_group_id_and_identity_is_deduplicated(self):
        with tempfile.TemporaryDirectory() as folder:
            suite = Path(folder)
            write_trial(suite, "a", trial_events(group_id="same"), seed=2400)
            write_trial(suite, "b", trial_events(group_id="same"), seed=2401)
            rows = export_suite(suite)
        self.assertEqual(len(rows), 1)

    def export_with_x_events(self, events):
        with tempfile.TemporaryDirectory() as folder:
            suite = Path(folder)
            write_trial(suite, "trial_s2400", events)
            return export_suite(suite)

    def test_x_state_uses_monotonic_time_across_interleaved_processes(self):
        events = trial_events(leader="peer-h0", assembly_monotonic=15.0)
        other_process = x_events(expiration=120.0, renewal_after_assembly=True)
        # Put X's later renewal in the file before the assembly row. Its local
        # monotonic time is still after assembly and must therefore be ignored.
        events.insert(4, other_process[-1])
        events.extend(other_process[:-1])
        row, = self.export_with_x_events(events)
        self.assertEqual(row["leader_name"], "H0")
        self.assertEqual(row["leader_expiration"], 110.0)
        self.assertEqual(row["leader_remaining_seconds"], -10.0)
        self.assertEqual(row["x_expiration"], 120.0)
        self.assertEqual(row["x_remaining_seconds"], 0.0)
        self.assertEqual(row["expiration_gap_x_minus_leader"], 10.0)
        self.assertEqual(row["expiration_comparison"], "leader_earlier")
        self.assertEqual(row["x_declaration_event_ref"], "30:3")
        self.assertEqual(row["x_current_step_id"], "X:2")
        self.assertTrue(row["x_in_roster"])
        self.assertTrue(row["leader_x_same_group_key"])

    def test_x_earlier_expiration_produces_negative_gap(self):
        events = trial_events(leader="peer-h0", assembly_monotonic=15.0)
        events.extend(x_events(expiration=100.0))
        row, = self.export_with_x_events(events)
        self.assertEqual(row["x_remaining_seconds"], -20.0)
        self.assertEqual(row["expiration_gap_x_minus_leader"], -10.0)
        self.assertEqual(row["expiration_comparison"], "x_earlier")

    def test_inactive_x_has_no_current_expiration_or_gap(self):
        events = trial_events(leader="peer-h0", assembly_monotonic=15.0)
        events.extend(x_events(expiration=120.0, active=False))
        row, = self.export_with_x_events(events)
        self.assertFalse(row["x_ad_active"])
        self.assertEqual(row["x_inactive_reason"], "inactive_declaration")
        self.assertIsNone(row["x_expiration"])
        self.assertIsNone(row["x_remaining_seconds"])
        self.assertIsNone(row["expiration_gap_x_minus_leader"])
        self.assertEqual(row["x_last_ad_started_expiration"], 120.0)
        self.assertEqual(row["expiration_comparison"], "x_not_advertising")

    def test_no_prior_x_declaration_is_reported_as_never_declared(self):
        row, = self.export_with_x_events(trial_events(leader="peer-h0"))
        self.assertFalse(row["x_ad_active"])
        self.assertEqual(row["x_inactive_reason"], "never_declared")
        self.assertIsNone(row["x_expiration"])
        self.assertIsNone(row["x_declaration_event_ref"])

    def test_rejected_active_ad_finished_is_not_reported_as_successful_store(self):
        row, = self.export_events(trial_events())
        self.assertEqual(row["last_active_ad_finished_accepted"], True)
        self.assertEqual(row["last_active_ad_finished_expiration"], 100.0)
        self.assertNotEqual(row["last_active_ad_finished_event_ref"], "10:4")

    def test_follower_view_precedes_handshake_and_excludes_later_renewals(self):
        events = trial_events(leader="peer-h0", assembly_monotonic=15.0,
                              roster=["peer-x", "peer-h0", "peer-h1"])
        x_history = x_events(expiration=100.0, assembly_monotonic=15.0)
        x_history.extend([
            make_event("ad_started", "peer-x", 30, 4, 9.0, other="group-key",
                       active=True, expiration=200.0),
            make_event("ad_started", "peer-x", 30, 5, 10.0, other="group-key",
                       active=True, expiration=300.0),
        ])
        x_history[-2]["monotonic"] = 14.5
        x_history[-1]["monotonic"] = 14.9
        events.extend(x_history)
        events.extend(follower_join_events())
        row, = self.export_with_x_events(events)
        self.assertEqual(row["x_expiration"], 300.0)
        view, = row["follower_views"]
        self.assertEqual(view["status"], "latest_logged_view_before_handshake")
        self.assertEqual(view["handshake_event_ref"], "21:2")
        self.assertEqual(view["view_event_ref"], "21:1")
        self.assertEqual(view["observed_leader_expiration"], 90.0)
        self.assertEqual(view["observed_X_expiration"], 100.0)
        self.assertTrue(view["x_visible"])
        self.assertEqual(view["deadline_gap_in_view"], 10.0)

    def test_follower_view_marks_x_invisible_without_inventing_deadline(self):
        events = trial_events(leader="peer-h0", assembly_monotonic=15.0,
                              roster=["peer-x", "peer-h0", "peer-h1"])
        events.extend(x_events(expiration=120.0, assembly_monotonic=15.0))
        events.extend(follower_join_events(include_x=False))
        row, = self.export_with_x_events(events)
        view, = row["follower_views"]
        self.assertTrue(row["x_ad_active"])
        self.assertEqual(row["x_expiration"], 120.0)
        self.assertFalse(view["x_visible"])
        self.assertIsNone(view["observed_X_expiration"])
        self.assertIsNone(view["deadline_gap_in_view"])

    def test_matching_reply_without_handshake_is_unavailable(self):
        events = trial_events(leader="peer-h0", assembly_monotonic=15.0,
                              roster=["peer-x", "peer-h0", "peer-h1"])
        reply = make_event("native_client_reply", "peer-h1", 21, 1, 8.0,
                           other="peer-h0", step_id="H1:1", code="BEGIN_ALLREDUCE",
                           group_id="g1")
        reply["monotonic"] = 15.2
        events.append(reply)
        row, = self.export_with_x_events(events)
        view, = row["follower_views"]
        self.assertEqual(view["status"], "no_matching_handshake")
        self.assertIsNone(view["view_event_ref"])
        self.assertIsNone(view["x_visible"])

    def test_matching_handshake_without_prior_view_is_unavailable(self):
        events = trial_events(leader="peer-h0", assembly_monotonic=15.0,
                              roster=["peer-x", "peer-h0", "peer-h1"])
        events.extend(follower_join_events(include_view=False))
        row, = self.export_with_x_events(events)
        view, = row["follower_views"]
        self.assertEqual(view["status"], "no_prior_logged_view")
        self.assertEqual(view["handshake_event_ref"], "21:1")
        self.assertIsNone(view["view_event_ref"])
        self.assertIsNone(view["x_visible"])


if __name__ == "__main__":
    unittest.main()
