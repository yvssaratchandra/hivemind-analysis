"""Synthetic, offline checks for Step 25 choice reconstruction."""

from __future__ import annotations

import hashlib
import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import analyze_step25_choices as choices


START = 100.0
DURATION = 20.0
IDS = {"X": "a", "H0": "z", "H1": "b", "H2": "c",
       "H3": "d", "H4": "e", "H5": "f", "H6": "g"}
PEER_BYTES = {"a": "ff", "z": "01", "b": "20", "c": "30",
              "d": "40", "e": "50", "f": "60", "g": "70"}


def trial_config(*, case="passive_stock", seed=2501, duration=DURATION,
                 timeout=3.0, peers=None):
    return {
        "case": {"name": case, "mode": "refresh"},
        "seed": seed,
        "duration": duration,
        "timeout": timeout,
        "outcome": "grouping",
        "instrumentation": "detailed",
        "started_monotonic": START,
        "peer_ids": dict(peers or IDS),
    }


class Events:
    """Build event records with the same outer shape as the native harness."""

    def __init__(self):
        self.rows = []
        self.number = 0

    def add(self, kind, at, *, peer="a", other="", pid=1, **detail):
        self.number += 1
        row = {
            "kind": kind,
            "peer": peer,
            "other": other,
            "pid": pid,
            "event_number": self.number,
            "monotonic": float(at),
            "wall_time": 1_000.0 + float(at - START),
            "detail": detail,
        }
        self.rows.append(row)
        return row


def span(events, *, peer, step_id, search_id, start, end, group_key="g"):
    events.add("search_span_start", start, peer=peer, step_id=step_id,
               search_id=search_id, group_key=group_key)
    events.add("read_enabled", start + 0.01, peer=peer, step_id=step_id,
               search_id=search_id, group_key=group_key)
    events.add("read_disabled", end - 0.01, peer=peer, step_id=step_id,
               search_id=search_id, group_key=group_key)
    events.add("search_span_end", end, peer=peer, step_id=step_id,
               search_id=search_id, group_key=group_key)


def ad_read(events, *, peer, step_id, search_id, read_id, start, end, entries,
            error=None, cancelled=False):
    context = dict(step_id=step_id, search_id=search_id, group_key="g", read_id=read_id)
    events.add("ad_read_start", start, peer=peer, **context)
    events.add("ad_read_end", end, peer=peer, **context,
               latency=end-start, entries=list(entries), error=error,
               cancelled=cancelled)


def pop_selection(events, *, peer, step_id, search_id, at, selected,
                  queue, own_expiration, read_id, past_attempts=(),
                  peer_bytes=None, selected_bytes=None,
                  selected_expiration=None, top_wall=None, group_key="g"):
    if peer_bytes is None:
        peer_bytes = PEER_BYTES[peer]
    if selected_bytes is None:
        selected_bytes = PEER_BYTES[selected]
    if selected_expiration is None:
        selected_expiration = next(row["expiration"] for row in queue
                                   if row["peer"] == selected)
    if top_wall is None:
        top_wall = 1_000.0 + at - START
    return events.add(
        "native_pop_selected", at, peer=peer, other=selected,
        step_id=step_id, search_id=search_id, group_key=group_key,
        selected_expiration=selected_expiration,
        own_expiration=own_expiration,
        own_peer_bytes_hex=peer_bytes,
        selected_peer_bytes_hex=selected_bytes,
        selection_top_wall=top_wall,
        read_id=read_id,
        queue=[dict(row) for row in queue],
        past_attempts=[dict(row) for row in past_attempts],
    )


def candidate(peer, expiration, peer_bytes=None):
    return {"peer": peer, "expiration": expiration,
            "peer_bytes_hex": peer_bytes or PEER_BYTES[peer]}


def x_step(events, *, step_id, start, status, assembled=(), result_at=None):
    sequence = int(step_id.split(":")[1])
    events.add("step_start", start, peer="a", name="X", sequence=sequence,
               step_id=step_id, value=10.0+sequence)
    for index, (at, group_id, roster, leader) in enumerate(assembled, 1):
        events.add("assembled", at, peer=leader, step_id=step_id,
                   group_id=group_id, roster=list(roster),
                   event_number=index)
    if result_at is None:
        result_at = max([start + 0.1] + [row[0] for row in assembled])
    events.add("step_result", result_at, peer="a", name="X", sequence=sequence,
               step_id=step_id, finished=result_at, status=status)


class Step25ChoiceReconstructionTests(unittest.TestCase):
    def reconstruct(self, events, *, config=None):
        if not any(row["kind"] == "trace_closed" for row in events.rows):
            end = max((row["monotonic"] for row in events.rows), default=START)
            events.add("trace_closed", end + 0.1, peer="", participants=list(IDS),
                       pending=[], error=None)
        return choices.reconstruct_trial(config or trial_config(), events.rows)

    def test_retries_within_one_step_count_once_and_use_first_assembly_latency(self):
        events = Events()
        x_step(events, step_id="X:1", start=101.0, status="completed",
               assembled=[(106.0, "gx1", ["a", "z"], "a")], result_at=107.0)
        # A single X step can make multiple native follow attempts. The failed retry
        # does not create another experimental attempt or move its latency origin.
        sid, search1, search2 = "X:1", "a:X:1:search1", "a:X:1:search2"
        events.add("search_span_start", 101.1, peer="a", step_id=sid,
                   search_id=search1, group_key="g")
        events.add("read_enabled", 101.11, peer="a", step_id=sid,
                   search_id=search1, group_key="g")
        entries = [dict(peer="z", expiration=1_110.0)]
        ad_read(events, peer="a", step_id=sid, search_id=search1, read_id="r1",
                start=101.2, end=101.3, entries=entries)
        first = pop_selection(events, peer="a", step_id=sid, search_id=search1,
                              at=101.4, selected="z", queue=entries,
                              own_expiration=1_120.0, read_id="r1")
        events.add("join_attempt_start", 101.41, peer="a", other="z", step_id=sid,
                   search_id=search1, join_id="j1", origin="queue",
                   selection_event_ref=f"1:{first['event_number']}")
        events.add("join_attempt_end", 102.0, peer="a", other="z", step_id=sid,
                   search_id=search1, join_id="j1", origin="queue",
                   selection_event_ref=f"1:{first['event_number']}", error="TimeoutError")
        events.add("read_disabled", 102.1, peer="a", step_id=sid,
                   search_id=search1, group_key="g")
        events.add("search_span_end", 102.2, peer="a", step_id=sid,
                   search_id=search1, group_key="g")
        events.add("search_span_start", 102.9, peer="a", step_id=sid,
                   search_id=search2, group_key="g")
        events.add("read_enabled", 102.91, peer="a", step_id=sid,
                   search_id=search2, group_key="g")
        entries2 = [dict(peer="b", expiration=1_111.0)]
        ad_read(events, peer="a", step_id=sid, search_id=search2, read_id="r2",
                start=103.0, end=103.1, entries=entries2)
        second = pop_selection(events, peer="a", step_id=sid, search_id=search2,
                               at=103.2, selected="b", queue=entries2,
                               own_expiration=1_120.0, read_id="r2")
        events.add("join_attempt_start", 103.21, peer="a", other="b", step_id=sid,
                   search_id=search2, join_id="j2", origin="queue",
                   selection_event_ref=f"1:{second['event_number']}")
        events.add("join_attempt_end", 104.0, peer="a", other="b", step_id=sid,
                   search_id=search2, join_id="j2", origin="queue",
                   selection_event_ref=f"1:{second['event_number']}", error=None)
        events.add("read_disabled", 106.1, peer="a", step_id=sid,
                   search_id=search2, group_key="g")
        events.add("search_span_end", 106.2, peer="a", step_id=sid,
                   search_id=search2, group_key="g")

        result = self.reconstruct(events)
        self.assertEqual(len(result["attempts"]), 1)
        attempt = result["attempts"][0]
        self.assertEqual(attempt["step_id"], "X:1")
        self.assertEqual(attempt["search_ids"], [search1, search2])
        self.assertEqual(attempt["assembled_group_ids"], ["gx1"])
        self.assertTrue(attempt["assembled_success"])
        self.assertTrue(attempt["led_success"])
        self.assertAlmostEqual(attempt["assembly_latency_seconds"], 4.9)
        self.assertTrue(attempt["success_by_5s"])
        self.assertEqual(result["metrics"]["x_attempts"]["denominator"], 1)
        self.assertEqual(result["metrics"]["x_attempts"]["success_count"], 1)
        self.assertEqual(result["metrics"]["x_attempts"]["success_by_5s"], 1)
        self.assertAlmostEqual(
            result["metrics"]["x_attempts"]["successful_assembly_latency_seconds"][0], 4.9)
        self.assertEqual(len(result["joins"]), 2)

    def test_no_group_failure_and_cutoff_drain_attempts_stay_in_denominator(self):
        events = Events()
        x_step(events, step_id="X:1", start=101.0, status="no_group", result_at=104.0)
        x_step(events, step_id="X:2", start=104.0, status="failed", result_at=108.0)
        x_step(events, step_id="X:3", start=109.5, status="failed", result_at=111.0)
        x_step(events, step_id="X:4", start=109.9, status="no_group", result_at=111.5)
        result = self.reconstruct(
            events, config=trial_config(duration=10.0, timeout=3.0))
        self.assertEqual(result["metrics"]["x_attempts"]["denominator"], 4)
        attempts = {row["step_id"]: row for row in result["attempts"]}
        self.assertEqual(attempts["X:1"]["eventual_outcome"], "no_group")
        self.assertEqual(attempts["X:2"]["eventual_outcome"], "failed")
        self.assertTrue(attempts["X:1"]["interior_attempt"])
        self.assertTrue(attempts["X:2"]["interior_attempt"])
        self.assertTrue(attempts["X:3"]["submission_started"])
        self.assertEqual(attempts["X:3"]["attempt_phase"], "submission")
        self.assertFalse(attempts["X:3"]["interior_attempt"])
        self.assertTrue(attempts["X:3"]["drain_outcome"])
        self.assertEqual(attempts["X:3"]["step_result_status"], "failed")
        self.assertTrue(attempts["X:4"]["submission_started"])
        self.assertEqual(attempts["X:4"]["attempt_phase"], "submission")
        self.assertFalse(attempts["X:4"]["interior_attempt"])
        self.assertTrue(attempts["X:4"]["drain_outcome"])
        self.assertEqual(attempts["X:4"]["step_result_status"], "no_group")
        # Interior attempts require a full configured timeout of separation from
        # cutoff; the two near-cutoff attempts remain only in the primary estimate.
        self.assertEqual(result["metrics"]["x_attempts"]["interior_denominator"], 2)

    def test_following_stock_group_is_recorded_as_a_group_and_join(self):
        events = Events()
        sid, search = "X:1", "a:X:1:search1"
        events.add("step_start", 101.0, peer="a", name="X", sequence=1,
                   step_id=sid, value=10.0)
        events.add("search_span_start", 101.1, peer="a", step_id=sid,
                   search_id=search, group_key="g")
        events.add("read_enabled", 101.11, peer="a", step_id=sid,
                   search_id=search, group_key="g")
        queue = [candidate("z", 1_110.0)]
        ad_read(events, peer="a", step_id=sid, search_id=search, read_id="x-r1",
                start=101.2, end=101.3,
                entries=[dict(peer="z", expiration=1_110.0)])
        selection = pop_selection(events, peer="a", step_id=sid, search_id=search,
                                  at=101.4, selected="z", queue=queue,
                                  own_expiration=1_120.0, read_id="x-r1")
        selection_ref = f"1:{selection['event_number']}"
        events.add("join_attempt_start", 101.41, peer="a", other="z", step_id=sid,
                   search_id=search, join_id="x-j1", origin="queue",
                   selection_event_ref=selection_ref)
        events.add("native_client_reply", 101.5, peer="a", other="z", step_id=sid,
                   search_id=search, join_id="x-j1", code="ACCEPTED",
                   phase="handshake", group_id="g-stock")
        events.add("native_client_reply", 101.6, peer="a", other="z", step_id=sid,
                   search_id=search, join_id="x-j1", code="BEGIN_ALLREDUCE",
                   phase="waiting_for_group", group_id="g-stock")
        events.add("assembled", 101.6, peer="z", step_id="H0:1", group_id="g-stock",
                   roster=["a", "z"])
        events.add("join_attempt_end", 101.7, peer="a", other="z", step_id=sid,
                   search_id=search, join_id="x-j1", origin="queue",
                   selection_event_ref=selection_ref, error=None)
        events.add("read_disabled", 102.0, peer="a", step_id=sid,
                   search_id=search, group_key="g")
        events.add("search_span_end", 102.1, peer="a", step_id=sid,
                   search_id=search, group_key="g")
        events.add("step_start", 100.9, peer="z", name="H0", sequence=1,
                   step_id="H0:1", value=10.0)
        events.add("step_result", 102.5, peer="a", name="X", sequence=1,
                   step_id=sid, finished=102.5, status="completed")
        events.add("step_result", 103.0, peer="z", name="H0", sequence=1,
                   step_id="H0:1", finished=103.0, status="completed")

        result = self.reconstruct(events)
        self.assertEqual(len(result["groups"]), 1)
        self.assertEqual(result["groups"][0]["group_id"], "g-stock")
        self.assertEqual(result["groups"][0]["leader_peer_id"], "z")
        self.assertEqual(result["groups"][0]["roster"], ["a", "z"])
        self.assertEqual(len(result["joins"]), 1)
        self.assertEqual(result["joins"][0]["origin"], "queue")
        x_attempt = next(row for row in result["attempts"] if row["step_id"] == "X:1")
        self.assertEqual(x_attempt["eventual_outcome"], "followed")

    def test_opportunity_requires_visible_unexpired_better_x_and_counts_once_per_search(self):
        events = Events()
        sid, search, peer = "H0:1", "z:H0:1:search1", "z"
        events.add("step_start", 100.8, peer=peer, name="H0", sequence=1,
                   step_id=sid, value=10.0)
        events.add("search_span_start", 100.9, peer=peer, step_id=sid,
                   search_id=search, group_key="g")
        events.add("read_enabled", 100.91, peer=peer, step_id=sid,
                   search_id=search, group_key="g")

        cases = [
            # X absent from the queue.
            ("absent", [candidate("b", 1_010.0)], "b", 1_020.0, (),
             [{"peer": "b", "expiration": 1_010.0}], None),
            # X is visible in the read but expired before the native queue top;
            # it has already been pruned from the queue snapshot.
            ("expired", [candidate("b", 1_010.0)], "b", 1_020.0, (),
             [{"peer": "a", "expiration": 1_000.0},
              {"peer": "b", "expiration": 1_010.0}], None),
            # X is present but its priority is after H0's own declaration.
            ("worse", [candidate("a", 1_030.0), candidate("b", 1_010.0)],
             "b", 1_020.0, (),
             [{"peer": "a", "expiration": 1_030.0},
              {"peer": "b", "expiration": 1_010.0}], None),
            # The exact prior X tuple is no longer queued after its attempted pop.
            ("past", [candidate("b", 1_012.0)], "b", 1_020.0,
             ({"peer": "a", "expiration": 1_010.0},),
             [{"peer": "a", "expiration": 1_010.0},
              {"peer": "b", "expiration": 1_012.0}], None),
            # X's raw bytes put it after H0 at an equal expiry. A better
            # H1 entry is selected, so this is a valid queue pop without X.
            ("tie_after", [candidate("a", 1_020.0, "ff"),
                           candidate("b", 1_010.0)],
             "b", 1_020.0, (),
             [{"peer": "a", "expiration": 1_020.0},
              {"peer": "b", "expiration": 1_010.0}], None),
            # Queue tie order uses peer text; X raw bytes still beat H0's own
            # priority. H1's raw bytes would win if the heap also used bytes.
            ("tie_before", [candidate("a", 1_020.0, "20"),
                            candidate("b", 1_020.0, "10")],
             "a", 1_020.0, (),
             [{"peer": "a", "expiration": 1_020.0},
              {"peer": "b", "expiration": 1_020.0}], "30"),
            # A new X expiration is eligible later in this same search, but
            # the search contributes only its first eligible opportunity.
            ("repeat", [candidate("a", 1_021.0, "20"),
                         candidate("b", 1_022.0, "10")],
             "a", 1_030.0, ({"peer": "a", "expiration": 1_020.0},),
             [{"peer": "a", "expiration": 1_021.0},
              {"peer": "b", "expiration": 1_022.0}], "30"),
        ]
        for index, (label, queue, selected, own_exp, past, entries, own_bytes) in enumerate(cases):
            read_id = f"r{index}"
            at = 101.0 + index
            ad_read(events, peer=peer, step_id=sid, search_id=search,
                    read_id=read_id, start=at, end=at+0.1, entries=entries)
            pop_selection(events, peer=peer, step_id=sid, search_id=search,
                          at=at+0.2, selected=selected, queue=queue,
                          own_expiration=own_exp, read_id=read_id,
                          past_attempts=past, peer_bytes=own_bytes,
                          selected_bytes=next(row["peer_bytes_hex"] for row in queue
                                              if row["peer"] == selected),
                          top_wall=1_000.0+at+0.2-START)

        events.add("read_disabled", 108.0, peer=peer, step_id=sid,
                   search_id=search, group_key="g")
        events.add("search_span_end", 108.1, peer=peer, step_id=sid,
                   search_id=search, group_key="g")
        events.add("step_result", 109.0, peer=peer, name="H0", sequence=1,
                   step_id=sid, finished=109.0, status="no_group")

        result = self.reconstruct(events)
        selections = result["selections"]
        self.assertEqual(len(selections), len(cases))
        self.assertEqual([row["x_eligible"] for row in selections],
                         [False, False, False, False, False, True, True])
        self.assertEqual([row["first_eligible"] for row in selections],
                         [False, False, False, False, False, True, False])
        reads = {row["read_id"]: row for row in result["reads"]}
        self.assertFalse(reads["r1"]["x_visible_at_completion"])
        self.assertTrue(selections[3]["read_x_visible_at_completion"])
        self.assertFalse(selections[3]["x_in_queue"])
        self.assertTrue(selections[6]["x_not_past_attempt"])
        self.assertEqual(result["metrics"]["choice_opportunity"]["eligible_first"], 1)
        self.assertEqual(result["metrics"]["choice_opportunity"]["chose_X"], 1)

    def test_redirect_attempt_is_not_attributed_to_a_native_selection(self):
        events = Events()
        sid, search = "H0:1", "z:H0:1:search1"
        events.add("step_start", 101.0, peer="z", name="H0", sequence=1,
                   step_id=sid, value=10.0)
        events.add("search_span_start", 101.1, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        events.add("read_enabled", 101.11, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        queue = [candidate("b", 1_110.0)]
        ad_read(events, peer="z", step_id=sid, search_id=search, read_id="r1",
                start=101.2, end=101.3,
                entries=[{"peer": "b", "expiration": 1_110.0}])
        selected = pop_selection(events, peer="z", step_id=sid, search_id=search,
                                 at=101.4, selected="b", queue=queue,
                                 own_expiration=1_120.0, read_id="r1")
        events.add("join_attempt_start", 101.5, peer="z", other="a", step_id=sid,
                   search_id=search, join_id="j1", origin="redirect",
                   selection_event_ref=None)
        events.add("join_attempt_end", 101.6, peer="z", other="a", step_id=sid,
                   search_id=search, join_id="j1", origin="redirect",
                   selection_event_ref=None, error=None)
        events.add("read_disabled", 102.0, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        events.add("search_span_end", 102.1, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        events.add("step_result", 103.0, peer="z", name="H0", sequence=1,
                   step_id=sid, finished=103.0, status="no_group")

        result = self.reconstruct(events)
        self.assertEqual(len(result["joins"]), 1)
        self.assertEqual(result["joins"][0]["origin"], "redirect")
        self.assertIsNone(result["joins"][0]["selection_event_ref"])
        self.assertEqual(result["metrics"]["choice_opportunity"]["eligible_first"], 0)
        self.assertEqual(result["metrics"]["choice_opportunity"]["chose_X"], 0)

    def test_pause_clips_enabled_time_and_pairs_cross_pause_and_cancelled_reads(self):
        events = Events()
        sid, search = "H0:1", "z:H0:1:search1"
        events.add("step_start", 109.2, peer="z", name="H0", sequence=1,
                   step_id=sid, value=10.0)
        events.add("search_span_start", 109.4, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        events.add("read_enabled", 109.5, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        context = dict(step_id=sid, search_id=search, group_key="g")
        entries = [{"peer": "a", "expiration": 1_120.0}]
        events.add("ad_read_start", 109.8, peer="z", **context, read_id="before")
        events.add("ad_read_end", 109.95, peer="z", **context, read_id="before",
                   latency=0.15, entries=entries, error=None, cancelled=False)
        events.add("ad_read_start", 109.96, peer="z", **context, read_id="cutoff")
        events.add("ad_read_end", 110.15, peer="z", **context, read_id="cutoff",
                   latency=0.19, entries=entries, error=None, cancelled=False)
        events.add("ad_read_start", 110.16, peer="z", **context, read_id="cross_pause")
        events.add("read_paused", 110.2, peer="z", **context)
        events.add("ad_read_end", 110.6, peer="z", **context, read_id="cross_pause",
                   latency=0.44, entries=entries, error=None, cancelled=False)
        events.add("read_resumed", 111.0, peer="z", **context)
        events.add("ad_read_start", 111.1, peer="z", **context, read_id="cancelled")
        events.add("ad_read_end", 111.5, peer="z", **context, read_id="cancelled",
                   latency=0.4, entries=[], error="CancelledError", cancelled=True)
        events.add("read_disabled", 112.0, peer="z", **context)
        events.add("search_span_end", 112.1, peer="z", **context)
        events.add("step_result", 112.2, peer="z", name="H0", sequence=1,
                   step_id=sid, finished=112.2, status="completed")
        # The event pump can flush a late trace after the final bounded step
        # result. It must not extend the measured drain window.
        events.add("step_start", 112.21, peer="b", name="H1", sequence=1,
                   step_id="H1:1", value=10.0)
        late_search = "b:H1:1:late"
        events.add("search_span_start", 112.22, peer="b", step_id="H1:1",
                   search_id=late_search, group_key="g")
        events.add("read_enabled", 112.23, peer="b", step_id="H1:1",
                   search_id=late_search, group_key="g")
        events.add("read_disabled", 112.28, peer="b", step_id="H1:1",
                   search_id=late_search, group_key="g")
        events.add("search_span_end", 112.29, peer="b", step_id="H1:1",
                   search_id=late_search, group_key="g")

        result = self.reconstruct(events, config=trial_config(duration=10.0))
        peer = result["per_peer"]["H0"]
        self.assertAlmostEqual(peer["submission_enabled_seconds"], 0.5)
        self.assertAlmostEqual(peer["drain_enabled_seconds"], 1.2)
        self.assertAlmostEqual(peer["enabled_seconds"], 1.7)
        reads = {row["read_id"]: row for row in result["reads"]}
        self.assertEqual(reads["before"]["start_phase"], "submission")
        self.assertEqual(reads["before"]["completion_phase"], "submission")
        self.assertEqual(reads["cutoff"]["start_phase"], "submission")
        self.assertEqual(reads["cutoff"]["completion_phase"], "drain")
        self.assertEqual(reads["cross_pause"]["start_phase"], "drain")
        self.assertEqual(reads["cross_pause"]["completion_phase"], "drain")
        self.assertTrue(reads["cross_pause"]["in_flight_across_pause"])
        self.assertFalse(reads["cancelled"]["success"])
        self.assertTrue(reads["cancelled"]["cancelled"])
        self.assertAlmostEqual(reads["cancelled"]["latency_seconds"], 0.4)
        attempt = result["attempts"][0]
        self.assertTrue(attempt["submission_started"])
        self.assertEqual(attempt["attempt_phase"], "submission")
        self.assertTrue(attempt["drain_outcome"])
        self.assertEqual(attempt["step_result_status"], "completed")
        cadence = result["metrics"]["cadence"]
        self.assertEqual(cadence["reads_in_flight_across_pause"], 1)
        self.assertEqual(result["metrics"]["drain_end_monotonic"], 112.2)
        self.assertGreater(result["metrics"]["trace_closed_monotonic"], 112.2)
        self.assertAlmostEqual(result["per_peer"]["H1"]["drain_enabled_seconds"], 0.0)

        phases = cadence["honest_by_phase"]
        submission, drain = phases["submission"], phases["drain"]
        self.assertAlmostEqual(submission["enabled_seconds"], 0.5)
        self.assertAlmostEqual(drain["enabled_seconds"], 1.2)
        self.assertEqual(submission["read_start_count"], 2)
        self.assertEqual(drain["read_start_count"], 2)
        self.assertEqual(submission["successful_completed_reads"], 1)
        self.assertEqual(drain["successful_completed_reads"], 2)
        self.assertEqual(submission["successful_read_latency_seconds"], [0.15])
        self.assertEqual(drain["successful_read_latency_seconds"], [0.19, 0.44])
        self.assertAlmostEqual(submission["read_start_intervals_seconds"][0], 0.16)
        self.assertEqual(drain["read_start_intervals_seconds"], [])
        self.assertAlmostEqual(cadence["honest_read_start_intervals_seconds"][1], 0.2)

    def test_duplicate_assembly_ids_and_malformed_pairs_are_rejected(self):
        duplicated = Events()
        x_step(duplicated, step_id="X:1", start=101.0, status="completed",
               assembled=[(102.0, "same", ["a", "z"], "a"),
                          (103.0, "same", ["a", "z"], "a")], result_at=104.0)
        result = self.reconstruct(duplicated)
        self.assertEqual([row["group_id"] for row in result["groups"]], ["same"])
        self.assertFalse(result["trace_integrity_pass"])
        self.assertTrue(any("duplicate assembled group_id same" in error
                            for error in result["trace_integrity_errors"]))

        unpaired = Events()
        unpaired.add("ad_read_start", 101.0, peer="z", step_id="H0:1",
                     search_id="s", group_key="g", read_id="r")
        result = self.reconstruct(unpaired)
        self.assertFalse(result["trace_integrity_pass"])
        self.assertTrue(any("has no ad_read_end" in error
                            for error in result["trace_integrity_errors"]))

        orphan_join = Events()
        orphan_join.add("join_attempt_end", 101.0, peer="z", other="a",
                        step_id="H0:1", search_id="s", join_id="j",
                        origin="queue", selection_event_ref="1:1", error=None)
        result = self.reconstruct(orphan_join)
        self.assertFalse(result["trace_integrity_pass"])
        self.assertTrue(any("join_attempt_end without start" in error
                            for error in result["trace_integrity_errors"]))

    def test_native_selection_corrupted_against_captured_queue_is_rejected(self):
        events = Events()
        sid, search = "H0:1", "z:H0:1:search1"
        events.add("search_span_start", 101.0, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        events.add("read_enabled", 101.01, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        queue = [candidate("a", 1_010.0), candidate("b", 1_020.0)]
        ad_read(events, peer="z", step_id=sid, search_id=search, read_id="r1",
                start=101.1, end=101.2,
                entries=[{"peer": "a", "expiration": 1_010.0},
                         {"peer": "b", "expiration": 1_020.0}])
        pop_selection(events, peer="z", step_id=sid, search_id=search,
                      at=101.3, selected="b", queue=queue,
                      own_expiration=1_030.0, read_id="r1")
        events.add("read_disabled", 102.0, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        events.add("search_span_end", 102.1, peer="z", step_id=sid,
                   search_id=search, group_key="g")
        result = self.reconstruct(events)
        self.assertFalse(result["trace_integrity_pass"])
        self.assertTrue(any("not the live native queue top" in error
                            for error in result["trace_integrity_errors"]))


class Step25ChoiceFilesTests(unittest.TestCase):
    def test_complete_bundle_exports_per_seed_case_once(self):
        measured = dict(control_plane_pass=True, provenance={}, trials=[],
                        paired_differences_vs_passive_stock=[],
                        policy_table=[dict(case="passive_stock", per_seed=[
                            dict(case="passive_stock", seed=2501, x_attempt_denominator=3)])])
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary)/"suite"
            suite.mkdir()
            output = Path(temporary)/"analysis"
            with patch.object(choices, "analyse_suite_choices", return_value=measured):
                result = choices.write_outputs(suite, output)
            with (output/"per_seed.csv").open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(rows, [{"case": "passive_stock", "seed": "2501",
                                     "x_attempt_denominator": "3"}])
            self.assertTrue(result["control_plane_pass"])

    def test_missing_summary_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            trial = Path(temporary) / "passive_stock_s2501"
            trial.mkdir()
            (trial / "config.json").write_text(json.dumps(trial_config()))
            (trial / "events.jsonl").write_text("")
            with self.assertRaises((ValueError, FileNotFoundError)):
                choices.analyse_trial_choices(trial)

    def test_suite_gate_rejects_a_manifest_trial_without_its_summary(self):
        with tempfile.TemporaryDirectory() as temporary:
            suite = Path(temporary)
            config = trial_config()
            (suite / "suite.json").write_text(json.dumps({
                "phase": "smoke", "outcome": "grouping", "instrumentation": "detailed",
                "jobs": [{"case": config["case"], "seed": config["seed"],
                           "duration": config["duration"]}],
            }))
            harness = suite / "harness"
            harness.mkdir()
            snapshot = harness / "synthetic_harness.py"
            snapshot.write_text("synthetic fixture")
            digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            (suite / "provenance.json").write_text(json.dumps({
                "harness_sha256": {snapshot.name: digest},
            }))

            trial = suite / "passive_stock_s2501"
            trial.mkdir()
            (trial / "config.json").write_text(json.dumps(config))
            (trial / "events.jsonl").write_text("")
            with self.assertRaisesRegex(ValueError, "incomplete Step 25 suite"):
                choices.analyse_suite_choices(suite)

    def test_fresh_output_guard_does_not_touch_existing_directory_or_read_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            suite = base / "suite"
            suite.mkdir()
            output = base / "existing-output"
            output.mkdir()
            sentinel = output / "keep.txt"
            sentinel.write_text("preserve")
            with self.assertRaisesRegex(ValueError, "output directory already exists"):
                choices.write_outputs(suite, output)
            self.assertEqual(sentinel.read_text(), "preserve")


if __name__ == "__main__":
    unittest.main()
