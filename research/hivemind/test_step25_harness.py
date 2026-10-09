"""Verify Step 25 observations preserve native control-plane operations."""
import asyncio
import contextlib
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import step25_choice_measurement as harness


class Peer:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def to_bytes(self):
        return self.name.encode()


def leaders():
    return SimpleNamespace(peer_id=Peer("H0"), _s25_step_id="H0:1", _s25_search_id="search1",
                           _s25_key_manager=SimpleNamespace(current_key="pool.0b", _s25_latest_read="read1"),
                           declared_expiration_time=200.0, past_attempts=set(), _s25_top_wall=100.0,
                           running=asyncio.Event(), _s25_enabled=True, _s25_selected=None)


class ObservationTests(unittest.IsolatedAsyncioTestCase):
    def test_batched_reads_keep_timestamps_numbers_and_flush_at_lifecycle_end(self):
        packets = []
        queue = SimpleNamespace(put=packets.append)
        with patch.object(harness.base, "EVENTS", queue), \
             patch.object(harness, "BUFFER", []), \
             patch.object(harness, "BUFFER_PID", os.getpid()), \
             patch.object(harness.sweep, "EVENT_NUMBER", 0), \
             patch.object(harness.time, "monotonic", side_effect=[100.0, 200.0]):
            first_ref = harness.emit("ad_read_start", "H0", read_id="read1")
            self.assertEqual(packets, [])
            harness.emit("search_span_end", "H0", search_id="search1")
            self.assertEqual(len(packets), 1)
            rows = packets[0]["step25_events"]
            self.assertEqual([r["monotonic"] for r in rows], [100.0, 200.0])
            self.assertEqual([r["event_number"] for r in rows], [1, 2])
            self.assertEqual(first_ref, f"{os.getpid()}:1")
            self.assertEqual(harness.BUFFER, [])

    def test_dense_buffer_is_bounded_and_selection_flushes_remaining_rows(self):
        packets = []
        with patch.object(harness.base, "EVENTS", SimpleNamespace(put=packets.append)), \
             patch.object(harness, "BUFFER", []), \
             patch.object(harness, "BUFFER_PID", os.getpid()), \
             patch.object(harness.sweep, "EVENT_NUMBER", 0):
            for _ in range(harness.BATCH_SIZE):
                harness.emit("ad_view", "H0")
            self.assertEqual(len(packets), 1)
            self.assertEqual(len(packets[0]["step25_events"]), harness.BATCH_SIZE)
            harness.emit("ad_read_end", "H0")
            harness.emit("native_pop_selected", "H0", "X")
            self.assertEqual(len(packets), 2)
            self.assertEqual([r["kind"] for r in packets[-1]["step25_events"]],
                             ["ad_read_end", "native_pop_selected"])
            self.assertEqual(harness.BUFFER, [])

    def test_forked_sender_does_not_replay_parent_buffer(self):
        packets = []
        with patch.object(harness.base, "EVENTS", SimpleNamespace(put=packets.append)), \
             patch.object(harness, "BUFFER", [{"kind": "parent_only"}]), \
             patch.object(harness, "BUFFER_PID", -1):
            harness.emit("search_span_end", "H0")
        self.assertEqual([r["kind"] for r in packets[0]["step25_events"]], ["search_span_end"])

    def test_drain_restores_mixed_packets_without_reordering(self):
        first, second, third = {"kind": "first"}, {"kind": "second"}, {"kind": "third"}
        with patch.object(harness, "ORIGINAL_DRAIN", return_value=[{"step25_events": [first, second]}, third]):
            self.assertEqual(harness.drain_events(object(), quiet=0.01), [first, second, third])

    def test_minimal_emission_remains_unbatched(self):
        packets = []
        with patch.object(harness, "DETAIL", False), \
             patch.object(harness.base, "EVENTS", SimpleNamespace(put=packets.append)):
            harness.emit("ad_view", "H0")
        self.assertEqual(packets[0]["kind"], "ad_view")
        self.assertNotIn("step25_events", packets[0])

    async def test_begin_span_pairs_and_preserves_declare(self):
        calls, events = [], []
        pl, km, step = leaders(), SimpleNamespace(current_key="pool.0b"), object()

        @contextlib.asynccontextmanager
        async def original(self, actual_step, actual_manager, declare=True):
            calls.append((self, actual_step, actual_manager, declare))
            yield self

        with patch.object(harness, "ORIGINAL_BEGIN", original), \
             patch.object(harness.sweep, "user_data", return_value={"step_id": "H0:1"}), \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append(kind)):
            async with harness.begin_search(pl, step, km, declare=False) as result:
                self.assertIs(result, pl)
        self.assertEqual(calls, [(pl, step, km, False)])
        self.assertEqual(events, ["search_span_start", "read_enabled", "read_disabled", "search_span_end"])

    async def test_pause_resume_observes_native_running_flag(self):
        pl, events = leaders(), []
        pl.running.set()

        @contextlib.asynccontextmanager
        async def original(self):
            self.running.clear()
            try:
                yield
            finally:
                self.running.set()

        with patch.object(harness, "ORIGINAL_PAUSE", original), \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append(kind)):
            async with harness.pause_search(pl):
                self.assertFalse(pl.running.is_set())
                self.assertFalse(pl._s25_enabled)
        self.assertEqual(events, ["read_paused", "read_resumed"])
        self.assertTrue(pl._s25_enabled)

    async def test_read_preserves_arguments_and_result(self):
        pl, events, calls = leaders(), [], []
        km = SimpleNamespace(_s25_leaders=pl)
        result = [(Peer("X"), 150.0)]

        async def original(self, key, active):
            calls.append((self, key, active))
            return result

        with patch.object(harness.base, "ORIGINAL_READ", original), \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append((kind, detail))):
            actual = await harness.read_ads(km, "pool.0b", True)
        self.assertIs(actual, result)
        self.assertEqual(calls, [(km, "pool.0b", True)])
        self.assertEqual([kind for kind, _ in events], ["ad_read_start", "ad_read_end"])
        self.assertEqual(events[-1][1]["entries"], [{"peer": "X", "expiration": 150.0}])
        self.assertEqual(km._s25_latest_read, events[0][1]["read_id"])

    async def test_cancelled_read_closes_and_reraises(self):
        events = []
        km = SimpleNamespace(_s25_leaders=leaders())
        error = asyncio.CancelledError()

        async def original(*args):
            raise error

        with patch.object(harness.base, "ORIGINAL_READ", original), \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append((kind, detail))):
            with self.assertRaises(asyncio.CancelledError) as caught:
                await harness.read_ads(km, "pool.0b", True)
        self.assertIs(caught.exception, error)
        self.assertTrue(events[-1][1]["cancelled"])
        self.assertEqual(events[-1][1]["entries"], [])

    async def test_passive_x_read_guard_remains_before_native_read(self):
        km = SimpleNamespace(_s25_leaders=leaders())
        events = []
        with patch.object(harness.base, "ATTACKER", True), \
             patch.object(harness.base, "CASE", {"mode": "passive"}), \
             patch.object(harness.base, "ORIGINAL_READ") as native, \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append(kind)):
            with self.assertRaises(AssertionError):
                await harness.read_ads(km, "pool.0b", True)
        native.assert_not_called()
        self.assertEqual(events, ["forbidden_attacker_read"])

    def test_native_deletion_snapshot_does_not_prune_or_reorder(self):
        pl, events = leaders(), []
        chosen, other = Peer("X"), Peer("H1")
        chosen_entry = SimpleNamespace(expiration_time=150.0)
        queue = SimpleNamespace(data={chosen: chosen_entry, other: SimpleNamespace(expiration_time=170.0)})
        pl.leader_queue = queue

        def native_call(self, key):
            entry = chosen_entry  # native frame variable inspected by the hook
            return harness.queue_delete(self.leader_queue, key)

        def original(storage, key):
            self.assertEqual(list(storage.data), [chosen, other])
            del storage.data[key]
            return "native result"

        with patch.object(harness, "POP_CODE", native_call.__code__), \
             patch.object(harness, "ORIGINAL_DELETE", original), \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append((kind, detail)) or "123:4"):
            result = native_call(pl, chosen)
        self.assertEqual(result, "native result")
        self.assertEqual(list(queue.data), [other])
        self.assertEqual(events[0][1]["selected_expiration"], 150.0)
        self.assertEqual(events[0][1]["queue"][0]["peer"], "X")
        self.assertEqual(pl._s25_selected, (chosen, "123:4"))

    def test_unrelated_queue_calls_pass_through_without_observation(self):
        events = []
        queue, key = object(), object()
        with patch.object(harness, "ORIGINAL_DELETE", return_value="same") as original, \
             patch.object(harness, "emit", lambda *args, **kwargs: events.append(args)):
            self.assertEqual(harness.queue_delete(queue, key), "same")
        original.assert_called_once_with(queue, key)
        self.assertEqual(events, [])

    def test_native_top_returns_same_entry_and_captures_clock_before_call(self):
        pl = leaders()
        queue, result = object(), (Peer("X"), object())
        pl.leader_queue = queue

        def native_call(self):
            return harness.queue_top(self.leader_queue)

        def original(storage):
            self.assertIs(storage, queue)
            self.assertEqual(pl._s25_top_wall, 111.0)
            return result

        with patch.object(harness, "POP_CODE", native_call.__code__), \
             patch.object(harness, "ORIGINAL_TOP", original), \
             patch.object(harness.base, "get_dht_time", return_value=111.0):
            self.assertIs(native_call(pl), result)

    async def test_join_retains_result_and_binds_context(self):
        pl, peer, events = leaders(), Peer("X"), []
        pl._s25_selected = (peer, "123:4")
        mm = SimpleNamespace(peer_id=pl.peer_id, potential_leaders=pl)
        sentinel = object()

        async def original(self, leader):
            self_test.assertEqual(harness.JOIN_CONTEXT.get(), events[0][1]["join_id"])
            return sentinel

        self_test = self
        with patch.object(harness, "ORIGINAL_JOIN", original), \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append((kind, detail))):
            result = await harness.join_leader(mm, peer)
        self.assertIs(result, sentinel)
        self.assertEqual(events[0][1]["origin"], "queue")
        self.assertEqual(events[0][1]["selection_event_ref"], "123:4")
        self.assertEqual(events[-1][0], "join_attempt_end")
        self.assertIsNone(harness.JOIN_CONTEXT.get())

    async def test_nested_redirect_joins_restore_parent_context(self):
        pl, events = leaders(), []
        first, second = Peer("X"), Peer("H1")
        pl._s25_selected = (first, "123:4")
        mm = SimpleNamespace(peer_id=pl.peer_id, potential_leaders=pl)

        async def original(self, peer):
            if peer is first:
                parent = harness.JOIN_CONTEXT.get()
                await harness.join_leader(self, second)
                self_test.assertEqual(harness.JOIN_CONTEXT.get(), parent)
            return peer

        self_test = self
        with patch.object(harness, "ORIGINAL_JOIN", original), \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append((kind, detail))):
            await harness.join_leader(mm, first)
        self.assertEqual([d["origin"] for k, d in events if k == "join_attempt_start"], ["queue", "redirect"])
        self.assertIsNone(harness.JOIN_CONTEXT.get())

    async def test_join_exception_is_not_swallowed_and_context_is_restored(self):
        pl, events = leaders(), []
        mm = SimpleNamespace(peer_id=pl.peer_id, potential_leaders=pl)
        error = RuntimeError("native failure")

        async def original(self, peer):
            raise error

        with patch.object(harness, "ORIGINAL_JOIN", original), \
             patch.object(harness, "emit", lambda kind, *args, **detail: events.append((kind, detail))):
            with self.assertRaises(RuntimeError) as caught:
                await harness.join_leader(mm, Peer("X"))
        self.assertIs(caught.exception, error)
        self.assertEqual(events[-1][1]["error"], "RuntimeError")
        self.assertIsNone(harness.JOIN_CONTEXT.get())

    def test_minimal_mode_passes_through_without_queue_introspection(self):
        with patch.object(harness, "DETAIL", False), \
             patch.object(harness, "ORIGINAL_TOP", return_value="native") as original:
            storage = object()
            self.assertEqual(harness.queue_top(storage), "native")
        original.assert_called_once_with(storage)


if __name__ == "__main__":
    unittest.main()
