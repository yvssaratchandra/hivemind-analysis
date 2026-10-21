"""Focused unit checks for native hook scoping and opaque roster handling."""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
from pathlib import Path
import sys
import typing
import unittest

from research.hivemind.compute_native_hooks import _event_scope, _filter_aligned_roster, install_hooks

NATIVE_ROOT = Path(__file__).resolve().parents[1] / "simulator" / ".native-runtime" / "root_snapshot_a"
if NATIVE_ROOT.is_dir() and str(NATIVE_ROOT) not in sys.path:
    sys.path.insert(0, str(NATIVE_ROOT))


class FakePeer:
    def __init__(self, label: str):
        self.label = label

    def __str__(self):
        return self.label

    def to_bytes(self):
        return self.label.encode()


class HookPureTests(unittest.TestCase):
    def test_event_scope_marks_only_exact_gradient_prefix(self):
        self.assertEqual(
            _event_scope("run_grad", "run_grad"),
            {"channel": "gradient", "prefix": "run_grad", "traceprefix": "run_grad"},
        )
        self.assertEqual(_event_scope("run", "run_grad")["channel"], "state")
        self.assertEqual(_event_scope("run_grad_extra", "run_grad")["channel"], "state")

    def test_roster_filter_preserves_opaque_metadata_alignment(self):
        peers = (FakePeer("B"), FakePeer("A"), FakePeer("D"), FakePeer("C"))
        metadata = (b"\x00opaque-B", b"\xffopaque-A", b"opaque-D", b"opaque-C")
        kept_peers, kept_metadata, removed = _filter_aligned_roster(
            peers, metadata, "A", lambda peer: peer.label
        )
        self.assertTrue(removed)
        self.assertEqual([peer.label for peer in kept_peers], ["B", "D", "C"])
        self.assertEqual(kept_metadata, (metadata[0], metadata[2], metadata[3]))

    def test_roster_filter_does_not_claim_absent_peer_was_removed(self):
        peers = (FakePeer("B"), FakePeer("D"))
        metadata = (b"opaque-B", b"opaque-D")
        kept_peers, kept_metadata, removed = _filter_aligned_roster(
            peers, metadata, "A", lambda peer: peer.label
        )
        self.assertFalse(removed)
        self.assertEqual(kept_peers, peers)
        self.assertEqual(kept_metadata, metadata)

    def test_roster_filter_rejects_misaligned_metadata(self):
        with self.assertRaises(ValueError):
            _filter_aligned_roster((FakePeer("A"),), (), "A", lambda peer: peer.label)

    def test_install_validates_condition_before_importing_native_runtime(self):
        with self.assertRaisesRegex(ValueError, "condition"):
            install_hooks(
                node="B",
                condition="unexpected",
                emit=lambda *_args, **_kwargs: None,
                gradient_prefix="run_grad",
                identities={},
            )


try:
    from hivemind.averaging.averager import DecentralizedAverager
    from hivemind.averaging.group_info import GroupInfo
    from hivemind.averaging.key_manager import GroupKeyManager
    from hivemind.averaging.matchmaking import Matchmaking, PotentialLeaders
except ImportError:
    DecentralizedAverager = GroupInfo = GroupKeyManager = Matchmaking = PotentialLeaders = None


@unittest.skipIf(DecentralizedAverager is None, "native Hivemind snapshot is not on sys.path")
class NativePatchTests(unittest.TestCase):
    def test_leader_fixture_is_exact_prefix_scoped_and_keeps_node_peer(self):
        events = []
        uninstall = install_hooks(
            node="C",
            condition="baseline",
            emit=lambda kind, **fields: events.append((kind, fields)),
            gradient_prefix="run_grad",
            identities={"C": "C", "B": "B"},
        )

        class DHT:
            def __init__(self):
                self.calls = []

            async def store(self, **kwargs):
                self.calls.append(kwargs)
                return True

        def manager(prefix):
            instance = object.__new__(GroupKeyManager)
            instance.prefix = prefix
            instance.dht = DHT()
            return instance

        peer = FakePeer("C")
        try:
            gradient = manager("run_grad")
            result = asyncio.run(gradient.declare_averager("run_grad.0b", peer, 1.0))
            self.assertFalse(result)
            self.assertEqual(gradient.dht.calls, [])
            self.assertTrue(gradient.dht is not None)  # suppression does not change peer role

            state = manager("run_state")
            result = asyncio.run(state.declare_averager("run_state.0b", peer, 1.0))
            self.assertTrue(result)
            self.assertEqual(len(state.dht.calls), 1)
            suppressed = [fields for kind, fields in events if kind == "leader_advertisement_suppressed"]
            self.assertEqual(len(suppressed), 1)
            self.assertEqual(suppressed[0]["channel"], "gradient")
            self.assertEqual(suppressed[0]["traceprefix"], "run_grad")
        finally:
            uninstall()

    def test_only_b_leads_in_absent_a_condition_too(self):
        events = []
        uninstall = install_hooks(
            node="D",
            condition="absent",
            emit=lambda kind, **fields: events.append((kind, fields)),
            gradient_prefix="run_grad",
            identities={"D": "D", "B": "B", "C": "C"},
        )

        class DHT:
            async def store(self, **kwargs):
                return True

        manager = object.__new__(GroupKeyManager)
        manager.prefix = "run_grad"
        manager.dht = DHT()
        try:
            result = asyncio.run(manager.declare_averager("run_grad.0b", FakePeer("D"), 1.0))
            self.assertFalse(result)
            suppressed = [fields for kind, fields in events if kind == "leader_advertisement_suppressed"]
            self.assertEqual(len(suppressed), 1)
            self.assertEqual(suppressed[0]["peer"], "D")
            self.assertEqual(suppressed[0]["channel"], "gradient")
        finally:
            uninstall()

    def test_follower_search_suppresses_declaration_and_uses_finite_gradient_deadline(self):
        original_begin_search = PotentialLeaders.begin_search
        original_expiration = Matchmaking.get_request_expiration_time
        begin_calls = []

        @contextlib.asynccontextmanager
        async def capture_begin_search(self, step, key_manager, declare=True):
            begin_calls.append((self.peer_id.label, key_manager.prefix, declare))
            yield self

        def native_expiration(_self):
            return 91.0

        PotentialLeaders.begin_search = capture_begin_search
        Matchmaking.get_request_expiration_time = native_expiration
        events = []
        uninstall = None
        try:
            uninstall = install_hooks(
                node="C",
                condition="absent",
                emit=lambda kind, **fields: events.append((kind, fields)),
                gradient_prefix="run_grad",
                identities={"A": "A", "B": "B", "C": "C"},
            )

            async def enter_search(peer_name, prefix):
                leaders = type("Leaders", (), {"peer_id": FakePeer(peer_name)})()
                key_manager = type("KeyManager", (), {"prefix": prefix, "current_key": f"{prefix}.0b0"})()
                step = type("Step", (), {"deadline": 123.0})()
                async with PotentialLeaders.begin_search(leaders, step, key_manager, declare=True):
                    pass

            asyncio.run(enter_search("C", "run_grad"))
            asyncio.run(enter_search("B", "run_grad"))
            asyncio.run(enter_search("C", "run_state"))
            self.assertEqual(
                begin_calls,
                [("C", "run_grad", False), ("B", "run_grad", True), ("C", "run_state", True)],
            )
            suppressed = [fields for kind, fields in events if kind == "leader_advertisement_suppressed"]
            self.assertEqual(len(suppressed), 1)
            self.assertEqual(suppressed[0]["method"], "PotentialLeaders.begin_search")
            self.assertFalse(suppressed[0]["effective_declare"])

            class FakeMatchmaking:
                def __init__(self, peer_name, prefix, deadline):
                    self.peer_id = FakePeer(peer_name)
                    self._prefix = prefix
                    self.step_control = type("Step", (), {"deadline": deadline})()

            follower = FakeMatchmaking("C", "run_grad", 123.0)
            leader = FakeMatchmaking("B", "run_grad", 123.0)
            state = FakeMatchmaking("C", "run_state", 123.0)
            infinite_deadline = FakeMatchmaking("C", "run_grad", float("inf"))
            self.assertEqual(Matchmaking.get_request_expiration_time(follower), 123.0)
            self.assertEqual(Matchmaking.get_request_expiration_time(leader), 91.0)
            self.assertEqual(Matchmaking.get_request_expiration_time(state), 91.0)
            self.assertEqual(Matchmaking.get_request_expiration_time(infinite_deadline), 91.0)
            expiration_events = [fields for kind, fields in events if kind == "follower_join_expiration"]
            self.assertEqual([event["basis"] for event in expiration_events], ["step_deadline", "native", "native", "native"])
            self.assertTrue(all(event["finite"] for event in expiration_events))
        finally:
            if uninstall is not None:
                uninstall()
            PotentialLeaders.begin_search = original_begin_search
            Matchmaking.get_request_expiration_time = original_expiration

    def test_state_download_records_donor_recipient_chunks_and_terminal_outcome(self):
        from hivemind.proto import averaging_pb2, runtime_pb2

        original = DecentralizedAverager.rpc_download_state
        tensor_part = runtime_pb2.Tensor(buffer=b"\x00\xfe", size=[2], dtype="torch.float32", chunks=1)
        data = averaging_pb2.DownloadData(metadata=b"\x00opaque-metadata", tensor_part=tensor_part)

        async def fake_download(self, _request, _context):
            if getattr(_context, "fail", False):
                raise RuntimeError("download failed")
            yield data

        fake_download.__annotations__ = typing.get_type_hints(original)
        DecentralizedAverager.rpc_download_state = fake_download
        events = []
        uninstall = None
        try:
            uninstall = install_hooks(
                node="B",
                condition="baseline",
                emit=lambda kind, **fields: events.append((kind, fields)),
                gradient_prefix="run_grad",
                identities={"B": "B", "C": "C"},
            )

            old_handlers = DecentralizedAverager._rpc_handlers
            old_stub_type = DecentralizedAverager._stub_type
            try:
                DecentralizedAverager._rpc_handlers = None
                DecentralizedAverager._stub_type = None
                DecentralizedAverager._collect_rpc_handlers()
                handler = next(
                    item
                    for item in DecentralizedAverager._rpc_handlers
                    if item.method_name == "rpc_download_state"
                )
                self.assertIs(handler.request_type, averaging_pb2.DownloadRequest)
                self.assertIs(handler.response_type, averaging_pb2.DownloadData)
                self.assertTrue(handler.stream_output)
            finally:
                DecentralizedAverager._rpc_handlers = old_handlers
                DecentralizedAverager._stub_type = old_stub_type

            class FakeDonor:
                prefix = "run_state"
                peer_id = FakePeer("B")

            request = averaging_pb2.DownloadRequest()

            async def collect(context):
                return [
                    item
                    async for item in DecentralizedAverager.rpc_download_state(
                        FakeDonor(), request, context
                    )
                ]

            delivered = asyncio.run(collect(type("Context", (), {"remote_id": FakePeer("C")})()))
            self.assertEqual(delivered[0].SerializeToString(), data.SerializeToString())
            chunk = next(fields for kind, fields in events if kind == "state_download_chunk")
            self.assertEqual(chunk["channel"], "state")
            self.assertEqual(chunk["sender"], "B")
            self.assertEqual(chunk["recipient"], "C")
            self.assertEqual(chunk["sender_peer_id_hex"], b"B".hex())
            self.assertEqual(chunk["recipient_peer_id_hex"], b"C".hex())
            self.assertEqual(chunk["metadata_hex"], b"\x00opaque-metadata".hex())
            self.assertEqual(chunk["tensor_payload_hex"], b"\x00\xfe".hex())
            self.assertEqual(
                chunk["serialized_chunk_sha256"],
                hashlib.sha256(tensor_part.SerializeToString()).hexdigest(),
            )
            terminal = next(fields for kind, fields in events if kind == "state_download_terminal")
            self.assertTrue(terminal["success"])
            self.assertEqual(terminal["outcome"], "complete")
            self.assertEqual(terminal["chunks"], 1)
            self.assertTrue(terminal["metadata_sent"])

            failing_context = type("Context", (), {"remote_id": FakePeer("C"), "fail": True})()
            with self.assertRaisesRegex(RuntimeError, "download failed"):
                asyncio.run(collect(failing_context))
            failed_terminal = [
                fields for kind, fields in events if kind == "state_download_terminal"
            ][-1]
            self.assertFalse(failed_terminal["success"])
            self.assertEqual(failed_terminal["outcome"], "error")
            self.assertIn("download failed", failed_terminal["error"])
            self.assertEqual(failed_terminal["chunks"], 0)
        finally:
            if uninstall is not None:
                uninstall()
            DecentralizedAverager.rpc_download_state = original

    def test_recovery_fetch_metadata_and_post_application_state_are_recorded(self):
        import torch
        from hivemind.optim.state_averager import TrainingStateAverager

        base_original = DecentralizedAverager.load_state_from_peers
        training_original = TrainingStateAverager.load_state_from_peers
        events = []

        def fetch_state(_self, *args, **kwargs):
            return {"epoch": 3, "group_bits": "01"}, [torch.tensor([4.0])]

        def apply_state(self, **kwargs):
            metadata, tensors = DecentralizedAverager.load_state_from_peers(self, **kwargs)
            self.value.copy_(tensors[0])
            self.local_epoch = metadata["epoch"]

        DecentralizedAverager.load_state_from_peers = fetch_state
        TrainingStateAverager.load_state_from_peers = apply_state

        class FakeStateAverager:
            prefix = "run_state"
            local_epoch = 1

            def __init__(self):
                self.value = torch.tensor([0.0])

            def _local_tensors(self):
                return (self.value,)

        state = FakeStateAverager()
        uninstall = None
        try:
            uninstall = install_hooks(
                node="D",
                condition="baseline",
                emit=lambda kind, **fields: events.append((kind, fields)),
                gradient_prefix="run_grad",
                identities={},
                snapshot=lambda: state.value.tolist(),
            )
            TrainingStateAverager.load_state_from_peers(state)
            fetch = next(fields for kind, fields in events if kind == "state_recovery_fetch")
            applications = [fields for kind, fields in events if kind == "state_recovery_application"]
            before = next(fields for fields in applications if fields["phase"] == "before")
            after = next(fields for fields in applications if fields["phase"] == "after")
            self.assertEqual(fetch["metadata"], {"epoch": 3, "group_bits": "01"})
            self.assertEqual(fetch["values"], [[4.0]])
            self.assertEqual(before["local_tensors"], [[0.0]])
            self.assertEqual(after["local_tensors"], [[4.0]])
            self.assertEqual(after["local_epoch_after"], 3)
            self.assertTrue(after["metadata_epoch_eligible"])
            self.assertTrue(after["local_epoch_advanced"])
            self.assertTrue(after["local_epoch_matches_metadata"])
            self.assertEqual(after["snapshot"], [4.0])
        finally:
            if uninstall is not None:
                uninstall()
            DecentralizedAverager.load_state_from_peers = base_original
            TrainingStateAverager.load_state_from_peers = training_original

    def test_exclusion_changes_only_b_local_gradient_aggregate(self):
        original = DecentralizedAverager._aggregate_with_group
        observed = {}

        async def capture(_self, group_info, min_vector_size, **kwargs):
            observed["group"] = group_info
            observed["min_vector_size"] = min_vector_size
            return group_info

        DecentralizedAverager._aggregate_with_group = capture
        events = []
        uninstall = None

        class FakeAverager:
            prefix = "run_grad"
            peer_id = FakePeer("B")

            @contextlib.contextmanager
            def get_tensors(self):
                yield ()

        peers = tuple(FakePeer(name) for name in ("B", "A", "D", "C"))
        gathered = (b"B:opaque", b"A:opaque", b"D:opaque", b"C:opaque")
        group = GroupInfo(b"group-id", peers, gathered)
        try:
            uninstall = install_hooks(
                node="B",
                condition="exclusion",
                emit=lambda kind, **fields: events.append((kind, fields)),
                gradient_prefix="run_grad",
                identities={name: name for name in ("A", "B", "C", "D")},
            )
            result = asyncio.run(DecentralizedAverager._aggregate_with_group(FakeAverager(), group, 0))
            self.assertEqual([peer.label for peer in observed["group"].peer_ids], ["B", "D", "C"])
            self.assertEqual(observed["group"].gathered, (gathered[0], gathered[2], gathered[3]))
            self.assertEqual(result, observed["group"])
            local_roster = next(fields for kind, fields in events if kind == "local_roster")
            self.assertEqual(local_roster["native_members"], ["B", "A", "D", "C"])
            self.assertEqual(local_roster["members"], ["B", "D", "C"])
            self.assertEqual(local_roster["excluded"], ["A"])
            self.assertEqual(local_roster["channel"], "gradient")
            self.assertEqual(local_roster["traceprefix"], "run_grad")
        finally:
            if uninstall is not None:
                uninstall()
            DecentralizedAverager._aggregate_with_group = original


if __name__ == "__main__":
    unittest.main()
