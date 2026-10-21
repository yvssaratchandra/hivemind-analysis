from __future__ import annotations

import importlib.util
import multiprocessing as mp
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest


MODULE_PATH = Path(__file__).with_name("recordholder_exclusion_smoke.py")
SPEC = importlib.util.spec_from_file_location("recordholder_exclusion_smoke", MODULE_PATH)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class RecordHolderExclusionSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        package_root = probe.ROOT / "research/simulator/.native-runtime/root_snapshot_a"
        dependencies = probe.ROOT / "research/agora/.runtime/venv/lib/python3.11/site-packages"
        if not package_root.exists() or not dependencies.exists():
            raise unittest.SkipTest("prepared native runtime is unavailable")
        cls.original_cwd = Path.cwd()
        cls.original_path = list(sys.path)
        probe.smoke.configure_native(SimpleNamespace(package_root=package_root,
                                                     dependencies=dependencies))
        from hivemind.dht.routing import DHTID
        from hivemind.dht.storage import DictionaryDHTValue
        from hivemind.proto import averaging_pb2
        from hivemind.utils import get_dht_time
        cls.DHTID = DHTID
        cls.DictionaryDHTValue = DictionaryDHTValue
        cls.averaging_pb2 = averaging_pb2
        cls.get_dht_time = get_dht_time

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "original_path"):
            sys.path[:] = cls.original_path
            os.chdir(cls.original_cwd)

    def test_twenty_crafted_ids_are_exactly_nearest_key_addresses(self):
        key = self.DHTID.generate(source="offline_test_gradient_probe.0b")
        ids = probe.xor_nearby_ids(int(key), 20)

        self.assertEqual(len(set(ids)), 20)
        self.assertEqual([candidate ^ int(key) for candidate in ids], list(range(20)))
        self.assertEqual([self.DHTID(candidate).to_bytes() for candidate in ids],
                         [self.DHTID(int(key) ^ i).to_bytes() for i in range(20)])

    def test_fresh_repeated_markers_distinguish_four_and_three_peer_means(self):
        four = [probe.marker_vector(node) for node in probe.NODES]
        three = [probe.marker_vector(node) for node in ("X", "D", "C")]

        self.assertEqual(len(four[0]), 192)
        self.assertTrue(probe.vector_matches(probe.mean_vectors(four),
                                             probe.expected_marker_mean(probe.NODES)))
        expected_three = probe.mean_vectors(three)
        self.assertTrue(probe.vector_matches(expected_three,
                                             probe.expected_marker_mean(("X", "D", "C"))))
        self.assertEqual(expected_three[0::4], [0.0] * 48)
        self.assertEqual(probe.marker_vector("A"), four[0])

    def test_native_dictionary_filter_preserves_x_value_and_expiration(self):
        source = self.DictionaryDHTValue(maxsize=10)
        expiration = self.get_dht_time() + 120.0
        source.store(b"peer-A", True, expiration)
        source.store(b"peer-X", True, expiration + 20.0)
        source.store(b"peer-D", False, expiration + 10.0)

        filtered = probe.filter_dictionary_subkeys(source, {b"peer-X"}, self.DictionaryDHTValue)

        self.assertEqual([key for key, _entry in filtered.items()], [b"peer-X"])
        self.assertEqual(filtered.get(b"peer-X").value, True)
        self.assertEqual(filtered.get(b"peer-X").expiration_time, expiration + 20.0)
        self.assertEqual([key for key, _entry in source.items()], [b"peer-A", b"peer-X", b"peer-D"])

    def test_native_protobuf_rewrite_filters_non_target_and_keeps_arrays_aligned(self):
        names = {b"peer-A": "A", b"peer-X": "X", b"peer-D": "D", b"peer-C": "C"}
        native = self.averaging_pb2.MessageFromLeader(
            code=self.averaging_pb2.BEGIN_ALLREDUCE,
            group_id=b"test-group",
            ordered_peer_ids=list(names),
            gathered=[b"gather-A", b"gather-X", b"gather-D", b"gather-C"],
        )
        original_bytes = native.SerializeToString()
        label_peer = lambda peer: names[bytes(peer)]
        peer_id_type = lambda raw: bytes(raw)

        control, control_changed, _, _ = probe.announcement_for_recipient(
            native, "capture_control", "D", peer_id_type,
            self.averaging_pb2.MessageFromLeader, label_peer)
        target, target_changed, _, _ = probe.announcement_for_recipient(
            native, "capture_exclusion", "A", peer_id_type,
            self.averaging_pb2.MessageFromLeader, label_peer)
        follower, follower_changed, native_members, effective_members = probe.announcement_for_recipient(
            native, "capture_exclusion", "D", peer_id_type,
            self.averaging_pb2.MessageFromLeader, label_peer)

        self.assertFalse(control_changed)
        self.assertFalse(target_changed)
        self.assertEqual(control.SerializeToString(), original_bytes)
        self.assertEqual(target.SerializeToString(), original_bytes)
        self.assertTrue(follower_changed)
        self.assertEqual(native_members, ["A", "X", "D", "C"])
        self.assertEqual(effective_members, ["X", "D", "C"])
        self.assertEqual(list(follower.ordered_peer_ids), [b"peer-X", b"peer-D", b"peer-C"])
        self.assertEqual(list(follower.gathered), [b"gather-X", b"gather-D", b"gather-C"])
        self.assertEqual(native.SerializeToString(), original_bytes)

    def test_overrides_keep_native_rpc_annotations_and_process_fields_intact(self):
        from hivemind.dht.protocol import DHTProtocol
        from hivemind.p2p import P2PContext
        from hivemind.proto import dht_pb2

        original_rpc_find = DHTProtocol.rpc_find
        try:
            probe._install_record_holder_filter({}, {}, None)
            patched = DHTProtocol.rpc_find
            self.assertIs(patched.__annotations__["request"], dht_pb2.FindRequest)
            self.assertIs(patched.__annotations__["context"], P2PContext)
            self.assertIs(patched.__annotations__["return"], dht_pb2.FindResponse)
        finally:
            DHTProtocol.rpc_find = original_rpc_find
            probe._FILTER = probe._IDENTITIES = probe._EVENTS = None

        averager_class = probe._make_averager_class({}, {}, {}, "capture_exclusion", "victim", "attacker")
        process_fields = set(vars(mp.Process(target=lambda: None)))
        subclass_methods = {name for name, member in averager_class.__dict__.items()
                            if callable(member) and not name.startswith("__")}
        self.assertEqual(sorted(process_fields & subclass_methods), [])
        self.assertIs(averager_class.rpc_join_group.__annotations__["request"],
                      self.averaging_pb2.JoinRequest)

    def test_summary_uses_saved_inputs_and_separates_partial_native_groups(self):
        events = []
        for node in probe.NODES:
            events.append({"kind": "step_started", "arm": "capture_exclusion",
                           "channel": "gradient_probe", "round": 0, "peer": node,
                           "input_marker": probe.marker_vector(node)})
        for node, effective in (("D", ["X", "D", "C"]), ("C", ["X", "D", "C"])):
            expected = probe.mean_vectors([probe.marker_vector(peer) for peer in effective])
            events.extend([
                {"kind": "step_result", "arm": "capture_exclusion", "channel": "gradient_probe",
                 "round": 0, "peer": node, "status": "success", "output": expected},
                {"kind": "aggregate_output", "arm": "capture_exclusion", "channel": "gradient_probe",
                 "round": 0, "peer": node, "leader": "X", "group_id": "g0",
                 "effective_members": effective, "tensor_after": expected},
                {"kind": "announcement_sent", "arm": "capture_exclusion", "channel": "gradient_probe",
                 "round": 0, "leader": "X", "recipient": node, "group_id": "g0",
                 "native_members": list(probe.NODES), "effective_members": effective,
                 "excluded_target": True},
            ])

        summary = probe.summarize_events(events, "capture_exclusion", rounds=1)["channels"][0]

        self.assertEqual(summary["honest_D_C_exact_effective_mean_count"], 2)
        self.assertEqual(summary["honest_D_C_zero_A_origin_count"], 2)
        self.assertEqual(len(summary["honest_D_C_full_native_group_outputs"]), 2)
        self.assertEqual(summary["honest_D_C_partial_or_unconfirmed_group_outputs"], [])
        self.assertNotIn("A", summary["honest_D_C_full_native_group_outputs"][0]["saved_group_input_vectors"])

        events = [event for event in events if not (event.get("kind") == "announcement_sent" and
                                                    event.get("recipient") == "D")]
        partial = probe.summarize_events(events, "capture_exclusion", rounds=1)["channels"][0]
        self.assertEqual(partial["honest_D_C_exact_effective_mean_count"], 1)
        self.assertEqual(len(partial["honest_D_C_partial_or_unconfirmed_group_outputs"]), 1)


if __name__ == "__main__":
    unittest.main()
