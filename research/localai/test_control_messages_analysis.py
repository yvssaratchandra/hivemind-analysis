"""Focused parser/analysis fixtures for captured LocalAI control traces."""
import base64
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import analyze_control_messages as analyzer


def encoded_data(value):
    # EdgeVPN blockchain.Data is a Go string containing JSON-encoded content.
    return json.dumps(value, separators=(",", ":"))


def block(index, block_hash, prev_hash, storage):
    return {"Index": index, "Timestamp": "2026-10-07T10:00:00Z", "Hash": block_hash,
            "PrevHash": prev_hash, "Storage": storage}


def gossip(block_value, timestamp, peer="relay-1", observer="tap-1"):
    body = gzip.compress(json.dumps(block_value, separators=(",", ":")).encode())
    return {"event": "gossip_message", "timestamp": timestamp, "observer_id": observer,
            "received_from_peer_id": peer, "body_sha256": hashlib.sha256(body).hexdigest(),
            "body_base64": base64.b64encode(body).decode(), "block": block_value}


class ControlMessageAnalysisTests(unittest.TestCase):
    def test_failed_runner_status_is_visible_without_completion_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            directory.joinpath("summary.json").write_text(json.dumps({"label": "failed-run",
                "status": "failed", "error": "Controller process limit"}))
            directory.joinpath("observer.jsonl").write_text("")
            result = analyzer.analyze(directory)
            self.assertEqual(result["run"]["status"], "failed")
            report = directory.joinpath("LIFECYCLE_REPORT.md").read_text()
            self.assertIn("Run status: **failed**", report)
            self.assertIn("**Partial capture:**", report)
            self.assertIn("Controller process limit", report)

    def test_snapshot_diff_deduplicates_retransmissions_and_separates_phases(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            directory.joinpath("summary.json").write_text(json.dumps({"label": "fixture", "source_revision": "abc"}))
            directory.joinpath("phases.jsonl").write_text("\n".join([
                json.dumps({"phase": "join", "action": "begin", "timestamp": "2026-10-07T10:00:00Z"}),
                json.dumps({"phase": "steady", "action": "begin", "timestamp": "2026-10-07T10:00:10Z"}),
            ]) + "\n")
            first = block(1, "block-1", "genesis", {
                "users": {"peer-1": encoded_data({"PeerID": "peer-1", "Timestamp": "2026-10-07T10:00:01Z"})},
                "healthcheck": {"peer-1": encoded_data("2026-10-07T10:00:01Z")},
                "net42_federated": {"worker-1": encoded_data({"Name": "worker-1", "ID": "peer-1", "LastSeen": "2026-10-07T10:00:01Z"})},
            })
            second = block(2, "block-2", "block-1", {
                "users": {"peer-1": encoded_data({"PeerID": "peer-1", "Timestamp": "2026-10-07T10:00:11Z"}),
                          "peer-2": encoded_data({"PeerID": "peer-2", "Timestamp": "2026-10-07T10:00:11Z"})},
                "healthcheck": {"peer-1": encoded_data("2026-10-07T10:00:11Z"), "peer-2": encoded_data("2026-10-07T10:00:11Z")},
                "net42_federated": {"worker-1": encoded_data({"Name": "worker-1", "ID": "peer-1", "LastSeen": "2026-10-07T10:00:11Z"})},
            })
            events = [gossip(first, "2026-10-07T10:00:02Z"),
                      gossip(second, "2026-10-07T10:00:12Z"),
                      gossip(second, "2026-10-07T10:00:13Z", peer="relay-2")]
            directory.joinpath("observer.jsonl").write_text("\n".join(json.dumps(item) for item in events) + "\n")

            result = analyzer.analyze(directory)
            counts = result["counts"]
            self.assertEqual(counts["gossip_envelope_receptions"], 3)
            self.assertEqual(counts["unique_ledger_block_hashes_per_observer"], 2)
            self.assertEqual(counts["duplicate_block_snapshot_receptions"], 1)
            self.assertEqual(result["phase_counts"]["join"]["gossip_receptions"], 1)
            self.assertEqual(result["phase_counts"]["steady"]["gossip_receptions"], 2)
            changes = {(row["bucket"], row["action"]): row["count"] for row in result["record_change_catalogue"]}
            self.assertEqual(changes[("users", "present_at_first_observation")], 1)
            self.assertEqual(changes[("users", "added")], 1)
            self.assertEqual(changes[("healthcheck", "added")], 1)
            self.assertEqual(changes[("net42_federated", "changed")], 1)
            health = next(row for row in result["lifecycle_categories"] if row["id"] == "health_and_keepalive")
            self.assertEqual(len(health["record_changes"]), 3)
            report = directory.joinpath("LIFECYCLE_REPORT.md").read_text()
            self.assertIn("whole ledger block", report)
            self.assertIn("Observed records/events", report)
            self.assertIn("observer.jsonl:2", report)

    def test_stale_future_and_removed_records_are_reported_without_calling_them_leave_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            directory.joinpath("phases.jsonl").write_text(json.dumps({"phase": "steady", "action": "begin", "timestamp": "2026-10-07T10:00:00Z"}) + "\n")
            old = block(1, "b1", "g", {"net_federated": {
                "old": encoded_data({"Name": "old", "ID": "peer-old", "LastSeen": "2026-10-07T09:58:00Z"}),
                "future": encoded_data({"Name": "future", "ID": "peer-future", "LastSeen": "2026-10-07T10:02:00Z"}),
            }})
            new = block(2, "b2", "b1", {"net_federated": {
                "future": encoded_data({"Name": "future", "ID": "peer-future", "LastSeen": "2026-10-07T10:00:30Z"}),
            }})
            rows = [gossip(old, "2026-10-07T10:00:00Z"), gossip(new, "2026-10-07T10:00:40Z")]
            directory.joinpath("observer.jsonl").write_text("\n".join(json.dumps(item) for item in rows) + "\n")
            result = analyzer.analyze(directory)
            statuses = {(row["bucket"], row["status"]): row["count"] for row in result["record_freshness"]["observations"]}
            self.assertEqual(statuses[("net_federated", "stale")], 1)
            self.assertEqual(statuses[("net_federated", "future")], 1)
            removed = next(row for row in result["record_change_catalogue"] if row["action"] == "removed")
            self.assertEqual(removed["lifecycle_category"], "churn_and_expiry")
            self.assertEqual(removed["snapshot_lineage_counts"], {"direct_child_or_initial": 1})
            self.assertIn("not explicit leave messages", result["limitations"][3])

    def test_unknown_bucket_malformed_json_and_unknown_event_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            state = block(1, "new-block", "g", {"future_bucket": {"opaque-key": encoded_data({"x": 1})}})
            lines = [json.dumps(gossip(state, "2026-10-07T10:00:00Z")), "{broken", json.dumps({"event": "future_event", "timestamp": "2026-10-07T10:00:01Z", "payload": "kept"})]
            directory.joinpath("observer.jsonl").write_text("\n".join(lines) + "\n")
            result = analyzer.analyze(directory)
            unknown = [row for row in result["record_change_catalogue"] if row["bucket"] == "future_bucket"]
            self.assertEqual(unknown[0]["lifecycle_category"], "unknown_or_unclassified")
            self.assertEqual(len(result["malformed_or_unknown_events"]), 2)
            self.assertEqual(result["counts"]["malformed_or_unknown_events"], 2)
            self.assertEqual(sum(item["observer_events"] for item in result["phase_counts"].values()), result["counts"]["parsed_observer_event_count"])
            self.assertTrue(any(row.get("error") == "invalid_json: Expecting property name enclosed in double quotes" for row in result["malformed_or_unknown_events"]))

    def test_wire_hash_check_and_bucket_reference_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            directory.joinpath("message_protocol_reference.json").write_text(json.dumps({
                "buckets": {"new_bucket": {"category": "shared_model_state", "meaning": "fixture-defined state"}},
                "timing": {"node_online_seconds": 25, "healthcheck_max_age_seconds": 120},
            }))
            state = block(1, "ref-block", "g", {"new_bucket": {"models": encoded_data(["tiny-model"])}})
            row = gossip(state, "2026-10-07T10:00:00Z")
            row["body_sha256"] = "0" * 64
            directory.joinpath("observer.jsonl").write_text(json.dumps(row) + "\n")
            result = analyzer.analyze(directory)
            self.assertEqual(result["counts"]["body_sha256_mismatches"], 1)
            self.assertEqual(result["record_freshness"]["thresholds"]["node_last_seen_seconds"], 25)
            self.assertEqual(result["record_change_catalogue"][0]["lifecycle_category"], "shared_model_state")

    def test_real_reference_shape_matches_prefixed_federation_buckets(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            directory.joinpath("message_protocol_reference.json").write_text(json.dumps({
                "source_pin": {"localai_commit": "abc"},
                "lifecycle_categories": [{"category": "2. Initial advertisement", "records_or_messages": [{
                    "name": "LocalAI federated worker advertisement",
                    "bucket": "<P2PNetworkID>_federated, federated",
                    "kind": "application ledger record",
                    "nominal_schedule": "Every 20s maximum",
                    "source": "core/p2p/p2p.go",
                }]}],
            }))
            metadata, info = analyzer.load_protocol_reference(directory)
            prefixed = analyzer.bucket_metadata("testnet_federated", metadata)
            empty_prefix = analyzer.bucket_metadata("federated", metadata)
            self.assertTrue(info["available"])
            self.assertTrue(prefixed["known"])
            self.assertEqual(prefixed["category"], "service_registration_and_worker_discovery")
            self.assertIn("federated worker advertisement", prefixed["meaning"])
            self.assertTrue(empty_prefix["known"])

    def test_samples_report_fresh_membership_and_container_resources(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            directory.joinpath("summary.json").write_text(json.dumps({"target_service_peers": 2, "actual_service_peers": 2, "stages": [{"phase": "steady", "service_peer_count": 2, "all_peer_membership_match": True}]}))
            sample = {"timestamp": "2026-10-07T10:00:00Z", "phase": "steady",
                      "stats": [{"Name": "lab-p01", "MemUsage": "25.5MiB / 256MiB", "CPUPerc": "2.00%"}],
                      "membership": {"witness-1": {"federated_nodes": [{"ID": "peer-1"}, {"ID": "peer-2"}]}},
                      "fresh_hosts": {"witness-1": ["peer-1", "peer-2"]},
                      "resources": {"memory_bytes": {"MemAvailable": 8 * 1024**3}}}
            directory.joinpath("samples.jsonl").write_text(json.dumps(sample) + "\n")
            directory.joinpath("observer.jsonl").write_text("")
            result = analyzer.analyze(directory)
            samples = result["membership_and_resources"]
            steady = samples["phase_summaries"]["steady"]
            self.assertEqual(samples["actual_service_peers"], 2)
            self.assertEqual(steady["fresh_membership_count"]["median"], 2)
            self.assertEqual(steady["listed_api_node_count"]["max"], 2)
            self.assertEqual(steady["stage_membership_check"], "pass")
            stats = samples["container_resource_samples"]["steady"]["lab-p01"]
            self.assertEqual(stats["memory_mib"]["max"], 25.5)
            self.assertEqual(stats["cpu_percent"]["median"], 2.0)

    def test_mdns_query_and_response_are_separately_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            rows = [
                {"event": "mdns_packet", "timestamp_utc": "2026-10-07T10:00:00Z", "dns": {"response": False, "questions": [{"name": "_p2p._udp.local", "type": "PTR"}]}},
                {"event": "mdns_packet", "timestamp_utc": "2026-10-07T10:00:01Z", "dns": {"response": True, "answers": [{"name": "peer.local", "type": "A", "data": "172.18.0.3"}]}},
            ]
            directory.joinpath("observer.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n")
            result = analyzer.analyze(directory)
            self.assertEqual(result["counts"]["mdns_queries"], 1)
            self.assertEqual(result["counts"]["mdns_responses"], 1)
            mdns_example = next(row for row in result["snapshot_and_event_examples"] if row["event"] == "mdns_packet")
            self.assertEqual(mdns_example["dns_summary"]["question_examples"][0]["type"], "PTR")

    def test_observer_production_field_names_and_null_mdns_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            block_value = block(3, "production-shape-block", "parent", {
                "net_federated": {"worker-a": encoded_data({"Name": "worker-a", "ID": "peer-a", "LastSeen": "2026-10-07T10:00:00Z"})}
            })
            body = gzip.compress(json.dumps(block_value, separators=(",", ":")).encode())
            rows = [
                {"event": "observer_started", "timestamp_utc": "2026-10-07T10:00:00Z", "observer_peer_id": "observer-a", "dht_enabled": False},
                {"event": "gossip_message", "timestamp_utc": "2026-10-07T10:00:01Z", "observer_peer_id": "observer-a",
                 "received_from_peer_id": "relay-a", "message_sha256": hashlib.sha256(body).hexdigest(),
                 "message_base64": base64.b64encode(body).decode(), "block_index": 3, "block_hash": "production-shape-block",
                 "block_prev_hash": "parent", "block_payload": block_value},
                {"event": "mdns_packet", "timestamp_utc": "2026-10-07T10:00:02Z", "observer_peer_id": "observer-a",
                 "dns": {"response": True, "questions": None, "answers": None, "authority": None, "additional": None}},
                {"event": "peer_connection", "action": "already_connected_at_observer_registration", "state": "already_connected_at_observer_registration",
                 "timestamp_utc": "2026-10-07T10:00:03Z", "observer_peer_id": "observer-a", "remote_peer_id": "peer-existing"},
            ]
            directory.joinpath("message_protocol_reference.json").write_text(json.dumps({
                "lifecycle_categories": [{"category": "registration", "records_or_messages": [{
                    "name": "worker advertisement", "bucket": "<P2PNetworkID>_federated, federated", "kind": "ledger record"
                }]}]
            }))
            directory.joinpath("observer.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n")
            result = analyzer.analyze(directory)
            self.assertEqual(result["counts"]["unique_ledger_block_hashes_per_observer"], 1)
            self.assertEqual(result["counts"]["body_sha256_mismatches"], 0)
            self.assertEqual(result["counts"]["connections_already_open_at_observer_start"], 1)
            self.assertEqual(result["counts"]["mdns_responses"], 1)
            self.assertEqual(result["protocol_reference"]["source_lifecycle_categories"][0]["records_or_messages"][0]["runtime_evidence"], "bucket present in observed snapshots: net_federated")
            discovery = next(row for row in result["lifecycle_categories"] if row["id"] == "discovery_and_connections")
            self.assertEqual(discovery["direct_event_counts"], {"existing_connections": 1, "mdns_responses": 1})
            ledger = next(row for row in result["lifecycle_categories"] if row["id"] == "ledger_replication")
            self.assertEqual(ledger["direct_event_counts"], {"gossip_receptions": 1})
            self.assertEqual(sum(item["observer_events"] for item in result["phase_counts"].values()), result["counts"]["parsed_observer_event_count"])

    def test_mdns_ttl_zero_is_reported_as_dns_withdrawal_not_localai_leave(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            directory.joinpath("phases.jsonl").write_text(json.dumps({"phase": "shutdown", "action": "begin", "timestamp": "2026-10-07T10:00:00Z"}) + "\n")
            event = {"event": "mdns_packet", "timestamp_utc": "2026-10-07T10:00:01Z", "dns": {
                "response": True, "answers": [{"name": "peer.local", "type": "PTR", "TTL": 0, "data": "peer._p2p._udp.local"}],
                "authority": None, "additional": None,
            }}
            directory.joinpath("observer.jsonl").write_text(json.dumps(event) + "\n")
            result = analyzer.analyze(directory)
            observed = result["mdns_ttl_zero_observations"]
            self.assertEqual(observed["count"], 1)
            self.assertEqual(observed["examples"][0]["phase"], "shutdown")
            self.assertEqual(observed["examples"][0]["line"], 1)
            report = directory.joinpath("LIFECYCLE_REPORT.md").read_text()
            self.assertIn("not evidence of a LocalAI ledger leave", report)

    def test_healthcheck_bucket_reset_is_a_snapshot_observation(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            before = block(4, "health-before", "p", {"healthcheck": {
                "peer-1": encoded_data("2026-10-07T10:00:00Z"),
                "peer-2": encoded_data("2026-10-07T10:00:00Z"),
            }})
            after = block(5, "health-reset", "health-before", {"services": {}})
            directory.joinpath("observer.jsonl").write_text("\n".join([
                json.dumps(gossip(before, "2026-10-07T10:00:01Z")),
                json.dumps(gossip(after, "2026-10-07T10:15:00Z")),
            ]) + "\n")
            result = analyzer.analyze(directory)
            transition = next(row for row in result["observed_bucket_absences"] if row["bucket"] == "healthcheck")
            self.assertEqual(transition["removed_record_count"], 2)
            removals = [row for row in result["record_change_catalogue"] if row["bucket"] == "healthcheck" and row["action"] == "removed"]
            self.assertEqual(sum(row["count"] for row in removals), 2)
            report = directory.joinpath("LIFECYCLE_REPORT.md").read_text()
            self.assertIn("not a distinct DELETE packet", report)
            self.assertIn("nonaccepted branch", report)

    def test_connections_and_missing_files_are_safe(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            directory.joinpath("observer.jsonl").write_text("\n".join([
                json.dumps({"event": "peer_connection", "action": "connect", "timestamp": "2026-10-07T10:00:00Z", "peer_id": "peer-1"}),
                json.dumps({"event": "peer_connection", "action": "disconnect", "timestamp": "2026-10-07T10:00:01Z", "peer_id": "peer-1"}),
                json.dumps({"event": "mdns_packet", "timestamp": "2026-10-07T10:00:02Z"}),
            ]) + "\n")
            result = analyzer.analyze(directory)
            self.assertEqual(result["counts"]["peer_connections"], 1)
            self.assertEqual(result["counts"]["peer_disconnections"], 1)
            self.assertTrue(result["input_files"]["observer.jsonl"])
            self.assertFalse(result["input_files"]["samples.jsonl"])


if __name__ == "__main__":
    unittest.main()
