import gzip
import json

from trainsim.protocols.agora.accounting import Accounting, replay_trace
from trainsim.protocols.agora.report import render


def event(time, kind, actor="", **data):
    return {"time": float(time), "kind": kind, "actor": actor, "data": data}


def snapshot(accounting, *, time=11, origin="N0004", epoch=0, layout="layout-a", partition=0,
             selected=12, components=("tensor-0", "tensor-1"), component_cardinalities=None):
    if component_cardinalities is None:
        base, remainder = divmod(selected, len(components))
        component_cardinalities = {
            component: base + (index < remainder)
            for index, component in enumerate(components)
        }
    accounting.consume(event(time, "agora_snapshot_created", origin, origin=origin, epoch=epoch,
                             layout=layout, partition=partition, selected_cardinality=selected,
                             expected_component_ids=list(components),
                             expected_component_cardinalities=component_cardinalities))


def application(accounting, *, time=12, origin="N0004", recipient="N0000", epoch=0,
                layout="layout-a", partition=0, selected=12, applied=12,
                status="complete", fallback=False,
                applied_component_ids=None,
                applied_component_cardinalities=None, **extra):
    component_mode = _has_any(extra, "component_id", "tensor_index", "tensor_id", "part_index")
    if applied_component_ids is None:
        applied_component_ids = () if component_mode else ("tensor-0", "tensor-1")
    if applied_component_cardinalities is None:
        ids = list(dict.fromkeys(applied_component_ids))
        if ids:
            base, remainder = divmod(applied, len(ids))
            applied_component_cardinalities = {
                component: base + (index < remainder)
                for index, component in enumerate(ids)
            }
    accounting.consume(event(time, "agora_sparse_application", recipient, origin=origin,
                             recipient=recipient, epoch=epoch, layout=layout, partition=partition,
                             selected_cardinality=selected, applied_cardinality=applied,
                             applied_component_ids=list(applied_component_ids),
                             applied_component_cardinalities=applied_component_cardinalities,
                             status=status, fallback=fallback, **extra))


def _has_any(mapping, *keys):
    return any(key in mapping for key in keys)


def test_credit_is_once_per_snapshot_unit_and_requires_matching_local_snapshot():
    accounting = Accounting({"population": 2, "target_accepted_contributions": 1,
                             "observation_seconds": 5})
    accounting.consume(event(10, "measurement_started"))
    snapshot(accounting)
    # Wrong layout, epoch, and partition attempts for the same origin must be
    # rejected before any valid receipt exists.
    application(accounting, layout="wrong-layout")
    application(accounting, epoch=1)
    application(accounting, partition=1)
    # A full aggregate count cannot hide incomplete part coverage.
    application(accounting, selected=12, applied=12, applied_component_ids=("tensor-0",))
    application(accounting, selected=12, applied=12,
                applied_component_ids=("tensor-0", "tensor-0", "tensor-1"))
    application(accounting, selected=12, applied=12,
                applied_component_cardinalities={"tensor-0": 7, "tensor-1": 5})
    assert accounting.summary()["accepted_work"] == 0

    application(accounting)
    application(accounting, time=13, recipient="N0001")  # another reducer cannot double count
    assert accounting.summary()["accepted_work"] == 1
    assert accounting.target_time == 2


def test_partial_fallback_local_and_cross_recipient_coverage_get_no_credit():
    accounting = Accounting()
    # Two partial recipients cannot be combined into a single accepted unit.
    snapshot(accounting, origin="N0004", selected=12)
    application(accounting, recipient="N0000", selected=12, applied=6, status="partial")
    application(accounting, recipient="N0001", selected=12, applied=6, status="partial")

    snapshot(accounting, origin="N0005", selected=12)
    application(accounting, origin="N0005", recipient="N0000", fallback=True)

    snapshot(accounting, origin="N0006", selected=12)
    application(accounting, origin="N0006", recipient="N0006")
    assert accounting.summary()["accepted_work"] == 0
    assert accounting.summary()["outcomes"].get("partial", 0) == 2


def test_component_receipts_need_explicit_complete_component_set():
    accounting = Accounting()
    snapshot(accounting, origin="N0004", selected=12)
    # A correct subset with matching totals still cannot prove full coverage.
    application(accounting, origin="N0004", recipient="N0000", selected=6, applied=6,
                component_id="tensor-0")
    assert accounting.summary()["accepted_work"] == 0

    # Replay the complete snapshot with an explicit expected set. Components
    # are counted once by their stable IDs and cannot be unioned across peers.
    accounting = Accounting()
    snapshot(accounting, origin="N0004", selected=12,
             components=("tensor-0", "tensor-1"))
    application(accounting, origin="N0004", recipient="N0000", selected=6, applied=6,
                component_id="tensor-0", expected_component_ids=["tensor-0", "tensor-1"],
                snapshot_selected_cardinality=12)
    assert accounting.summary()["accepted_work"] == 0
    application(accounting, origin="N0004", recipient="N0000", selected=6, applied=6,
                component_id="tensor-1", expected_component_ids=["tensor-0", "tensor-1"],
                snapshot_selected_cardinality=12)
    assert accounting.summary()["accepted_work"] == 1


def test_conflicting_duplicate_component_cannot_inflate_coverage():
    accounting = Accounting()
    snapshot(accounting, origin="N0004", selected=12)
    application(accounting, origin="N0004", recipient="N0000", selected=6, applied=6,
                component_id="tensor-0")
    # The same ID with altered values is a conflict, not extra coverage.
    application(accounting, origin="N0004", recipient="N0000", selected=12, applied=12,
                component_id="tensor-0")
    application(accounting, origin="N0004", recipient="N0000", selected=6, applied=6,
                component_id="tensor-1")
    assert accounting.summary()["accepted_work"] == 0


def test_measurement_relative_target_and_half_open_observation_boundary():
    accounting = Accounting({"target_accepted_contributions": 1, "observation_seconds": 5})
    accounting.consume(event(100, "measurement_started"))
    snapshot(accounting, time=101)
    application(accounting, time=102)
    snapshot(accounting, time=103, origin="N0005", epoch=1)
    application(accounting, time=105, origin="N0005", epoch=1)
    accounting.consume(event(105, "observation_boundary"))
    summary = accounting.summary()
    assert summary["target_time"] == 2
    assert summary["accepted_work"] == 2
    assert summary["accepted_work_at_observation"] == 1
    assert summary["last_time"] == 5


def test_credit_key_includes_partition_and_epoch_local_events_ignore_registry_reads():
    accounting = Accounting()
    snapshot(accounting, origin="N0004", epoch=0, partition=0)
    snapshot(accounting, origin="N0004", epoch=0, partition=1)
    # Only named, epoch-bearing local protocol events feed the dashboard;
    # lower-level registry activity remains available in the raw event trace.
    accounting.consume(event(10, "agora_registry_lookup_snapshot", "N0004",
                             key="stage/body/", peer_ids=["p0", "p1"]))
    accounting.consume(event(10, "agora_registry_lookup_snapshot", "N0004", epoch=0,
                             key="stage/body/", peer_ids=["p0", "p1"]))
    accounting.consume(event(10, "agora_registry_expected_count", "N0004", epoch=0,
                             expected_count=2))
    accounting.consume(event(10, "agora_registry_local_roster", "N0004", epoch=0,
                             ordered_node_ids=["N0004", "N0000"]))
    application(accounting, epoch=0, partition=0)
    application(accounting, epoch=0, partition=1)
    summary = accounting.summary()
    assert summary["accepted_work"] == 2
    assert "?" not in summary["epochs"]
    assert summary["epochs"]["0"]["lookup_count"] == 0
    assert summary["epochs"]["0"]["expected_observations"] == []
    assert summary["epochs"]["0"]["roster_observations"] == []


def test_snapshot_and_receipt_must_match_component_cardinalities():
    accounting = Accounting()
    snapshot(accounting, origin="N0004", selected=12,
             component_cardinalities={"tensor-0": 7, "tensor-1": 5})
    application(accounting, origin="N0004", recipient="N0000", selected=12, applied=12,
                applied_component_cardinalities={"tensor-0": 6, "tensor-1": 6})
    assert accounting.summary()["accepted_work"] == 0


def test_averaging_attempts_and_timeouts_keep_separate_units():
    accounting = Accounting()
    accounting.consume(event(1, "agora_aggregate_finished", "N0000", status="complete"))
    accounting.consume(event(2, "agora_aggregate_finished", "N0001", status="failed"))
    accounting.consume(event(3, "agora_network_timeout", "N0002"))
    summary = accounting.summary()
    assert summary["averaging_outcomes"] == {"complete": 1, "failed": 1}
    assert summary["timeouts"] == {"events": 1}
    assert summary["outcomes"].get("failed", 0) == 0


def test_rosters_roles_wait_and_raw_trace_replay_ignore_derived_credit(tmp_path):
    trace = tmp_path / "events.jsonl.gz"
    rows = [
        event(10, "measurement_started"),
        event(10, "agora_role", "N0000", role="NODE", weight=1),
        event(10, "agora_role", "N0004", role="CLIENT"),
        event(11, "agora_step_advertised", "N0004", epoch=0, key="avg_s0"),
        event(12, "agora_expected_count", "N0004", epoch=0, expected_count=2),
        event(13, "agora_lookup", "N0004", epoch=0, peer_ids=["p0", "p1"]),
        event(14, "agora_local_roster", "N0004", epoch=0, node_id="N0004",
              ordered_node_ids=["N0000", "N0004"], roster_fingerprint="r1",
              expected=2, observed=2, reason="expected_count"),
        event(15, "agora_local_roster", "N0000", epoch=0, node_id="N0000",
              ordered_node_ids=["N0004", "N0000"], roster_fingerprint="r2",
              expected=2, observed=2, reason="expected_count"),
        event(15, "agora_local_roster", "N0002", epoch=0, node_id="N0002",
              ordered_node_ids=["N0000", "N0004"], roster_fingerprint="r1",
              expected=2, observed=2, reason="expected_count"),
    ]
    recorder = Accounting({"population": 2, "observation_seconds": 10})
    with gzip.open(trace, "wt", encoding="utf-8") as stream:
        for row in rows:
            recorder.consume(row)
            stream.write(json.dumps(row) + "\n")
        # Snapshot/application source rows must also be present in replay.
        for row in (
            event(16, "agora_snapshot_created", "N0004", origin="N0004", epoch=0,
                  layout="layout-a", partition=0, selected_cardinality=12,
                  expected_component_ids=["tensor-0", "tensor-1"],
                  expected_component_cardinalities={"tensor-0": 6, "tensor-1": 6}),
            event(17, "agora_sparse_application", "N0000", origin="N0004", recipient="N0000",
                  epoch=0, layout="layout-a", partition=0, selected_cardinality=12,
                  applied_cardinality=12, applied_component_ids=["tensor-0", "tensor-1"],
                  applied_component_cardinalities={"tensor-0": 6, "tensor-1": 6},
                  status="complete", fallback=False, weight=1),
            event(17.5, "agora_sparse_application", "N0001", origin="N0004", recipient="N0001",
                  epoch=0, layout="layout-a", partition=0, selected_cardinality=12,
                  applied_cardinality=12, applied_component_ids=["tensor-0", "tensor-1"],
                  applied_component_cardinalities={"tensor-0": 6, "tensor-1": 6},
                  status="complete", fallback=False, weight=1),
            event(18, "work_accepted", "N0004", amount=999),
        ):
            recorder.consume(row)
            stream.write(json.dumps(row) + "\n")
    summary = replay_trace(trace, {"population": 2, "observation_seconds": 10})
    online = recorder.summary()
    assert summary["accepted_work"] == 1
    assert summary["accepted_work"] == online["accepted_work"]
    assert summary["role_counts"] == {"contributor": 1, "reducer": 1}
    reducer = next(node for node in summary["nodes"] if node["node"] == "N0000")
    assert reducer["mean_reducer_weight"] == 1
    assert summary["ad_to_roster_wait_seconds"]["mean"] == 3
    assert summary["epochs"]["0"]["divergent_roster_count"] == 2
    assert summary["epochs"]["0"]["expected_min"] == 2
    assert summary["epochs"]["0"]["observed_min"] == 2
    assert summary["participant_counts"]["rostered_nodes"] == 2
    assert summary["participant_counts"]["roster_observer_nodes"] == 3
    assert summary["participant_counts"]["incorporating_recipients"] == 2
    assert summary["participant_counts"]["fully_applied_snapshot_receipts"] == 2


def test_report_writes_self_contained_dashboard_and_scope_note(tmp_path):
    rows = [event(5, "measurement_started"),
            event(5, "agora_role", "N0000", role="NODE", weight=1),
            event(5, "agora_role", "N0001", role="CLIENT"),
            event(6, "agora_step_advertised", "N0001", epoch=0),
            event(7, "agora_local_roster", "N0001", epoch=0, node_id="N0001",
                  roster=["N0000", "N0001"], expected=2, observed=2,
                  roster_fingerprint="r0")]
    with gzip.open(tmp_path / "events.jsonl.gz", "wt", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")
    (tmp_path / "resolved_config.json").write_text(json.dumps({"population": 2, "seed": 1}))
    (tmp_path / "summary.json").write_text(json.dumps({"status": "completed", "seed": 1}))
    (tmp_path / "manifest.json").write_text(json.dumps({"assumptions": ["manifest assumption"]}))
    html_path, markdown_path = render(tmp_path)
    page = html_path.read_text()
    report = markdown_path.read_text()
    assert "DASHBOARD" not in html_path.name or html_path.name == "DASHBOARD.html"
    assert "Local work is represented by delays" in page
    assert "manifest assumption" in page
    assert "epochSelect" in page and "dashboard-data" in page
    assert "<script src=" not in page and "https://" not in page
    assert "Credit unit: one unique" in report
