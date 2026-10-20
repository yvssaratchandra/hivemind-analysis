"""Validation never treats missing evidence or unmatched observations as success."""
import gzip
import json

from trainsim.native_validation import audit_native_exit, grouping_metrics, validate_suite


def test_grouping_wait_uses_each_members_local_registration(tmp_path):
    trace = tmp_path/"events.jsonl.gz"
    roster = dict(group_id="G", members=["A", "B"], work_ids=["A:0", "B:0"], formation_time=1.)
    rows = [dict(time=0., kind="work_ready", actor=node,
                 data=dict(work=dict(work_id=f"{node}:0", compute_finish=0.))) for node in ("A", "B")]
    rows += [dict(time=t, kind="group_formed", actor=node, data=dict(roster=roster))
             for t, node in ((1., "A"), (3., "B"))]
    rows += [dict(time=4., kind="group_completed", actor="A", data=dict(group_id="G"))]
    with gzip.open(trace, "wt") as f:
        for row in rows:
            f.write(json.dumps(row)+"\n")
    metrics = grouping_metrics(trace, 10)
    assert metrics["waits"] == [1., 3.]
    assert metrics["median"] == 2.
    assert metrics["group_count"] == metrics["completed_group_count"] == 1


def test_empty_held_out_matrix_cannot_validate(tmp_path):
    pairs = tmp_path/"pairs.json"
    pairs.write_text("[]")
    result = validate_suite(pairs)
    assert not result["passed"]
    assert not result["checks"]["complete_held_out_matrix"]
    assert not result["checks"]["deterministic_fixtures"]


def test_process_audit_rejects_unknown_namespace(tmp_path):
    (tmp_path/"manifest.json").write_text(json.dumps(dict(status="completed", survivors=[12345])))
    result = audit_native_exit(tmp_path)
    assert not result["passed"]
    assert not result["namespace_matched"]
    assert result["checked_pids"] == []
    assert result["unverified_pids"] == [12345]
