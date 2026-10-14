"""Run the frozen raw verifier with an explicit non-participant closure check.

Preserves the original verifier and its report. Only the unknown-peer error for
a valid final trace_closed marker is corrected; all other checks remain intact.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import verify_step26_raw as frozen


def closure_event(path):
    markers, last_is_marker = [], False
    with Path(path).open() as stream:
        for line in stream:
            if not line.strip():
                continue
            last_is_marker = False
            if '"trace_closed"' in line:
                event = json.loads(line)
                if event.get("kind") == "trace_closed":
                    markers.append(event)
                    last_is_marker = True
    if len(markers) != 1 or not last_is_marker:
        raise ValueError("expected exactly one final trace_closed marker")
    event = markers[0]
    detail = event.get("detail", {})
    participants = detail.get("participants", [])
    if (event.get("peer") != "" or event.get("other") != ""
            or detail.get("error") is not None or detail.get("pending") != []
            or len(participants) != 8 or set(participants) != set(frozen.PEER_NAMES)):
        raise ValueError("closure marker has invalid identity, participants, pending work or error")
    return event


def apply_closure_check(result, marker):
    error = f"event {frozen.event_ref(marker)} has unknown peer identity"
    corrected = error in result["errors"]
    result["errors"] = [item for item in result["errors"] if item != error]
    result["closure_marker"] = dict(event_ref=frozen.event_ref(marker),
                                     peer="", participant_event=False, valid=True,
                                     corrected_original_identity_error=corrected)
    result["pass"] = not result["errors"]
    return result


def verify_suite(suite, preservation=None):
    original = frozen.verify_trial

    def verified_trial(suite, trial_dir, job, exports):
        result = original(suite, trial_dir, job, exports)
        try:
            marker = closure_event(suite / trial_dir / "events.jsonl")
            return apply_closure_check(result, marker)
        except (OSError, ValueError, TypeError) as exc:
            result["errors"].append(f"closure contract: {exc}")
            result["pass"] = False
            return result

    frozen.verify_trial = verified_trial
    try:
        report = frozen.verify_suite(suite, preservation)
    finally:
        frozen.verify_trial = original
    report["provenance"]["closure_adapter_sha256"] = frozen.sha256(Path(__file__))
    report["provenance"]["closure_adapter_source"] = str(Path(__file__).resolve())
    report["scope"] += " Empty peer identity is permitted only on a validated final non-participant closure marker."
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--preservation", type=Path)
    args = parser.parse_args()
    suite = args.suite.resolve()
    report = verify_suite(suite, args.preservation)
    output = suite / "raw_verification_corrected.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(dict(pass_=report["pass"], totals=report["totals"],
                          preservation_matches=report["preservation"]["matches"] if report["preservation"] else None,
                          report=str(output))))
    raise SystemExit(0 if report["pass"] else 1)


if __name__ == "__main__":
    main()
