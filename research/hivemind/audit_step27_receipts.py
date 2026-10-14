#!/usr/bin/env python3
"""Read-only correction of Step 27's auxiliary disband receipt linkage.

The frozen analyzer mistakenly compares selected leader expiration to the
follower's request expiration. Match raw client request expirations instead,
and require ACCEPTED and DISBANDED on the same native client join ID.
This does not rewrite frozen inputs, exports, or primary metrics.
"""
import argparse
import hashlib
import json
from pathlib import Path


def ref(event):
    return f"{event['pid']}:{event['event_number']}"


def audit_trial(path):
    raw = path / "events.jsonl"
    events = [json.loads(line) for line in raw.read_text().splitlines()]
    accepts = [e for e in events if e['kind'] == 'unreliable_accept']
    disbands = [e for e in events if e['kind'] == 'unreliable_disband']
    replies = [e for e in events if e['kind'] == 'native_client_reply']
    used = set()
    rows = []
    for disband in disbands:
        detail = disband['detail']
        expiration = detail['request_expiration']
        accept = [e for e in accepts
                  if e['detail']['visit_id'] == detail['visit_id']]
        assert len(accept) == 1
        accept = accept[0]
        next_accept = min((e['monotonic'] for e in accepts
                           if e['peer'] == disband['peer']
                           and e['other'] == disband['other']
                           and e['monotonic'] > accept['monotonic']), default=float('inf'))
        candidates = [e for e in replies
                      if e['peer'] == disband['other']
                      and e['other'] == disband['peer']
                      and e['detail'].get('request_expiration') == expiration
                      and e['detail'].get('code') == 'GROUP_DISBANDED'
                      and disband['monotonic'] <= e['monotonic'] < next_accept
                      and ref(e) not in used]
        assert len(candidates) <= 1, 'ambiguous client receipt'
        receipt = candidates[0] if candidates else None
        handshake = []
        if receipt:
            handshake = [e for e in replies
                         if e['pid'] == receipt['pid']
                         and e['peer'] == receipt['peer']
                         and e['other'] == receipt['other']
                         and e['detail'].get('join_id') == receipt['detail']['join_id']
                         and e['detail'].get('request_expiration') == expiration
                         and e['detail'].get('code') == 'ACCEPTED'
                         and accept['monotonic'] <= e['monotonic'] <= receipt['monotonic']]
            assert len(handshake) == 1, 'missing/ambiguous matching ACCEPTED'
            used.add(ref(receipt))
        rows.append(dict(visit_id=detail['visit_id'],
                         request_expiration=expiration,
                         disband_event_ref=ref(disband),
                         client_accepted_event_ref=ref(handshake[0]) if handshake else None,
                         client_disband_event_ref=ref(receipt) if receipt else None,
                         client_join_id=receipt['detail']['join_id'] if receipt else None))
    return dict(trial=path.name, events_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),
                disbands=len(rows), matched_client_receipts=len(used), rows=rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', type=Path, required=True)
    args = parser.parse_args()
    trials = [audit_trial(p) for p in sorted(args.suite.glob('unreliable_*_s*'))
              if p.is_dir()]
    report = dict(source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  scope='Auxiliary receipt correction only; primary exports unchanged',
                  disbands=sum(t['disbands'] for t in trials),
                  matched_client_receipts=sum(t['matched_client_receipts'] for t in trials),
                  trials=trials)
    (args.suite / 'receipt_audit.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k != 'trials'}))
    for t in trials:
        print(t['trial'], t['matched_client_receipts'], '/', t['disbands'])


if __name__ == '__main__':
    main()
