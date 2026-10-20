"""Self-contained one-page dashboard for an Agora simulation run."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any, Mapping

from .accounting import replay_trace


def _read_json(path: Path, default: Any) -> Any:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default
    return value


def _trace_path(run_dir: Path) -> Path:
    for name in ("events.jsonl.gz", "events.jsonl", "trace.jsonl.gz", "trace.jsonl"):
        candidate = run_dir / name
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"No JSONL event trace found in {run_dir}")


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _escape(value: Any) -> str:
    return html.escape(_fmt(value), quote=True)


def _json_for_script(value: Any) -> str:
    # Escaping '<' prevents trace strings from closing the data script element.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).replace("<", "\\u003c")


def _assumptions(config: Mapping[str, Any], metadata: Mapping[str, Any]) -> list[str]:
    raw = metadata.get("assumptions", [])
    assumptions = [str(item) for item in raw] if isinstance(raw, list) else []
    fixed = [
        "Synthetic timing and resource capacities; no Internet deployment measurements.",
        "One simulated stage; stage placement, Authorizer behavior, and full-pipeline effects are not modeled.",
        "Local work is represented by delays; this run does not establish model convergence.",
    ]
    for item in fixed:
        if item not in assumptions:
            assumptions.append(item)
    if config.get("attack") or config.get("attacks"):
        assumptions.append("The saved configuration declares attack settings; inspect the run config before interpreting outcomes.")
    return assumptions


def _markdown(data: Mapping[str, Any], config: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    status = metadata.get("status", "unknown")
    roles = data.get("role_counts", {})
    counts = data.get("participant_counts", {})
    waits = data.get("ad_to_roster_wait_seconds", {})
    outcomes = data.get("outcomes", {})
    average = data.get("averaging_outcomes", {})
    protocol_failures = data.get("protocol_failures", {}).get("events", 0)
    timeouts = data.get("timeouts", {}).get("events", 0)
    divergent_epochs = [epoch for epoch, row in data.get("epochs", {}).items()
                        if row.get("divergent_roster_count", 0) > 1]
    roster_comparable = any(len(row.get("roster_observations", [])) > 1
                            for row in data.get("epochs", {}).values())
    if (outcomes.get("failed", 0) or outcomes.get("partial", 0) or outcomes.get("skipped", 0)
            or average.get("failed", 0) or average.get("partial", 0) or average.get("incomplete", 0)
            or protocol_failures or timeouts):
        outcome_note = "At least one unsuccessful or partial outcome was recorded."
    else:
        outcome_note = "No failures, partial applications, or skipped opportunities were recorded."
    if divergent_epochs:
        outcome_note += f" Divergent local rosters appeared in {len(divergent_epochs)} epoch(s)."
    elif roster_comparable:
        outcome_note += " No divergent local rosters were observed."
    else:
        outcome_note += " Roster disagreement could not be assessed because local roster comparisons were absent."
    lines = [
        "# Agora single-stage simulation",
        "",
        f"Run status: **{status}**. Seed: **{metadata.get('seed', config.get('seed', 'unknown'))}**. "
        f"Population: **{data.get('population', config.get('population', 'unknown'))}**.",
        "",
        f"Accepted sparse snapshot contributions: **{data.get('accepted_sparse_snapshot_contributions', 0)}**. "
        f"Observation-window total: **{data.get('accepted_work_at_observation')}**. "
        f"Target time: **{_fmt(data.get('target_time'))} s**; first accepted contribution: **{_fmt(data.get('first_accept_time'))} s**.",
        "",
        f"Roles: **{roles.get('reducer', 0)} reducers**, **{roles.get('contributor', 0)} contributors**. "
        "Contributors have simulated contribution weight 1 and reducer allocation 0; reducers contribute weight 1.",
        f"Nodes advertised: {counts.get('advertised_nodes', 0)}; observed in rosters: {counts.get('rostered_nodes', 0)}.",
        "",
        f"Ad-to-roster wait: n={waits.get('count', 0)}, mean={_fmt(waits.get('mean'))} s, median={_fmt(waits.get('median'))} s, max={_fmt(waits.get('max'))} s.",
        "",
        "Credit unit: one unique `(origin, epoch, tensor layout, sparse partition)` fully applied at one remote recipient. Partial and fallback applications receive no credit; coverage is never combined across recipients or epochs.",
        "",
        "## Scope",
        "",
    ]
    lines.extend(f"- {item}" for item in _assumptions(config, metadata))
    lines.extend([
        "",
        "## Epoch results",
        "",
        "| Epoch | Expected peers | Observed peers | Roster variants | Accepted contributions |",
        "|---:|---:|---:|---:|---:|",
    ])
    for epoch, row in data.get("epochs", {}).items():
        expected = "—" if row.get("expected_min") is None else f"{row['expected_min']}–{row['expected_max']}"
        observed = "—" if row.get("observed_min") is None else f"{row['observed_min']}–{row['observed_max']}"
        lines.append(f"| {epoch} | {expected} | {observed} | {row.get('divergent_roster_count', 0)} | {len(row.get('incorporations', []))} |")
    if not data.get("epochs"):
        lines.append("| — | — | — | — | 0 |")
    lines.extend([
        "",
        "## Outcomes",
        "",
        f"Averaging attempts: complete={average.get('complete', 0)}, partial={average.get('partial', 0) + average.get('incomplete', 0)}, failed={average.get('failed', 0) + average.get('error', 0) + average.get('rejected', 0)}.",
        f"Per-origin/recipient application receipts: accepted units={data.get('accepted_sparse_snapshot_contributions', 0)}, fully applied receipts={data.get('participant_counts', {}).get('fully_applied_snapshot_receipts', 0)}, partial={outcomes.get('partial', 0)}, failed={outcomes.get('failed', 0)}, fallback={outcomes.get('fallback', 0)}.",
        f"Other protocol failure events: {protocol_failures}; timeout events: {timeouts}; skipped opportunities: {outcomes.get('skipped', 0)}.",
        outcome_note,
        "",
        "The detailed, interactive event walkthrough is in `DASHBOARD.html`. Raw trace events and resolved configuration remain the evidence source.",
        "",
    ])
    return "\n".join(lines)


def _html(data: Mapping[str, Any], config: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    roles = data.get("role_counts", {})
    waits = data.get("ad_to_roster_wait_seconds", {})
    epoch_options = "".join(
        f'<option value="{_escape(epoch)}">Epoch {_escape(epoch)}</option>'
        for epoch in data.get("epochs", {})
    )
    assumptions = "".join(f"<li>{_escape(item)}</li>" for item in _assumptions(config, metadata))
    json_data = _json_for_script(data)
    json_config = _json_for_script(config)
    status = _escape(metadata.get("status", "unknown"))
    seed = _escape(metadata.get("seed", config.get("seed", "unknown")))
    population = _escape(data.get("population", config.get("population", "unknown")))
    accepted = _escape(data.get("accepted_sparse_snapshot_contributions", 0))
    target = _escape(data.get("target_time"))
    at_observation = _escape(data.get("accepted_work_at_observation"))
    reducer_n = _escape(roles.get("reducer", 0))
    contributor_n = _escape(roles.get("contributor", 0))
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Agora single-stage simulation</title>
<style>
:root{{--ink:#17212b;--muted:#566473;--line:#d8e0e7;--paper:#fff;--wash:#f3f6f8;--blue:#1769aa;--green:#247a52;--orange:#b86113;--red:#a73f3f}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--wash);color:var(--ink);font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif}}
main{{max-width:1320px;margin:0 auto;padding:24px}}header{{display:flex;justify-content:space-between;align-items:flex-start;gap:18px;margin-bottom:16px}}
h1{{font-size:25px;margin:0 0 5px}}h2{{font-size:16px;margin:0 0 12px}}p{{margin:4px 0;color:var(--muted)}}.badge{{border:1px solid var(--line);border-radius:999px;background:white;padding:6px 12px;font-weight:650;white-space:nowrap}}
.cards{{display:grid;grid-template-columns:repeat(5,minmax(120px,1fr));gap:10px;margin:14px 0}}
.card,.panel{{background:var(--paper);border:1px solid var(--line);border-radius:10px;padding:14px;box-shadow:0 1px 2px #17212b0b}}
.label{{color:var(--muted);font-size:12px}}.value{{font-size:23px;font-weight:700;margin-top:4px}}.sub{{color:var(--muted);font-size:12px}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:12px 0}}.panel{{min-width:0}}.full{{grid-column:1/-1}}
.chart{{width:100%;height:205px;display:block}}.tablewrap{{overflow:auto;max-height:320px}}table{{border-collapse:collapse;width:100%;font-size:12px}}th,td{{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);vertical-align:top}}th{{color:var(--muted);font-weight:650;position:sticky;top:0;background:white}}
.pill{{display:inline-block;border-radius:999px;padding:2px 8px;background:#edf3f8;color:#24455f;font-size:11px}}.warn{{color:var(--orange)}}.good{{color:var(--green)}}.small{{font-size:12px;color:var(--muted)}}
select{{font:inherit;padding:6px 8px;border:1px solid var(--line);border-radius:6px;background:#fff}}.timeline{{max-height:350px;overflow:auto}}
.assumptions{{display:grid;grid-template-columns:1fr 1fr;gap:4px 20px;padding-left:18px;margin:0;color:var(--muted)}}.legend{{display:flex;gap:14px;font-size:12px;color:var(--muted)}}.dot{{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px}}
@media(max-width:900px){{main{{padding:14px}}.cards{{grid-template-columns:repeat(2,1fr)}}.grid2{{grid-template-columns:1fr}}.full{{grid-column:auto}}header{{display:block}}.badge{{display:inline-block;margin-top:10px}}.assumptions{{grid-template-columns:1fr}}}}
</style></head><body><main>
<header><div><h1>Agora single-stage simulation</h1><p>Honest baseline trace replay · time is relative to measurement start</p></div><span class="badge">{status} · seed {seed} · {population} nodes</span></header>
<div class="cards">
 <div class="card"><div class="label">Accepted sparse snapshot contributions</div><div class="value">{accepted}</div><div class="sub">Unique origin × epoch × layout × partition</div></div>
 <div class="card"><div class="label">Accepted by observation window</div><div class="value">{at_observation}</div><div class="sub">Boundary is half-open</div></div>
 <div class="card"><div class="label">Time to target</div><div class="value">{target}<span class="sub"> s</span></div><div class="sub">Elapsed since measurement start</div></div>
 <div class="card"><div class="label">Reducers</div><div class="value">{reducer_n}</div><div class="sub">Expected reducer averaging weight: 1</div></div>
 <div class="card"><div class="label">Contributors</div><div class="value">{contributor_n}</div><div class="sub">Contribution weight 1 · reducer allocation 0</div></div>
</div>
<section class="panel"><h2>Work curve</h2><p>Count of unique sparse snapshot contributions fully applied at a remote recipient.</p><svg id="workcurve" class="chart" role="img" aria-label="Accepted sparse snapshot contributions over elapsed seconds"></svg></section>
<div class="grid2">
 <section class="panel"><h2>Matchmaking wait</h2><p>Local advertisement to that node’s roster formation. Independent local views may finish at different times.</p><div id="waitSummary" class="value">{_escape(waits.get('mean'))}<span class="sub"> s mean</span></div><div class="sub">n={_escape(waits.get('count'))} · median {_escape(waits.get('median'))} s · max {_escape(waits.get('max'))} s</div><svg id="waitchart" class="chart" role="img" aria-label="Advertisement to roster wait by epoch"></svg></section>
 <section class="panel"><h2>Expected versus observed peers</h2><p>Peer counts returned by discovery polls, including incomplete views before roster formation. Final rosters are shown separately.</p><div class="tablewrap"><table><thead><tr><th>Epoch</th><th>Expected</th><th>Observed</th><th>Local views</th></tr></thead><tbody id="expectedTable"></tbody></table></div></section>
 <section class="panel"><h2>Roster agreement</h2><p>Number of distinct ordered roster fingerprints seen by nodes in each epoch.</p><div class="tablewrap"><table><thead><tr><th>Epoch</th><th>Variants</th><th>Fingerprint</th><th>Observers</th></tr></thead><tbody id="rosterTable"></tbody></table></div></section>
 <section class="panel"><h2>Participation and outcomes</h2><p>Role counts, accepted origins, recipient coverage, partial applications, failures, and skipped opportunities.</p><div class="tablewrap"><table><thead><tr><th>Node</th><th>Role</th><th>Ads</th><th>Rosters</th><th>Snapshots</th><th>Accepted</th><th>Partial</th><th>Failed</th><th>Skipped</th><th>Mean reducer weight</th></tr></thead><tbody id="nodeTable"></tbody></table></div><p id="outcomeText" class="small"></p></section>
 <section class="panel full"><h2>Epoch walkthrough</h2><p>Follow one node’s local advertisements, expected-population queries, lookups, roster observation, and related application outcomes. Other nodes’ raw events remain in the trace.</p><label for="epochSelect">Epoch </label><select id="epochSelect">{epoch_options}</select> <label for="nodeSelect">Node </label><select id="nodeSelect"></select><div class="tablewrap timeline"><table><thead><tr><th>Elapsed s</th><th>Event</th><th>Node / recipient</th><th>Details</th></tr></thead><tbody id="timelineTable"></tbody></table></div><p id="emptyTimeline" class="small"></p></section>
 <section class="panel full"><h2>Interpretation limits</h2><ul class="assumptions">{assumptions}</ul><p class="small">Credit requires complete selected-cardinality application, no fallback, a different recipient, and a matching local snapshot declaration. A roster variant is an observed local view, not a network-wide canonical group.</p></section>
</div>
</main>
<script type="application/json" id="dashboard-data">{json_data}</script>
<script type="application/json" id="dashboard-config">{json_config}</script>
<script>
const D=JSON.parse(document.getElementById('dashboard-data').textContent);
const esc=s=>String(s??'—').replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
const fmt=(x,n=2)=>x==null?'—':(typeof x==='number'?x.toFixed(n):String(x));
const ns=D.epochs||{{}}; const keys=Object.keys(ns);
function svgBase(el,w=800,h=205){{el.setAttribute('viewBox',`0 0 ${{w}} ${{h}}`);return {{w,h}}}}
function drawCurve(){{const el=document.getElementById('workcurve'),{{w,h}}=svgBase(el);const a=D.accepted_curve||[];const pad=42;const boundary=(D.observation_boundary!=null&&D.measurement_start!=null)?D.observation_boundary-D.measurement_start:0;const xmax=Math.max(1,D.last_time||0,boundary,...a.map(x=>x.time));const ymax=Math.max(1,...a.map(x=>x.accepted_sparse_snapshot_contributions));const x=t=>pad+(w-pad-14)*t/xmax;const y=v=>h-pad-(h-pad-14)*v/ymax;let s=`<line x1="${{pad}}" y1="${{h-pad}}" x2="${{w-8}}" y2="${{h-pad}}" stroke="#9aa8b5"/><line x1="${{pad}}" y1="12" x2="${{pad}}" y2="${{h-pad}}" stroke="#9aa8b5"/>`;for(let i=0;i<=4;i++){{const yy=y(ymax*i/4);s+=`<line x1="${{pad}}" y1="${{yy}}" x2="${{w-8}}" y2="${{yy}}" stroke="#e5eaee"/><text x="${{pad-7}}" y="${{yy+4}}" text-anchor="end" font-size="10" fill="#566473">${{Math.round(ymax*i/4)}}</text>`;const tick=x(xmax*i/4);s+=`<text x="${{tick}}" y="${{h-9}}" text-anchor="middle" font-size="10" fill="#566473">${{Math.round(xmax*i/4)}}</text>`}}let points=`${{x(0)}},${{y(0)}} `+a.map(p=>`${{x(p.time)}},${{y(p.accepted_sparse_snapshot_contributions)}}`).join(' ');if(a.length){{points+=` ${{xmax}},${{y(a[a.length-1].accepted_sparse_snapshot_contributions)}}`;s+=`<polyline points="${{points}}" fill="none" stroke="#1769aa" stroke-width="3"/>`;}}if(boundary>0){{s+=`<line x1="${{x(boundary)}}" y1="12" x2="${{x(boundary)}}" y2="${{h-pad}}" stroke="#b86113" stroke-dasharray="5 4"/><text x="${{x(boundary)+4}}" y="18" font-size="10" fill="#b86113">observation</text>`}}s+=`<text x="${{w/2}}" y="${{h-2}}" text-anchor="middle" font-size="11" fill="#566473">Elapsed seconds from measurement start</text><text transform="translate(12 ${{h/2}}) rotate(-90)" text-anchor="middle" font-size="11" fill="#566473">Accepted contributions</text>`;el.innerHTML=s}}
function drawWait(){{const el=document.getElementById('waitchart'),{{w,h}}=svgBase(el);const rows=keys.map(k=>{{const waits=(ns[k].roster_observations||[]).map(r=>r.wait_seconds).filter(x=>x!=null);return {{k,mean:waits.length?waits.reduce((a,b)=>a+b,0)/waits.length:0,n:waits.length}}}});const pad=36,max=Math.max(1,...rows.map(x=>x.mean)),bw=Math.max(8,(w-pad-12)/Math.max(1,rows.length)-9);let s=`<line x1="${{pad}}" y1="${{h-28}}" x2="${{w-8}}" y2="${{h-28}}" stroke="#9aa8b5"/>`;rows.forEach((r,i)=>{{const bh=(h-48)*r.mean/max,x=pad+i*(bw+9),y=h-28-bh;s+=`<rect x="${{x}}" y="${{y}}" width="${{bw}}" height="${{bh}}" rx="3" fill="#247a52"><title>Epoch ${{esc(r.k)}}, mean ${{fmt(r.mean)}} s, n=${{r.n}}</title></rect><text x="${{x+bw/2}}" y="${{h-11}}" text-anchor="middle" font-size="10" fill="#566473">${{esc(r.k)}}</text>`}});s+=`<text x="8" y="14" font-size="10" fill="#566473">mean seconds · hover for local count</text>`;el.innerHTML=s}}
function minmax(a,b){{return a==null?'—':(a===b?String(a):`${{a}}–${{b}}`)}}
function tables(){{const expected=document.getElementById('expectedTable'),rosters=document.getElementById('rosterTable');keys.forEach(k=>{{const e=ns[k];const tr=document.createElement('tr');tr.innerHTML=`<td>${{esc(k)}}</td><td>${{minmax(e.expected_min,e.expected_max)}}</td><td>${{minmax(e.observed_min,e.observed_max)}}</td><td>${{(e.roster_observations||[]).length}}</td>`;expected.appendChild(tr);(e.roster_variants||[]).forEach(v=>{{const rr=document.createElement('tr');rr.innerHTML=`<td>${{esc(k)}}</td><td>${{e.divergent_roster_count}}</td><td><code>${{esc(v.fingerprint)}}</code></td><td>${{esc((v.observers||[]).join(', '))}}</td>`;rosters.appendChild(rr)}})}});const nt=document.getElementById('nodeTable');(D.nodes||[]).forEach(n=>{{const tr=document.createElement('tr');tr.innerHTML=`<td>${{esc(n.node)}}</td><td><span class="pill">${{esc(n.role)}}</span></td><td>${{n.advertisements}}</td><td>${{n.rosters_observed}}</td><td>${{n.snapshots_offered}}</td><td>${{n.accepted_sparse_snapshot_contributions}}</td><td>${{n.partial}}</td><td>${{n.failed}}</td><td>${{n.skipped}}</td><td>${{fmt(n.mean_reducer_weight)}}</td>`;nt.appendChild(tr)}});const o=D.outcomes||{{}},a=D.averaging_outcomes||{{}},div=keys.filter(k=>ns[k].divergent_roster_count>1),timeouts=D.timeouts?.events||0,otherfails=D.protocol_failures?.events||0;let bad=(o.failed||o.partial||o.skipped||a.failed||a.partial||a.incomplete||timeouts||otherfails);let note=bad?'At least one unsuccessful, partial, timed-out, or skipped outcome was recorded.':'No failures, partial applications, timeouts, or skipped opportunities were recorded.';if(div.length)note+=` Divergent local rosters appeared in ${{div.length}} epoch(s).`;else if(keys.some(k=>(ns[k].roster_observations||[]).length>1))note+=' No divergent local rosters were observed.';else note+=' Roster disagreement could not be assessed because local roster comparisons were absent.';document.getElementById('outcomeText').textContent=`Averaging attempts complete/partial/failed: ${{a.complete||0}} / ${{(a.partial||0)+(a.incomplete||0)}} / ${{(a.failed||0)+(a.error||0)+(a.rejected||0)}}. Application receipts accepted/partial/failed/fallback: ${{D.participant_counts?.fully_applied_snapshot_receipts||0}} / ${{o.partial||0}} / ${{o.failed||0}} / ${{o.fallback||0}}. Timeouts: ${{timeouts}} · other protocol failures: ${{otherfails}} · skipped: ${{o.skipped||0}}. Advertised nodes: ${{D.participant_counts?.advertised_nodes||0}} · roster members: ${{D.participant_counts?.rostered_nodes||0}} · roster observers: ${{D.participant_counts?.roster_observer_nodes||0}} · incorporating recipients: ${{D.participant_counts?.incorporating_recipients||0}}. ${{note}}`}}
const labels={{advertisement:'step advertisement',expected_count:'expected-count query',lookup:'peer lookup',snapshot:'snapshot offered',roster:'local roster',application:'application result',incorporation:'accepted contribution'}};
const nodeSelect=document.getElementById('nodeSelect');const walkNodes=[...new Set(Object.values(ns).flatMap(e=>(e.roster_observations||[]).map(r=>r.node)).filter(Boolean))].sort();(walkNodes.length?walkNodes:(D.nodes||[]).map(n=>n.node)).forEach(n=>{{const o=document.createElement('option');o.value=n;o.textContent=n;nodeSelect.appendChild(o)}});
function showTimeline(){{const sel=document.getElementById('epochSelect'),table=document.getElementById('timelineTable'),empty=document.getElementById('emptyTimeline');table.textContent='';const k=sel.value,e=ns[k];if(!e){{empty.textContent='No epoch event records were captured.';return}}const chosen=nodeSelect.value;const all=e.timeline||[];const rows=all.filter(r=>!chosen||r.node===chosen||r.actor===chosen||r.origin===chosen||r.recipient===chosen).slice(0,200);empty.textContent=all.length?`Showing ${{rows.length}} of ${{all.length}} epoch events involving ${{chosen||'all nodes'}} (maximum 200 rows).`:'No walkthrough events were captured for this epoch.';rows.forEach(r=>{{const tr=document.createElement('tr');const elapsed=Math.max(0,r.time-(D.measurement_start||0));let actor=r.node||r.actor||r.recipient||r.origin||'—';let details='';if(r.type==='roster')details=`Observed ${{r.observed}} / expected ${{r.expected??'—'}} peers; wait ${{fmt(r.wait_seconds)}} s; ${{r.reason||''}}; roster ${{r.fingerprint||''}}`;else if(r.type==='lookup')details=`Observed ${{r.observed}} peer records; ${{(r.peers||[]).slice(0,8).join(', ')}}${{(r.peers||[]).length>8?' …':''}}`;else if(r.type==='expected_count')details=`Expected stage population: ${{r.expected}}`;else if(r.type==='advertisement')details=`Step key: ${{r.key||'—'}}`;else if(r.type==='snapshot')details=`Layout ${{r.layout}}, partition ${{r.partition}}, selected ${{r.selected_cardinality??'—'}}`;else if(r.type==='application')details=`${{r.status||'unknown'}} · selected ${{r.selected_cardinality??'—'}} / applied ${{r.applied_cardinality??'—'}} · fallback ${{r.fallback?'yes':'no'}} · snapshot match ${{r.snapshot_matches?'yes':'no'}}`;else details=`${{r.origin}} → ${{r.recipient}} · selected ${{r.selected_cardinality}} / applied ${{r.applied_cardinality}}`;tr.innerHTML=`<td>${{fmt(elapsed,3)}}</td><td>${{esc(labels[r.type]||r.type)}}</td><td>${{esc(actor)}}</td><td>${{esc(details)}}</td>`;table.appendChild(tr)}})}}
document.getElementById('epochSelect').addEventListener('change',showTimeline);nodeSelect.addEventListener('change',showTimeline);drawCurve();drawWait();tables();showTimeline();
</script></body></html>"""


def render(run_path: str | Path) -> tuple[Path, Path]:
    """Replay a run and write ``DASHBOARD.html`` and ``report.md`` in its directory.

    The saved summary is used only for run metadata such as status/runtime. All
    Agora contribution counts and protocol tables are independently rebuilt
    from raw events by :func:`replay_trace`.
    """
    run_dir = Path(run_path)
    config_path = next((run_dir / name for name in ("resolved_config.json", "config.json")
                        if (run_dir / name).exists()), None)
    config = _read_json(config_path, {}) if config_path else {}
    if not isinstance(config, dict):
        config = {}
    metadata = _read_json(run_dir / "summary.json", {})
    if not isinstance(metadata, dict):
        metadata = {}
    manifest = _read_json(run_dir / "manifest.json", {})
    if isinstance(manifest, dict) and not metadata.get("assumptions"):
        metadata["assumptions"] = manifest.get("assumptions", [])
    data = replay_trace(_trace_path(run_dir), config)
    html_path = run_dir / "DASHBOARD.html"
    markdown_path = run_dir / "report.md"
    html_path.write_text(_html(data, config, metadata), encoding="utf-8")
    markdown_path.write_text(_markdown(data, config, metadata), encoding="utf-8")
    return html_path, markdown_path
