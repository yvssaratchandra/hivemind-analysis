"""Render a compact baseline overview from completed, audited seed results."""
import argparse
import json
from pathlib import Path


def render(suite):
    status = json.loads((suite / "status.json").read_text())
    rows = json.loads((suite / "runs.json").read_text())
    config = json.loads((suite / "config.json").read_text())
    stats = json.loads((suite / "statistics.json").read_text())["100"]
    if status["status"] != "completed" or sorted(r["seed"] for r in rows) != config["seeds"]:
        raise ValueError("Dashboard requires the completed, audited baseline")
    rows.sort(key=lambda r: r["seed"])
    metrics = stats["metrics"]

    def chart(key, unit, color):
        values = [r[key] for r in rows]
        maximum = max((v for v in values if v is not None), default=1) or 1
        parts = ['<svg viewBox="0 0 600 290" role="img">']
        for i, (row, value) in enumerate(zip(rows, values)):
            x = 43 + i * 27
            height = 210 * value / maximum if value is not None else 0
            display = f"{value:.2f} {unit}" if value is not None else "censored"
            parts.append(f'<rect x="{x}" y="{245-height:.2f}" width="18" height="{height:.2f}" fill="{color}" rx="3"><title>Seed {row["seed"]}: {display}</title></rect>')
            parts.append(f'<text x="{x+9}" y="265" text-anchor="middle">{row["seed"]}</text>')
        parts += [f'<text x="8" y="38">{maximum:.1f}</text>', '<text x="15" y="245">0</text>',
                  '<text x="280" y="287">seed</text>', '</svg>']
        return ''.join(parts)

    target = metrics["target_time"]
    throughput = metrics["accepted_work_per_second_at_observation"]
    cards = [
        ("Audited seeds", f"{len(rows)} / {len(config['seeds'])}", "100 honest nodes per seed"),
        ("Time to 500 credits", f"{target['median']:.2f} s" if target['median'] is not None else "No completions", "median across completed seeds"),
        ("Accepted work per second", f"{throughput['median']:.2f}", "median over the first 180 simulated seconds"),
        ("Seeds reaching target", f"{stats['completed']} / {len(rows)}", f"{stats['censored']} censored at 600 simulated seconds"),
    ]
    cards_html = ''.join(f'<div class="card"><span>{a}</span><strong>{b}</strong><small>{c}</small></div>' for a,b,c in cards)
    table = ''.join(
        f'<tr><td>{r["seed"]}</td><td>{r["target_time"]:.2f}</td><td>{r["accepted_work_at_observation"]:.0f}</td>'
        f'<td>{r["groups_at_observation"]}</td><td>{r["zero_participation_at_observation"]}</td></tr>'
        if r["target_time"] is not None else
        f'<tr><td>{r["seed"]}</td><td>censored</td><td>{r["accepted_work_at_observation"]:.0f}</td><td>{r["groups_at_observation"]}</td><td>{r["zero_participation_at_observation"]}</td></tr>'
        for r in rows)
    zero = sum(r["zero_participation_at_observation"] > 0 for r in rows)
    failed_attempts = sum(r["terminal_outcomes"].get("failed", 0) for r in rows)
    seeds_with_failures = sum(r["terminal_outcomes"].get("failed", 0) > 0 for r in rows)
    doc = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>100-node baseline · 20 seeds</title>
<style>body{{font-family:system-ui,sans-serif;background:#f4f7fa;color:#173047;margin:0}}main{{max-width:1240px;margin:auto;padding:24px}}h1{{margin:0 0 6px}}p{{color:#526579;font-size:14px;line-height:1.5}}.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}}.card,.panel{{background:white;border:1px solid #dce5ed;border-radius:12px;padding:16px}}.card span,.card small{{display:block;font-size:12px;color:#526579}}.card strong{{display:block;font-size:28px;margin:8px 0}}.charts{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px}}h2{{font-size:18px;margin:0 0 4px}}svg{{width:100%;font-size:11px;fill:#63788c}}table{{border-collapse:collapse;width:100%;font-size:12px}}td,th{{padding:5px 10px;text-align:right;border-bottom:1px solid #e7edf2}}th{{color:#526579}}details{{margin-top:16px}}summary{{cursor:pointer;font-weight:650}}a{{color:#176ca4}}@media(max-width:800px){{.charts{{grid-template-columns:1fr}}.cards{{grid-template-columns:repeat(2,1fr)}}}}</style></head>
<body><main><h1>100-node honest baseline · 20 seeds</h1><p>Each seed uses independently randomized initial compute starts over 0–10 s. Fixed compute and network resources; no adversarial actions or churn.</p>
<div class="cards">{cards_html}</div><div class="charts">
<div class="panel"><h2>Time to the work target</h2><p>Simulated seconds to 500 unique accepted contributions. Hover for values.</p>{chart('target_time','s','#397ca5')}<p>Middle 50% of completed seeds: {target['iqr'][0]:.2f}–{target['iqr'][1]:.2f} s.</p></div>
<div class="panel"><h2>Useful-work rate</h2><p>Accepted contributions per simulated second over the same 180 s window.</p>{chart('accepted_work_per_second_at_observation','credits/s','#5b9a80')}<p>Middle 50%: {throughput['iqr'][0]:.2f}–{throughput['iqr'][1]:.2f} contributions/s.</p></div></div>
<p><strong>Participation:</strong> {zero} of {len(rows)} seeds had a node with no credited contribution by 180 s. Group count median: {metrics['groups_at_observation']['median']:.1f}. Median readiness-to-credit wait across seeds: {metrics['median_ready_to_accept_seconds_at_observation']['median']:.2f} s.</p>
<p><strong>Normal failure count:</strong> {failed_attempts} individual work attempts failed across {seeds_with_failures} seeds. These are separate from whole-run target completion; preserve them when comparing attacks.</p>
<p>This establishes a controlled simulation baseline for matched-seed attack comparisons. It does not measure model accuracy or convergence, or validate Internet deployment timing.</p>
<details><summary>Inspect all 20 seeds</summary><table><thead><tr><th>Seed</th><th>Target time (s)</th><th>Credits by 180 s</th><th>Groups by 180 s</th><th>Nodes without credit</th></tr></thead><tbody>{table}</tbody></table></details>
<p><a href="REPORT.md">Full report</a> · <a href="runs.csv">Per-seed CSV</a> · <a href="statistics.json">Statistics and uncertainty intervals</a></p></main></body></html>'''
    output = suite / "BASELINE_DASHBOARD.html"
    output.write_text(doc)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", type=Path)
    print(render(parser.parse_args().suite))
