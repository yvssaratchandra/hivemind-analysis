"""Render a self-contained, one-page dashboard from an audited pilot suite.

Usage: python research/simulator/render_pilot_dashboard.py RESULTS_DIRECTORY
"""
import argparse
import csv
import gzip
import json
from pathlib import Path
from string import Template


PAGE = Template(r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hivemind simulation · 100-node pilot</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#173047;background:#f5f7fb}
*{box-sizing:border-box}body{margin:0}main{max-width:1440px;margin:auto;padding:24px 28px 36px}
.eyebrow{text-transform:uppercase;letter-spacing:.15em;font-size:11px;font-weight:800;color:#3068a1}h1{font-size:30px;line-height:1.13;margin:7px 0 5px;letter-spacing:-.025em}h2{font-size:17px;margin:0 0 3px}p{margin:0;color:#526579;font-size:13px;line-height:1.45}
.header{display:flex;justify-content:space-between;align-items:end;gap:20px;margin-bottom:18px}.tag{background:#e6f1e9;color:#1e6941;border-radius:20px;padding:7px 12px;font-size:12px;font-weight:750;white-space:nowrap}
.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:14px}.card,.panel,.foot{background:white;border:1px solid #e0e7ef;border-radius:14px;box-shadow:0 2px 6px #1f3b5808}.card{padding:16px 18px}.card .label{font-size:12px;color:#66788b}.card .value{font-size:27px;font-weight:750;margin:5px 0 2px;letter-spacing:-.03em}.card .detail{font-size:11px;color:#66788b}
.grid{display:grid;grid-template-columns:1.65fr 1fr;gap:14px}.panel{padding:18px 18px 12px;min-width:0}.panel-head{display:flex;justify-content:space-between;gap:10px;align-items:baseline}.panel-head p{font-size:12px}.plot{width:100%;height:244px;display:block}.smallplot{height:204px}.bottom{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px}.foot{padding:15px 18px;display:flex;gap:24px;justify-content:space-between;align-items:start}.foot strong{font-size:13px}.foot p{font-size:12px;margin-top:4px}.links{white-space:nowrap;font-size:12px;line-height:1.8}a{color:#176ca4;text-decoration:none}a:hover{text-decoration:underline}
.legend{display:flex;gap:13px;flex-wrap:wrap;font-size:11px;color:#607386;margin-top:3px}.key{display:inline-block;width:9px;height:9px;border-radius:2px;margin-right:4px}.axis{font-size:10px;fill:#728297}.gridline{stroke:#e8edf2;stroke-width:1}.tooltip{position:fixed;pointer-events:none;background:#173047;color:white;padding:8px 10px;border-radius:6px;font-size:12px;max-width:260px;display:none;z-index:9;box-shadow:0 4px 14px #0003}
@media(max-width:900px){.grid,.bottom{grid-template-columns:1fr}.cards{grid-template-columns:repeat(2,1fr)}.foot{display:block}.links{margin-top:8px}}@media(max-width:520px){main{padding:18px 12px}.cards{gap:8px}.card{padding:12px}.card .value{font-size:22px}.header{display:block}.tag{display:inline-block;margin-top:10px}}
</style></head><body><main>
<div class="header"><div><div class="eyebrow">Control-plane experiment · honest baseline pilot</div><h1>100 nodes, one seed</h1><p>Simulated Hivemind-style grouping and aggregation · 180 seconds of virtual time</p></div><span class="tag">Audited pilot · awaiting research review</span></div>
<div class="cards">
 <div class="card"><div class="label">Accepted contributions</div><div class="value" id="accepted"></div><div class="detail">By the 180 s observation boundary</div></div>
 <div class="card"><div class="label">500-contribution target</div><div class="value" id="target"></div><div class="detail">Reached before observation ended</div></div>
 <div class="card"><div class="label">Groups formed</div><div class="value" id="groups"></div><div class="detail" id="groups-detail"></div></div>
 <div class="card"><div class="label">Nodes receiving credit</div><div class="value" id="nodes"></div><div class="detail">No honest node was left without credit</div></div>
 <div class="card"><div class="label">First compute starts</div><div class="value">0–10 s</div><div class="detail" id="starts-detail"></div></div>
</div>
<div class="grid">
 <section class="panel"><div class="panel-head"><div><h2>Training progress</h2><p>Unique contributions incorporated in a completed aggregate</p></div></div><svg id="progress" class="plot" viewBox="0 0 760 250" role="img" aria-label="Accepted contributions over simulated time"></svg><div class="legend"><span><i class="key" style="background:#176ca4"></i>accepted work</span><span><i class="key" style="background:#d18b19"></i>500-contribution target</span></div></section>
 <section class="panel"><div class="panel-head"><div><h2>Group sizes</h2><p>How many contributors joined each group</p></div></div><svg id="sizes" class="plot" viewBox="0 0 440 250" role="img" aria-label="Group size counts"></svg><div class="legend"><span><i class="key" style="background:#3a82ae"></i>full group of 8</span><span><i class="key" style="background:#c49750"></i>smaller group</span></div></section>
</div>
<div class="bottom">
 <section class="panel"><div class="panel-head"><div><h2>Participation across nodes</h2><p>Number of accepted contributions per node</p></div><p id="participation-label"></p></div><svg id="participation" class="plot smallplot" viewBox="0 0 660 210" role="img" aria-label="Accepted contributions for each of 100 nodes"></svg></section>
 <section class="panel"><div class="panel-head"><div><h2>Time from ready to credited</h2><p>For contributions accepted during the observation window</p></div><p id="wait-label"></p></div><svg id="waits" class="plot smallplot" viewBox="0 0 660 210" role="img" aria-label="Distribution of wait times from work ready to accepted"></svg></section>
</div>
<div class="foot"><div><strong>What this pilot shows</strong><p>Grouping, per-node participation, waiting, and work progress are visible in the trace. Most groups had 8 members. Every node earned credit in this honest run.</p><p>One synthetic seed cannot show variability or deployed Internet timing. No timeout or partial aggregation occurred in this pilot.</p></div><div class="links"><a href="PILOT_REVIEW.md">Pilot details</a> · <a href="group_formations.csv">Groups CSV</a><br><a href="accepted_work_curve.csv">Progress CSV</a> · <a href="n100_controlled_s1/per_node.csv">Per-node CSV</a></div></div>
</main><div class="tooltip" id="tip"></div>
<script>const d=$data;
const NS='http://www.w3.org/2000/svg';const fmt=n=>Number(n).toLocaleString('en-US',{maximumFractionDigits:1});
function el(tag,a={},parent){let x=document.createElementNS(NS,tag);for(let[k,v]of Object.entries(a))x.setAttribute(k,v);parent.appendChild(x);return x}
function tx(parent,x,y,s,a={}){let n=el('text',{x,y,...a},parent);n.textContent=s;return n}
function tip(node,message){node.addEventListener('pointermove',e=>{let t=document.getElementById('tip');t.textContent=message;t.style.display='block';t.style.left=Math.min(e.clientX+12,innerWidth-270)+'px';t.style.top=(e.clientY+12)+'px'});node.addEventListener('pointerleave',()=>document.getElementById('tip').style.display='none')}
function base(svg,w,h,l=43,r=12,t=12,b=32){el('line',{x1:l,y1:h-b,x2:w-r,y2:h-b,stroke:'#b9c6d3'},svg);return{x0:l,x1:w-r,y0:t,y1:h-b}}
document.getElementById('accepted').textContent=fmt(d.accepted);
document.getElementById('target').textContent=fmt(d.targetTime)+' s';
document.getElementById('groups').textContent=fmt(d.groupCount);
document.getElementById('groups-detail').textContent=fmt(d.fullGroups)+' full-sized · '+fmt(d.groupCount-d.fullGroups)+' smaller';
document.getElementById('nodes').textContent=fmt(d.nodesWithCredit)+' / '+fmt(d.population);
document.getElementById('starts-detail').textContent=fmt(d.startCount)+' distinct seeded offsets · observed '+fmt(d.startMin)+'–'+fmt(d.startMax)+' s';
document.getElementById('participation-label').textContent='range '+fmt(d.minCredit)+'–'+fmt(d.maxCredit);
document.getElementById('wait-label').textContent='median '+fmt(d.waitMedian)+' s · p90 '+fmt(d.waitP90)+' s';
{
 const s=document.getElementById('progress'),p=base(s,760,250,48,18,14,33),x=t=>p.x0+t/d.finalTime*(p.x1-p.x0),y=v=>p.y1-v/d.accepted*(p.y1-p.y0);
 for(let v of [0,200,400,600,800]){if(v>d.accepted)continue;el('line',{x1:p.x0,y1:y(v),x2:p.x1,y2:y(v),class:'gridline'},s);tx(s,8,y(v)+4,v,{class:'axis'})}
 for(let t of [0,30,60,90,120,150,180]){tx(s,x(t)-8,240,t,{class:'axis'})}
 el('line',{x1:p.x0,y1:y(500),x2:p.x1,y2:y(500),stroke:'#d18b19','stroke-width':1.5,'stroke-dasharray':'5 5'},s);
 const points=d.progress.map(a=>x(a[0]).toFixed(1)+','+y(a[1]).toFixed(1)).join(' ');
 el('polyline',{points,fill:'none',stroke:'#176ca4','stroke-width':3,'stroke-linejoin':'round'},s);
 el('circle',{cx:x(d.targetTime),cy:y(500),r:5,fill:'#d18b19',stroke:'white','stroke-width':2},s);
 tx(s,x(d.targetTime)-40,y(500)-13,'target reached',{class:'axis',fill:'#945f0e'});
 tip(s,'At '+fmt(d.targetTime)+' s, the 500-contribution target was reached. By 180 s: '+fmt(d.accepted)+'.')
}
{
 const s=document.getElementById('sizes'),p=base(s,440,250,36,14,16,38),entries=Object.entries(d.sizeCounts).sort((a,b)=>+a[0]-+b[0]),max=Math.max(...entries.map(a=>a[1]));
 entries.forEach(([size,n],i)=>{let bw=(p.x1-p.x0)/entries.length,x=p.x0+i*bw+bw*.18,h=n/max*(p.y1-p.y0),bar=el('rect',{x,y:p.y1-h,width:bw*.64,height:h,rx:4,fill:+size===8?'#3a82ae':'#c49750'},s);tip(bar,n+' groups had '+size+' contributors');tx(s,x+bw*.32,p.y1+18,size,{class:'axis','text-anchor':'middle'});tx(s,x+bw*.32,p.y1-h-7,n,{class:'axis','text-anchor':'middle'})});tx(s,180,246,'contributors per group',{class:'axis'})
}
{
 const s=document.getElementById('participation'),p=base(s,660,210,42,10,14,33),max=Math.max(...d.nodeCredit),bw=(p.x1-p.x0)/d.nodeCredit.length;
 for(let v of [0,2,4,6,8]){let y=p.y1-v/max*(p.y1-p.y0);el('line',{x1:p.x0,y1:y,x2:p.x1,y2:y,class:'gridline'},s);tx(s,15,y+3,v,{class:'axis'})}
 d.nodeCredit.forEach((v,i)=>{let h=v/max*(p.y1-p.y0),bar=el('rect',{x:p.x0+i*bw,y:p.y1-h,width:Math.max(1,bw-.7),height:h,fill:'#6ba18c'},s);tip(bar,'Node N'+String(i).padStart(4,'0')+': '+fmt(v)+' accepted contributions')});tx(s,45,199,'N0000',{class:'axis'});tx(s,590,199,'N0099',{class:'axis'})
}
{
 const s=document.getElementById('waits'),p=base(s,660,210,45,12,14,33),n=d.waitBins.length,max=Math.max(...d.waitBins),bw=(p.x1-p.x0)/n;
 for(let v of [0,Math.round(max/2),max]){let y=p.y1-v/max*(p.y1-p.y0);el('line',{x1:p.x0,y1:y,x2:p.x1,y2:y,class:'gridline'},s);tx(s,8,y+3,v,{class:'axis'})}
 d.waitBins.forEach((v,i)=>{let h=v/max*(p.y1-p.y0),bar=el('rect',{x:p.x0+i*bw+1,y:p.y1-h,width:bw-2,height:h,fill:'#9476ad'},s);tip(bar,v+' contributions waited '+fmt(i*d.waitStep)+'–'+fmt((i+1)*d.waitStep)+' s')});
 tx(s,p.x0,p.y1+18,'0 s',{class:'axis'});tx(s,p.x1-35,p.y1+18,fmt(n*d.waitStep)+' s',{class:'axis'})
}
</script></body></html>''')


def render(suite: Path) -> Path:
    status = json.loads((suite / "status.json").read_text())
    if status["status"] != "awaiting_review" or status["audited_runs"] != 1:
        raise ValueError("Expected a completed, audited pilot awaiting review")
    child = suite / "n100_controlled_s1"
    summary = json.loads((child / "summary.json").read_text())
    audit = json.loads((child / "baseline_audit.json").read_text())["row"]
    curve = list(csv.DictReader((suite / "accepted_work_curve.csv").open()))
    progress = [[float(r["simulated_seconds"]), float(r["cumulative_accepted_work"])] for r in curve]
    ready, waits, starts = {}, [], {}
    with gzip.open(child / "events.jsonl.gz", "rt") as trace:
        for line in trace:
            event = json.loads(line)
            if event["kind"] == "node_phase" and event["data"].get("phase") == "compute":
                starts.setdefault(event["actor"], event["time"])
            elif event["kind"] == "work_ready":
                ready[event["data"]["work"]["work_id"]] = event["time"]
            elif event["kind"] == "work_accepted" and event["time"] < summary["final_time"]:
                waits.append(event["time"] - ready[event["data"]["work_id"]])
    if (len(waits) != summary["accepted_work_items"] or progress[-1][1] != summary["accepted_work"]
            or len(starts) != summary["population"] or len(set(starts.values())) != len(starts)):
        raise ValueError("Dashboard data disagree with audited work credit")
    wait_step = max(1, (max(waits) + 0.001) / 16)
    bins = [0] * 16
    for wait in waits:
        bins[min(15, int(wait / wait_step))] += 1
    size_counts = {}
    for group in summary["groups"]:
        size = str(group["group_size"])
        size_counts[size] = size_counts.get(size, 0) + 1
    node_credit = [float(r["accepted_work"]) for r in sorted(summary["per_node"], key=lambda r: r["node_id"])]
    data = dict(population=summary["population"], accepted=summary["accepted_work"],
                targetTime=summary["target_time"], groupCount=summary["group_count"],
                fullGroups=size_counts.get("8", 0), sizeCounts=size_counts,
                nodesWithCredit=sum(x > 0 for x in node_credit), minCredit=min(node_credit),
                maxCredit=max(node_credit), nodeCredit=node_credit, progress=progress,
                startCount=len(starts), startMin=min(starts.values()), startMax=max(starts.values()),
                finalTime=summary["final_time"], waitMedian=audit["median_ready_to_accept_seconds_at_observation"],
                waitP90=audit["p90_ready_to_accept_seconds_at_observation"],
                waitBins=bins, waitStep=wait_step)
    output = suite / "PILOT_DASHBOARD.html"
    output.write_text(PAGE.substitute(data=json.dumps(data, separators=(",", ":")).replace("</", "<\\/")))
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", type=Path)
    args = parser.parse_args()
    print(render(args.suite))
