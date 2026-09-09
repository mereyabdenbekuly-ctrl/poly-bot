from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from polybot.storage import Storage

_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Polybot — autonomous observer</title>
<style>
:root{--bg:#f4f7fb;--panel:#fff;--ink:#172033;--muted:#6d7890;--line:#e6ebf2;
--blue:#3867f2;--blue-soft:#eef2ff;--green:#0f9d69;--green-soft:#e9fbf3;
--amber:#a66b00;--amber-soft:#fff7df;--red:#c53f52;--red-soft:#fff0f2;
--shadow:0 10px 30px rgba(25,39,73,.06)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.shell{max-width:1400px;margin:0 auto;padding:28px 28px 48px}.topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;margin-bottom:28px}
.brand{display:flex;align-items:center;gap:13px}.logo{width:42px;height:42px;border-radius:13px;background:linear-gradient(135deg,#5279ff,#7256dc);color:#fff;display:grid;place-items:center;font-weight:800;font-size:20px;box-shadow:0 8px 20px rgba(56,103,242,.24)}
h1{font-size:25px;letter-spacing:-.03em;margin:0}.subtitle{color:var(--muted);margin:3px 0 0}.live{display:flex;align-items:center;gap:9px;color:var(--muted);font-size:13px;white-space:nowrap}.dot{width:9px;height:9px;border-radius:50%;background:var(--green);box-shadow:0 0 0 4px var(--green-soft)}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:18px}.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;box-shadow:var(--shadow)}.metric{padding:18px 19px;min-height:112px}.eyebrow{color:var(--muted);font-size:12px;font-weight:700;letter-spacing:.05em;text-transform:uppercase}.value{font-size:27px;font-weight:750;letter-spacing:-.04em;margin-top:11px}.value.good{color:var(--green)}.value.warn{color:var(--amber)}.value.neutral{color:var(--ink)}.hint{color:var(--muted);font-size:12px;margin-top:3px}
.layout{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(340px,.75fr);gap:18px}.panel{padding:20px;margin-bottom:18px}.panel-head{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:16px}.panel h2{font-size:16px;margin:0;letter-spacing:-.015em}.panel-note{font-size:12px;color:var(--muted)}
.window{position:relative;overflow:hidden}.window:after{content:"";position:absolute;left:0;right:0;bottom:0;height:4px;background:linear-gradient(90deg,var(--blue) 0 18%,#e7ecf7 18%)}.window-meta{display:flex;gap:10px;flex-wrap:wrap;color:var(--muted);font-size:13px}.pill{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:5px 9px;font-size:12px;font-weight:700}.pill.blue{background:var(--blue-soft);color:var(--blue)}.pill.green{background:var(--green-soft);color:var(--green)}.pill.amber{background:var(--amber-soft);color:var(--amber)}.pill.gray{background:#f0f3f7;color:#657087}
.bar{height:8px;background:#edf1f7;border-radius:99px;overflow:hidden;margin-top:18px}.bar>span{display:block;height:100%;width:20%;border-radius:99px;background:linear-gradient(90deg,var(--blue),#7d6bf2)}
.positions{width:100%;min-width:620px;border-collapse:collapse}.positions th{text-align:left;color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.05em;font-weight:700;padding:0 10px 10px}.positions td{padding:13px 10px;border-top:1px solid var(--line);vertical-align:top}.positions th:first-child,.positions td:first-child{padding-left:0}.positions th:last-child,.positions td:last-child{padding-right:0}.market{font-weight:700}.sub{color:var(--muted);font-size:12px;margin-top:2px}.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}.positive{color:var(--green);font-weight:700}.negative{color:var(--red);font-weight:700}.empty{padding:24px 0;color:var(--muted);text-align:center}#positions{overflow-x:auto}
.source{display:flex;align-items:flex-start;justify-content:space-between;gap:15px;padding:14px 0;border-top:1px solid var(--line)}.source:first-of-type{border-top:0;padding-top:0}.source-name{font-weight:700}.source-detail{color:var(--muted);font-size:12px;margin-top:3px}.source-state{white-space:nowrap}
.timeline{display:grid;gap:0}.event{display:grid;grid-template-columns:110px 1fr;gap:14px;padding:13px 0;border-top:1px solid var(--line)}.event:first-child{border-top:0}.event-time{color:var(--muted);font-size:12px}.event-title{font-weight:700}.event-detail{color:var(--muted);font-size:12px;margin-top:3px}.status-icon{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:7px;background:var(--blue)}
.decisions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.decision{border:1px solid var(--line);border-radius:11px;padding:12px}.decision-top{display:flex;justify-content:space-between;gap:10px}.decision-id{font-weight:700}.decision-reason{color:var(--muted);font-size:12px;margin-top:5px}.decision-metrics{display:flex;gap:12px;margin-top:9px;font-size:12px;color:var(--muted)}
.forecast-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.forecast-card{border:1px solid var(--line);border-radius:13px;padding:14px;background:linear-gradient(180deg,#fff,#fbfcff)}.forecast-title{font-weight:750}.forecast-meta{display:flex;gap:7px;flex-wrap:wrap;margin-top:7px}.version-row{border-top:1px solid var(--line);padding-top:10px;margin-top:10px}.version-head{display:flex;justify-content:space-between;gap:10px;align-items:center}.version-name{font-weight:700;font-size:12px}.version-top{font-size:13px;margin-top:5px}.dist{display:grid;gap:5px;margin-top:9px}.dist-row{display:grid;grid-template-columns:72px 1fr 45px;gap:7px;align-items:center;font-size:11px;color:var(--muted)}.dist-track{height:6px;background:#edf1f7;border-radius:99px;overflow:hidden}.dist-fill{height:100%;background:linear-gradient(90deg,var(--blue),#7d6bf2);border-radius:99px}.quality{width:100%;border-collapse:collapse;font-size:12px}.quality th{text-align:left;color:var(--muted);padding:0 8px 9px;text-transform:uppercase;font-size:10px}.quality td{padding:10px 8px;border-top:1px solid var(--line)}.quality .num{text-align:right}.delta{color:var(--blue);font-size:11px}.forecast-empty{padding:20px;border:1px dashed var(--line);border-radius:12px;color:var(--muted);text-align:center}
.footer{color:var(--muted);font-size:12px;text-align:center;margin-top:7px}.error{background:var(--red-soft);color:var(--red);padding:12px;border-radius:10px;font-size:13px;margin-top:10px}
@media(max-width:1050px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.layout{grid-template-columns:1fr}.decisions,.forecast-grid{grid-template-columns:1fr}}
@media(max-width:620px){.shell{padding:20px 14px}.topbar{display:block}.live{margin-top:14px}.grid{grid-template-columns:1fr}.panel{padding:15px}.positions{font-size:12px}.positions th:nth-child(3),.positions td:nth-child(3){display:none}.event{grid-template-columns:82px 1fr}}
</style>
</head>
<body>
<main class="shell">
  <header class="topbar" aria-label="Polybot header">
    <div class="brand"><div class="logo">P</div><div><h1>Polybot</h1><p class="subtitle">Autonomous weather-market observer</p></div></div>
    <div class="live" aria-live="polite"><span class="dot"></span><span id="updated">Connecting…</span><span id="mode" class="pill blue">PAPER ONLY</span></div>
  </header>
  <section class="grid" id="metrics"></section>
  <section class="card panel window" id="window"></section>
  <section class="card panel"><div class="panel-head"><div><h2>Forecast comparison</h2><div class="panel-note">latest archived events · forecasts appear even without a trade</div></div><span id="forecast-count" class="pill blue">COLLECTING</span></div><div id="forecasts" class="forecast-grid"></div></section>
  <section class="card panel"><div class="panel-head"><div><h2>Out-of-sample quality</h2><div class="panel-note">resolved unique events only · lead-time and intraday evaluated separately</div></div><span class="panel-note">MAE · accuracy · Brier · calibration · coverage</span></div><div id="quality"></div></section>
  <div class="layout">
    <div>
      <section class="card panel"><div class="panel-head"><h2>Open paper positions</h2><span class="panel-note">same-token marks · no live orders</span></div><div id="positions"></div></section>
      <section class="card panel"><div class="panel-head"><h2>Latest decisions</h2><span class="panel-note">most recent autonomous cycle</span></div><div id="decisions" class="decisions"></div></section>
    </div>
    <aside>
      <section class="card panel"><div class="panel-head"><h2>Data sources</h2><span class="panel-note">live status</span></div><div id="sources"></div></section>
      <section class="card panel"><div class="panel-head"><h2>Runtime timeline</h2><span class="panel-note">persistent reports</span></div><div id="timeline" class="timeline"></div></section>
    </aside>
  </div>
  <p class="footer">View-only dashboard · refreshes every 30 seconds · this page never starts scans or places orders</p>
</main>
<script>
const $ = id => document.getElementById(id);
const money = v => v == null ? '—' : '$' + Number(v).toFixed(4);
const compactMoney = v => v == null ? '—' : '$' + Number(v).toFixed(2);
const dateTime = v => v ? new Date(v).toLocaleString('en-US', {timeZone:'Asia/Almaty',month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';
const elapsed = s => { s=Number(s||0); return s<60 ? `${s}s` : `${Math.floor(s/60)}m`; };
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function latestCycle(d){ return [...(d.reports||[])].reverse().find(r=>r.kind==='CYCLE' && r.payload && r.payload.scan); }
function renderMetrics(d){
  const p=d.portfolio||{}, scan=d.latest_scan||{};
  const cycle=latestCycle(d)?.payload?.scan||{}, cycleGeo=cycle.geoblock||{};
  const allowed=cycleGeo.blocked===false || (cycleGeo.blocked==null && scan.geoblocked===0);
  const networkLocation=[cycleGeo.country,cycleGeo.region].filter(Boolean).join(' / ');
  const status=allowed?'good':'warn';
  $('mode').textContent=d.active_window?.paper===false?'OBSERVE ONLY':'PAPER ONLY';
  $('mode').className='pill '+(d.active_window?.paper===false?'gray':'blue');
  $('metrics').innerHTML = [
    ['Open exposure',compactMoney(p.open_exposure_usd),'risk reserved','neutral'],
    ['Realized P&L',compactMoney(p.realized_pnl_usd),'settled paper only',Number(p.realized_pnl_usd||0)>=0?'good':'warn'],
    ['API spend',compactMoney(p.api_spend_usd),'persistent project budget','neutral'],
    ['Network',allowed?(networkLocation||'allowed'):'blocked',allowed?'public API reachable':'new entries disabled',status]
  ].map(x=>`<div class="card metric"><div class="eyebrow">${x[0]}</div><div class="value ${x[3]}">${x[1]}</div><div class="hint">${x[2]}</div></div>`).join('');
}
function renderWindow(d){
  const w=d.active_window, reports=d.reports||[];
  if(!w){$('window').innerHTML='<div class="empty">No active runtime window yet.</div>';return}
  const start=new Date(w.started_at), secs=Math.max(0,(Date.now()-start.getTime())/1000), pct=Math.min(100,secs/7200*100), last=d.latest_scan||{}, next=last.completed_at?new Date(new Date(last.completed_at).getTime()+Number(w.interval_seconds||300)*1000):null;
  $('window').innerHTML=`<div class="panel-head"><h2>Autonomous window #${w.id}</h2><span class="pill green"><span class="dot"></span> RUNNING</span></div>
  <div class="window-meta"><span>Started ${dateTime(w.started_at)}</span><span>Every ${w.interval_seconds/60} min</span><span>${w.paper?'Paper trading':'Observe only'}</span><span>Astra ${w.astra?'on':'off'}</span><span>Next scan ${next?dateTime(next):'pending'}</span></div>
  <div class="bar"><span style="width:${pct}%"></span></div><div class="hint" style="margin-top:8px">${elapsed(secs)} of 120m · ${reports.length} saved reports · times shown in Almaty</div>`;
}
function renderPositions(d){
  const rows=d.positions||[];
  if(!rows.length){$('positions').innerHTML='<div class="empty">No open paper positions.</div>';return}
  $('positions').innerHTML=`<table class="positions"><thead><tr><th>Market</th><th>State</th><th>Entry</th><th>Exit mark</th><th>P&L</th></tr></thead><tbody>${rows.map(o=>{const pnl=o.estimated_full_exit_pnl_usd; const title=o.market_question||o.market_id; return `<tr><td><div class="market">${esc(title)} <span class="pill gray">${esc(o.outcome_label||o.outcome||'YES')}</span></div><div class="sub">market ${esc(o.market_id)} · ${o.shares} shares</div></td><td><span class="pill ${o.status==='PAPER_SETTLED'?'green':'blue'}">${esc(o.status)}</span><div class="sub">${esc(o.strategy_version||'v0')}</div></td><td class="num">${money(o.entry_price)}<div class="sub">cost ${money(o.notional_usd)}</div></td><td class="num">${o.current_bid_price==null?'—':money(o.current_bid_price)}<div class="sub">${o.immediately_sellable_shares||0}/${o.shares} sellable</div></td><td class="num ${pnl!=null?(Number(pnl)>=0?'positive':'negative'):''}">${pnl==null?'—':money(pnl)}<div class="sub">${o.full_exit_value_usd==null?'no full exit':'hypothetical'}</div></td></tr>`}).join('')}</tbody></table>`;
}
const shortVersion = v => v==='open-meteo-truncated-normal-v1'?'v1 baseline':v==='ecmwf-ifs025-raw-ensemble-v1'?'IFS ENS via Open-Meteo':v?.startsWith('forecast-engine-v2')?'v2 shadow / conditioned':v||'unknown';
function renderForecasts(d){
 const fc=d.forecast_comparison||{}, events=fc.events||[], count=fc.counts?.predictions||0;
 $('forecast-count').textContent=count?`${fc.shown_event_count||events.length} EVENTS · ${count} HISTORICAL PREDICTIONS`:'COLLECTING';
 if(fc.error){$('forecasts').innerHTML=`<div class="error">${esc(fc.error)}</div>`;return}
 if(!events.length){$('forecasts').innerHTML='<div class="forecast-empty">The first versioned forecast snapshots will appear after the next autonomous scan.</div>';return}
 $('forecasts').innerHTML=events.map(e=>`<article class="forecast-card"><div class="forecast-title">${esc(e.event_title||e.event_id)}</div><div class="forecast-meta"><span class="pill gray">${esc(e.station_id||'station')}</span><span class="pill gray">${esc(e.observation_date||'')}</span><span class="pill ${e.observed_max_c==null?'gray':'green'}">observed max ${e.observed_max_c==null?'—':esc(e.observed_max_c)+'°C'}</span></div>${(e.versions||[]).sort((a,b)=>shortVersion(a.algorithm_version).localeCompare(shortVersion(b.algorithm_version))).map(v=>{const top=v.top||{}, prev=v.previous_top||{}, sameTop=top.market_id&&prev.market_id&&top.market_id===prev.market_id, delta=sameTop&&top.probability!=null&&prev.probability!=null?Number(top.probability)-Number(prev.probability):null, meta=v.metadata||{}, profile=meta.station_correction||{}, viaEcmwf=String(v.source||'').includes('ecmwf'); const label=shortVersion(v.algorithm_version); const provenance=viaEcmwf?(v.model_init_time_utc&&v.model_published_at_utc?'official run metadata':'JSON archived · run/publication unknown'):v.source; const topFive=(v.distribution||[]).slice(0,5), remaining=Math.max(0,100-topFive.reduce((s,p)=>s+Number(p.probability||0)*100,0)); return `<div class="version-row"><div class="version-head"><span class="version-name">${esc(label)}</span><span class="pill ${viaEcmwf?'blue':'gray'}">${esc(provenance)} · ${v.scenario_count}</span></div><div class="version-top"><b>${esc(top.label||'no distribution')}</b> ${top.probability==null?'—':(Number(top.probability)*100).toFixed(1)+'%'} ${delta==null?(prev.market_id&&top.market_id!==prev.market_id?`<span class="delta">top changed from ${esc(prev.label||prev.market_id)}</span>`:''):`<span class="delta">${delta>=0?'+':''}${(delta*100).toFixed(1)} pp</span>`}</div><div class="sub">mean member max ${v.point_forecast_c==null?'—':Number(v.point_forecast_c).toFixed(2)+'°C'} · ${esc(v.phase)} · ${dateTime(v.issued_at_utc)}${label.includes('v2')?` · ${profile.state==='fitted'?`correction fitted n=${profile.sample_count}`:'correction not fitted'}`:''}</div><div class="dist"><div class="sub">top brackets</div>${topFive.map(p=>`<div class="dist-row"><span>${esc(p.label)}</span><span class="dist-track"><span class="dist-fill" style="width:${Math.min(100,Number(p.probability||0)*100)}%"></span></span><span class="num">${(Number(p.probability||0)*100).toFixed(1)}%</span></div>`).join('')}${(v.distribution||[]).length>5?`<div class="sub">+ ${(v.distribution||[]).length-5} other brackets · remaining ${remaining.toFixed(1)}%</div>`:''}</div></div>`}).join('')}</article>`).join('');
}
function renderQuality(d){
 const reports=d.forecast_comparison?.metrics||[];
 if(!reports.length || !reports.some(r=>Number(r.overall?.outcome_event_count||0)>0)){$('quality').innerHTML='<div class="forecast-empty">No resolved outcomes yet; quality metrics are unavailable. Forecast coverage is not scored until an official result exists.</div>';return}
 const rows=reports.map(r=>{const o=r.overall||{},q=r.query||{};return `<tr><td><b>${esc(shortVersion(q.algorithm_version))}</b><div class="sub">${esc(q.source)} · ${esc(q.model)}</div></td><td class="num">${o.event_count||0}</td><td class="num">${o.max_temperature_mae_c==null?'—':Number(o.max_temperature_mae_c).toFixed(2)+'°C'}</td><td class="num">${o.exact_bracket_accuracy==null?'—':(Number(o.exact_bracket_accuracy)*100).toFixed(1)+'%'}</td><td class="num">${o.multiclass_brier_score==null?'—':Number(o.multiclass_brier_score).toFixed(3)}</td><td class="num">${o.expected_calibration_error==null?'—':Number(o.expected_calibration_error).toFixed(3)}</td><td class="num">${o.outcome_event_count?((Number(o.coverage||0)*100).toFixed(1)+'%'):'—'}</td></tr>`}).join('');
 $('quality').innerHTML=`<div style="overflow-x:auto"><table class="quality"><thead><tr><th>Version</th><th class="num">Events</th><th class="num">MAE</th><th class="num">Exact</th><th class="num">Brier</th><th class="num">ECE</th><th class="num">Coverage</th></tr></thead><tbody>${rows}</tbody></table></div>`;
}
function renderSources(d){
 const reports=d.reports||[], latest=[...reports].reverse().find(r=>r.payload?.weathernext), wn=(latest?.payload?.weathernext)||{state:'unknown',message:'No status'}; const scan=d.latest_scan||{}; const cycle=latestCycle(d)?.payload?.scan||{}, cycleGeo=cycle.geoblock||{}; const allowed=cycleGeo.blocked===false || (cycleGeo.blocked==null && scan.geoblocked===0); const versions=(d.forecast_comparison?.events||[]).flatMap(e=>e.versions||[]), hasEcmwf=versions.some(v=>String(v.source||'').includes('ecmwf')), official=(d.forecast_comparison?.source_statuses||[]).find(x=>x.source==='ecmwf-open-data-ifs-ens'), officialGood=official?.state==='available';
 const networkLocation=[cycleGeo.country,cycleGeo.region].filter(Boolean).join(' / ');
 $('sources').innerHTML=`<div class="source"><div><div class="source-name">Polymarket API</div><div class="source-detail">${allowed?`${networkLocation||'API'} endpoint accepted`:'status from latest scan'}</div></div><div class="source-state pill ${allowed?'green':'amber'}">${allowed?'AVAILABLE':'CHECK'}</div></div>
 <div class="source"><div><div class="source-name">Station observations</div><div class="source-detail">NOAA WRH / Synoptic + AWC cross-check</div></div><div class="source-state pill green">ACTIVE</div></div>
 <div class="source"><div><div class="source-name">GPT-6 Astra</div><div class="source-detail">Rules only · cached · no wallet access</div></div><div class="source-state pill ${d.active_window?.astra?'green':'gray'}">${d.active_window?.astra?'ON':'OFF'}</div></div>
 <div class="source"><div><div class="source-name">IFS ENS via Open-Meteo</div><div class="source-detail">50 members · JSON archived · run/publication unknown · official GRIB adapter not active</div></div><div class="source-state pill ${hasEcmwf?'amber':'gray'}">${hasEcmwf?'PARTIAL PROVENANCE':'NOT COLLECTING'}</div></div>
 <div class="source"><div><div class="source-name">Official ECMWF Open Data archive</div><div class="source-detail">${esc(official?.payload?.message||'Raw mx2t3 archive has not completed yet.')} · station decoding separate</div></div><div class="source-state pill ${officialGood?'green':'amber'}">${officialGood?'ARCHIVED':esc(official?.state||'PENDING')}</div></div>
 <div class="source"><div><div class="source-name">WeatherNext 3</div><div class="source-detail">${esc(wn.message||'')}</div></div><div class="source-state pill ${wn.state==='snapshot_available'?'green':'amber'}">${esc(wn.state||'UNKNOWN')}</div></div>`;
}
function renderTimeline(d){
 const reports=[...(d.reports||[])].reverse();
 $('timeline').innerHTML=reports.length?reports.map(r=>`<div class="event"><div class="event-time">${dateTime(r.created_at)}<br><small>${elapsed(r.elapsed_seconds)}</small></div><div><div class="event-title"><span class="status-icon"></span>${esc(r.kind.replaceAll('_',' '))}</div><div class="event-detail">${r.payload?.error?esc(r.payload.error):r.payload?.scan?`${r.payload.scan.events_scanned||0} events · ${r.payload.scan.markets_scanned||0} markets · ${r.payload.scan.paper_orders_opened||0} opened`:esc(r.payload?.message||'Autonomous service is running.')}</div></div></div>`).join(''):'<div class="empty">Reports will appear automatically.</div>';
}
function renderDecisions(d){
 const decisions=d.decisions||[];
 if(!decisions.length){$('decisions').innerHTML='<div class="empty">No decisions in the latest cycle.</div>';return}
 const interesting=decisions.filter(x=>x.action!=='SKIP'||(x.expected_profit_usd!=null&&Number(x.expected_profit_usd)>0)).slice(0,12);
 $('decisions').innerHTML=(interesting.length?interesting:decisions.slice(0,12)).map(x=>`<div class="decision"><div class="decision-top"><span class="decision-id">${esc(x.market_id)}</span><span class="pill ${x.action==='PAPER_BUY'?'green':x.action==='OBSERVE'?'blue':'gray'}">${esc(x.action)}</span></div><div class="decision-metrics"><span>p ${x.probability==null?'—':(Number(x.probability)*100).toFixed(1)+'%'}</span><span>EV ${money(x.expected_profit_usd)}</span><span>entry ${money(x.executable_price)}</span></div><div class="decision-reason">${esc((x.reason_codes||[]).concat(x.warning_codes||[]).join(' · ')||'qualified')}</div></div>`).join('');
}
async function refresh(){try{const response=await fetch('/api/dashboard',{cache:'no-store'});const data=await response.json();$('updated').textContent='Updated '+dateTime(data.generated_at);renderMetrics(data);renderWindow(data);renderForecasts(data);renderQuality(data);renderPositions(data);renderSources(data);renderTimeline(data);renderDecisions(data)}catch(error){$('updated').textContent='Connection error';console.error(error)}}
refresh();setInterval(refresh,30000);
</script>
</body>
</html>"""


def serve_dashboard(storage: Storage, *, host: str, port: int) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/health":
                self._send_json({"ok": True})
            elif path in {"/api/dashboard", "/api/status"}:
                self._send_json(storage.dashboard_payload())
            elif path == "/":
                body = _HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header(
                    "Content-Security-Policy",
                    "default-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'",
                )
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)

        def _send_json(self, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header(
                "Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'"
            )
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    ThreadingHTTPServer((host, port), Handler).serve_forever()
