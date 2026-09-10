from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from polybot.forecast_comparison import COMPARISON_VERSION, compare_forecasts
from polybot.forecast_store import ForecastStore
from polybot.storage import Storage

# The page refreshes every 30 seconds while forecast data normally changes on
# the much slower observer cadence.  A one-minute TTL prevents every refresh
# from rebuilding the same bootstrap report while keeping it visibly fresh.
COMPARISON_CACHE_TTL_SECONDS = 60.0


class ComparisonCache:
    """Single-flight, short-lived cache for the read-only comparison report.

    Building a comparison report scans several forecast tables and computes
    deterministic bootstrap intervals.  The dashboard is served by a threaded
    HTTP server and may receive concurrent requests, so a plain per-request
    call would duplicate the work (and could make a refresh burst expensive).
    Holding the lock while refreshing deliberately gives the cache single-flight
    semantics: one request computes, concurrent requests reuse the result once
    it is available.  No write-capable storage object is used here.
    """

    def __init__(
        self,
        database: Path | str,
        *,
        ttl_seconds: float = COMPARISON_CACHE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds < 0:
            raise ValueError("comparison cache TTL must be non-negative")
        self.database = Path(database).expanduser().resolve()
        self.ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._payload: dict[str, object] | None = None
        self._expires_at = 0.0

    def get(self) -> dict[str, object]:
        """Return a cached report or build one exactly once per TTL window."""

        now = float(self._clock())
        with self._lock:
            if self._payload is not None and now < self._expires_at:
                return self._payload
            payload = compare_forecasts(self.database)
            self._payload = payload
            self._expires_at = float(self._clock()) + self.ttl_seconds
            return payload

    def clear(self) -> None:
        """Invalidate the cached report (used after an explicit refresh)."""

        with self._lock:
            self._payload = None
            self._expires_at = 0.0


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
.forecast-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.forecast-card{border:1px solid var(--line);border-radius:13px;padding:14px;background:linear-gradient(180deg,#fff,#fbfcff)}.forecast-title{font-weight:750}.forecast-meta{display:flex;gap:7px;flex-wrap:wrap;margin-top:7px}.version-row{border-top:1px solid var(--line);padding-top:10px;margin-top:10px}.version-head{display:flex;justify-content:space-between;gap:10px;align-items:center}.version-name{font-weight:700;font-size:12px}.version-top{font-size:13px;margin-top:5px}.dist{display:grid;gap:5px;margin-top:9px}.dist-row{display:grid;grid-template-columns:72px 1fr 45px;gap:7px;align-items:center;font-size:11px;color:var(--muted)}.dist-track{height:6px;background:#edf1f7;border-radius:99px;overflow:hidden}.dist-fill{height:100%;background:linear-gradient(90deg,var(--blue),#7d6bf2);border-radius:99px}.quality-models{display:grid;gap:14px}.quality-model{border:1px solid var(--line);border-radius:14px;padding:15px;background:#fbfcff}.quality-model-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px}.quality-model-title{font-weight:750}.quality-phases{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.quality-phase{border:1px solid var(--line);border-radius:11px;background:#fff;padding:12px}.quality-phase-head{display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:4px}.quality-phase-title{font-weight:750}.quality-table{width:100%;border-collapse:collapse;font-size:11px;margin-top:10px}.quality-table th{text-align:left;color:var(--muted);padding:0 5px 7px;text-transform:uppercase;font-size:9px}.quality-table td{padding:8px 5px;border-top:1px solid var(--line);vertical-align:top}.quality-table .num{text-align:right}.ci{font-variant-numeric:tabular-nums;white-space:nowrap}.calibration{display:grid;gap:4px;margin-top:10px;padding-top:9px;border-top:1px solid var(--line)}.cal-row{display:grid;grid-template-columns:44px 1fr 1fr 28px;gap:5px;align-items:center;font-size:9px;color:var(--muted)}.cal-bar{height:5px;background:#edf1f7;border-radius:99px;overflow:hidden}.cal-pred,.cal-actual{height:100%;border-radius:99px}.cal-pred{background:var(--blue)}.cal-actual{background:var(--green)}.sample-note{color:var(--amber);font-size:11px;margin-top:3px}.sample-note.good{color:var(--green)}.method{font-size:9px;color:var(--muted);margin-top:2px}.delta{color:var(--blue);font-size:11px}.forecast-empty{padding:20px;border:1px dashed var(--line);border-radius:12px;color:var(--muted);text-align:center}
.diagnostic-summary{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}.diagnostic-table{width:100%;border-collapse:collapse;font-size:11px}.diagnostic-table th{text-align:left;color:var(--muted);padding:0 7px 8px;text-transform:uppercase;font-size:9px}.diagnostic-table td{padding:9px 7px;border-top:1px solid var(--line);vertical-align:top}.diagnostic-table .num{text-align:right}.diagnostic-warn{color:var(--amber)}.diagnostic-split{width:100%;border-collapse:collapse;font-size:11px;margin-top:12px}.diagnostic-split th{text-align:left;color:var(--muted);padding:0 7px 7px;text-transform:uppercase;font-size:9px}.diagnostic-split td{padding:8px 7px;border-top:1px solid var(--line)}.diagnostic-split .num{text-align:right}
.footer{color:var(--muted);font-size:12px;text-align:center;margin-top:7px}.error{background:var(--red-soft);color:var(--red);padding:12px;border-radius:10px;font-size:13px;margin-top:10px}
@media(max-width:1050px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.layout{grid-template-columns:1fr}.decisions,.forecast-grid{grid-template-columns:1fr}}
@media(max-width:820px){.quality-phases{grid-template-columns:1fr}}
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
  <section class="card panel"><div class="panel-head"><div><h2>Trade diagnostics</h2><div class="panel-note">forecast correctness vs. bracket selection · sigma sensitivity is descriptive</div></div><span class="pill amber">PAPER ONLY</span></div><div id="diagnostics"></div></section>
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
    ['Realized P&L',compactMoney(p.realized_pnl_usd),'after order-level API allocation',Number(p.realized_pnl_usd||0)>=0?'good':'warn'],
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
 $('forecasts').innerHTML=events.map(e=>`<article class="forecast-card"><div class="forecast-title">${esc(e.event_title||e.event_id)}</div><div class="forecast-meta"><span class="pill gray">${esc(e.station_id||'station')}</span><span class="pill gray">${esc(e.observation_date||'')}</span><span class="pill ${e.outcome?'green':e.observed_max_c==null?'gray':'blue'}">${e.outcome?`resolved ${esc(e.outcome.winning_label||e.outcome.displayed_max+'°C')}`:`observed max ${e.observed_max_c==null?'—':esc(e.observed_max_c)+'°C'}${e.observed_max_at_utc?' at '+dateTime(e.observed_max_at_utc):''}`}</span></div>${(e.versions||[]).sort((a,b)=>shortVersion(a.algorithm_version).localeCompare(shortVersion(b.algorithm_version))).map(v=>{const top=v.top||{}, prev=v.previous_top||{}, sameTop=top.market_id&&prev.market_id&&top.market_id===prev.market_id, delta=sameTop&&top.probability!=null&&prev.probability!=null?Number(top.probability)-Number(prev.probability):null, meta=v.metadata||{}, profile=meta.station_correction||{}, viaEcmwf=String(v.source||'').includes('ecmwf'); const label=shortVersion(v.algorithm_version); const provenance=viaEcmwf?(v.model_init_time_utc&&v.model_published_at_utc?'official run metadata':'JSON archived · run/publication unknown'):v.source; const distribution=v.distribution||[], resolvedNote=e.outcome?` · ${top.market_id===e.outcome.winning_market_id?'correct bracket':'wrong bracket'} · error ${v.point_forecast_c==null?'—':Math.abs(Number(v.point_forecast_c)-Number(e.outcome.actual_max_c)).toFixed(2)+'°C'}`:''; return `<div class="version-row"><div class="version-head"><span class="version-name">${esc(label)}</span><span class="pill ${viaEcmwf?'blue':'gray'}">${esc(provenance)} · ${v.scenario_count}</span></div><div class="version-top"><b>${esc(top.label||'no distribution')}</b> ${top.probability==null?'—':(Number(top.probability)*100).toFixed(1)+'%'} ${delta==null?(prev.market_id&&top.market_id!==prev.market_id?`<span class="delta">top changed from ${esc(prev.label||prev.market_id)}</span>`:''):`<span class="delta">${delta>=0?'+':''}${(delta*100).toFixed(1)} pp</span>`}</div><div class="sub">mean member max ${v.point_forecast_c==null?'—':Number(v.point_forecast_c).toFixed(2)+'°C'} · ${esc(v.phase)} · ${dateTime(v.issued_at_utc)}${label.includes('v2')?` · ${profile.state==='fitted'?`correction fitted n=${profile.sample_count}`:'correction not fitted'}`:''}${resolvedNote}</div><div class="dist"><div class="sub">complete bracket distribution</div>${distribution.map(p=>`<div class="dist-row"><span>${esc(p.label)}</span><span class="dist-track"><span class="dist-fill" style="width:${Math.min(100,Number(p.probability||0)*100)}%"></span></span><span class="num">${(Number(p.probability||0)*100).toFixed(1)}%</span></div>`).join('')}</div></div>`}).join('')}</article>`).join('');
}
function renderQuality(d){
 const reports=d.forecast_comparison?.quality||[];
 if(!reports.length){$('quality').innerHTML='<div class="forecast-empty">No eligible production-cohort forecast opportunities yet. Legacy backfill is excluded from model ranking.</div>';return}
 const fmt=(metric,kind)=>{if(!metric||metric.value==null)return `<span>—</span><div class="method">${metric?.interval_status==='not_applicable_no_denominator'?'not applicable yet':'not estimable'}</div>`;const value=kind==='pct'?(Number(metric.value)*100).toFixed(1)+'%':kind==='temp'?Number(metric.value).toFixed(2)+'°C':Number(metric.value).toFixed(3);const ci=metric.ci95?(kind==='pct'?metric.ci95.map(x=>(Number(x)*100).toFixed(1)+'%'):kind==='temp'?metric.ci95.map(x=>Number(x).toFixed(2)+'°C'):metric.ci95.map(x=>Number(x).toFixed(3))):null;return `<span>${value}</span><div class="method">${ci?`95% CI ${ci[0]}–${ci[1]}`:'95% CI needs n≥2'}</div>`};
 const phaseCard=p=>{const m=p.metrics||{}, enough=p.sample_state==='monitoring_threshold_met', calibration=p.calibration||[];return `<section class="quality-phase"><div class="quality-phase-head"><span class="quality-phase-title">${esc(p.label)}</span><span class="pill ${enough?'green':'amber'}">resolved n=${p.event_count||0}</span></div><div class="sample-note ${enough?'good':''}">${esc(p.sample_message)}</div><div class="sub">eligible ${p.eligible_event_count||0} · ended ${p.eligible_ended_event_count||0}</div><div style="overflow-x:auto"><table class="quality-table"><thead><tr><th>Metric</th><th class="num">Estimate & uncertainty</th></tr></thead><tbody><tr><td>MAE</td><td class="num ci">${fmt(m.mae_c,'temp')}</td></tr><tr><td>Exact bracket</td><td class="num ci">${fmt(m.accuracy,'pct')}</td></tr><tr><td>Brier</td><td class="num ci">${fmt(m.brier,'score')}</td></tr><tr><td>ECE</td><td class="num ci">${fmt(m.ece,'score')}</td></tr><tr><td>Forecast coverage</td><td class="num ci">${fmt(m.forecast_coverage,'pct')}</td></tr><tr><td>Outcome coverage</td><td class="num ci">${fmt(m.outcome_coverage,'pct')}</td></tr><tr><td>Evaluated resolved</td><td class="num ci">${fmt(m.evaluation_coverage,'pct')}</td></tr></tbody></table></div><div class="calibration"><div class="sub">Calibration · blue predicted / green observed</div>${calibration.length?calibration.map(b=>`<div class="cal-row"><span>${(Number(b.lower)*100).toFixed(0)}–${(Number(b.upper)*100).toFixed(0)}%</span><span class="cal-bar"><span class="cal-pred" style="display:block;width:${Number(b.mean_confidence||0)*100}%"></span></span><span class="cal-bar"><span class="cal-actual" style="display:block;width:${Number(b.empirical_accuracy||0)*100}%"></span></span><span>n=${b.count}</span></div>`).join(''):'<div class="method">No populated calibration bins yet.</div>'}</div></section>`};
 $('quality').innerHTML=`<div class="quality-models">${reports.map(r=>`<article class="quality-model"><div class="quality-model-head"><div><div class="quality-model-title">${esc(shortVersion(r.algorithm_version))}</div><div class="sub">${esc(r.source)} · ${esc(r.model)}</div></div><span class="pill gray">95% intervals · rank only after n≥${r.minimum_reliable_events}</span></div><div class="quality-phases">${(r.phases||[]).map(phaseCard).join('')}</div></article>`).join('')}</div>`;
}
function renderDiagnostics(d){
 const report=d.forecast_diagnostics||{}, s=report.summary||{}, proxy=report.history_sigma_proxy||{}, rows=report.trades||[];
 if(!Object.keys(s).length && !rows.length){$('diagnostics').innerHTML='<div class="forecast-empty">Trade diagnostics are not available yet.</div>';return}
 const pill=(label,value,kind='gray')=>`<span class="pill ${kind}">${esc(label)} ${esc(value??'—')}</span>`;
 const split=s.strategy_pnl||{};
 const splitTable=Object.keys(split).length?`<div style="overflow-x:auto"><table class="diagnostic-split"><thead><tr><th>Entry version</th><th class="num">Settled</th><th class="num">Wins</th><th class="num">Losses</th><th class="num">Realized P&L</th><th class="num">Gross before allocated API</th></tr></thead><tbody>${Object.entries(split).map(([version,x])=>`<tr><td><b>${esc(version)}</b></td><td class="num">${x.settled_count}</td><td class="num">${x.wins}</td><td class="num">${x.losses}</td><td class="num ${Number(x.realized_pnl_usd)>=0?'positive':'negative'}">${money(x.realized_pnl_usd)}</td><td class="num">${money(x.gross_trade_pnl_usd)}</td></tr>`).join('')}</tbody></table></div>`:'';
 const winners=s.winning_trades||[];
 const winnerNote=winners.length?`<div class="sub" style="margin-top:10px">Winning entry: ${winners.map(x=>`${esc(x.event_title||x.event_id)} · ${esc(x.strategy_version)} · ${money(x.realized_pnl_usd)} · forecast provenance ${esc(x.sensitivity_status||'unknown')}`).join(' · ')}</div>`:'';
 const summary=`<div class="diagnostic-summary">${pill('settled',s.settled_trade_count)}${pill('forecast available',s.forecast_available_count)}${pill('forecast correct',s.forecast_correct_count,'green')}${pill('trade wrong',s.trade_incorrect_count,'amber')}${pill('sigma-created',s.sigma_created_signal_count,'amber')}${pill('double-conditioned',s.double_conditioning_created_signal_count,'amber')}${pill('sigma proxy',proxy.value_c+'°C',proxy.state==='insufficient_history'?'amber':'blue')}</div><div class="sub">${esc(proxy.message||'')}</div>${winnerNote}${splitTable}`;
 const table=rows.length?`<div style="overflow-x:auto;margin-top:12px"><table class="diagnostic-table"><thead><tr><th>Event</th><th>Bought</th><th>Top forecast</th><th>Winner</th><th>σ=1.5</th><th>Raw</th><th>Result</th></tr></thead><tbody>${rows.map(r=>`<tr><td><b>${esc(r.event_title||r.event_id)}</b><div class="sub">order ${esc(r.paper_order_id)} · ${esc(r.strategy_version)}</div></td><td>${esc(r.bought_label||r.bought_market_id||'—')}<div class="sub">p ${r.bought_probability==null?'—':(Number(r.bought_probability)*100).toFixed(1)+'%'}</div></td><td>${esc(r.top_label||'—')}<div class="sub">p ${r.top_probability==null?'—':(Number(r.top_probability)*100).toFixed(1)+'%'}</div></td><td>${esc(r.winning_label||'—')}<div class="sub">${r.actual_max_c==null?'—':esc(r.actual_max_c)+'°C'}</div></td><td>${r.current_sigma_probability==null?'—':(Number(r.current_sigma_probability)*100).toFixed(1)+'%'}<div class="sub">${r.current_sigma_qualifies==null?'':r.current_sigma_qualifies?'qualifies':'rejects'}</div></td><td>${r.raw_probability==null?'—':(Number(r.raw_probability)*100).toFixed(1)+'%'}<div class="sub">${r.sigma_created_signal?'created signal':''}</div></td><td class="${r.forecast_vs_trade==='FORECAST_CORRECT_TRADE_WRONG'?'diagnostic-warn':''}">${esc(r.forecast_vs_trade||'—')}<div class="sub">P&L ${r.realized_pnl_usd==null?'—':money(r.realized_pnl_usd)}</div></td></tr>`).join('')}</tbody></table></div>`:'<div class="forecast-empty">No settled paper trades yet.</div>';
 $('diagnostics').innerHTML=summary+table;
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
async function refresh(){try{const response=await fetch('/api/dashboard',{cache:'no-store'});const data=await response.json();$('updated').textContent='Updated '+dateTime(data.generated_at);renderMetrics(data);renderWindow(data);renderForecasts(data);renderQuality(data);renderDiagnostics(data);renderPositions(data);renderSources(data);renderTimeline(data);renderDecisions(data)}catch(error){$('updated').textContent='Connection error';console.error(error)}}
refresh();setInterval(refresh,30000);
</script>
</body>
</html>"""


def serve_dashboard(storage: Storage, *, host: str, port: int) -> None:
    # Scanner/observer owns schema migrations. The dashboard opens SQLite in
    # read-only URI mode and HTTP GET handlers can never mutate runtime state.
    forecast_store = ForecastStore(storage.path, read_only=True)
    comparison_cache = ComparisonCache(storage.path)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/health":
                self._send_json({"ok": True})
            elif path in {"/api/dashboard", "/api/status"}:
                self._send_json(storage.dashboard_payload(forecast_store=forecast_store))
            elif path == "/api/comparison":
                try:
                    self._send_json(comparison_cache.get())
                except Exception as error:
                    # Comparison is diagnostic-only. A transient read error
                    # must not affect the observer or expose a traceback.
                    self._send_json(
                        {
                            "version": COMPARISON_VERSION,
                            "error": str(error),
                            "promotion": {"status": "unavailable", "v2_promoted": False},
                        },
                        status=503,
                    )
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

        def _send_json(self, payload: dict[str, Any], *, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status)
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
