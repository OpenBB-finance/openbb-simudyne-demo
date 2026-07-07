'use strict';

// Defaults are fetched from /simudyne_config at boot (see loadConfig).
var DEFAULT_SYMBOL = '';
var DEFAULT_DATE = '';

// ── Refs ─────────────────────────────────────
var tapeTbody = document.getElementById('tape-tbody');
var bookTbody = document.getElementById('book-tbody');
var bk = {};
var fragBook = document.createDocumentFragment();
for (var _i = 1; _i <= 10; _i++) {
  var row = document.createElement('tr');
  row.id = 'brow-' + _i;
  row.innerHTML = '<td id="bsz-' + _i + '" class="r"></td>' +
    '<td id="bp-' + _i + '" class="r bp"></td>' +
    '<td class="sep">&#x7c;</td>' +
    '<td id="ap-' + _i + '" class="l ap"></td>' +
    '<td id="asz-' + _i + '" class="l"></td>';
  fragBook.appendChild(row);
}
bookTbody.appendChild(fragBook);
for (var _i = 1; _i <= 10; _i++) {
  bk['bsz' + _i] = document.getElementById('bsz-' + _i);
  bk['bp'  + _i] = document.getElementById('bp-'  + _i);
  bk['ap'  + _i] = document.getElementById('ap-'  + _i);
  bk['asz' + _i] = document.getElementById('asz-' + _i);
  bk['row' + _i] = document.getElementById('brow-' + _i);
}

// ── State ────────────────────────────────────
var ws = null, meta = null, agg = false, paused = false;
var chart = null, priceLine = null;
var lastBarClose = null;
var currentFrame = 0, totalFrames = 0;
var frameTrades = {};
var framePrices = {};
var lastRenderedChartFrame = -1;
var isScrubbing = false;
var speedOptions = [1, 2, 4, 8, 10, 25, 50, 100];
var defaultSpeed = 10;
var speedIndex = speedOptions.indexOf(defaultSpeed);
var basePlaybackMs = 120;
var paramCatalog = {
  symbols: [DEFAULT_SYMBOL],
  dates: [DEFAULT_DATE],
  scenarios: ['flash_crash', 'buy_panic', 'normal'],
  runs: ['0', 'all']
};

function pickValid(value, options, fallback) {
  var list = Array.isArray(options) ? options : [];
  if (!list.length) return value || fallback;
  if (value && list.indexOf(value) !== -1) return value;
  if (fallback && list.indexOf(fallback) !== -1) return fallback;
  return list[0];
}

function secToClock(seconds) {
  var s = Math.max(0, Math.floor(seconds));
  var m = Math.floor(s / 60);
  var r = s % 60;
  return String(m).padStart(2, '0') + ':' + String(r).padStart(2, '0');
}

function updateTimeline(i, n, timeLabel) {
  currentFrame = i;
  totalFrames = n;
  var slider = document.getElementById('timeline');
  slider.max = String(Math.max(0, n - 1));
  if (!isScrubbing) slider.value = String(i);
  document.getElementById('timeline-now').textContent = timeLabel || secToClock(i);
  document.getElementById('timeline-total').textContent = meta && meta.end_time ? meta.end_time.slice(11, 19) : '—';
}

function sendWsAction(payload) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(payload));
}

function applySpeed(speed) {
  document.getElementById('btn-ff').textContent = 'FF ' + speed + 'x';
  var cadence = speed <= 2 ? Math.max(50, Math.round(basePlaybackMs / speed)) : 80;
  sendWsAction({ action: 'set_speed', playback_ms: cadence, frame_step: speed });
}

// ── Theme (light/dark, driven by OpenBB Workspace) ────────
// Workspace appends ?theme=dark|light to the iframe URL and reloads it on
// toggle; we also honour a theme sent via postMessage as a fallback.
function currentTheme() {
  var t = (new URL(window.location.href).searchParams.get('theme') || '').toLowerCase();
  if (t === 'dark' || t === 'light') return t;
  return document.documentElement.getAttribute('data-theme') || 'light';
}
function chartColors(theme) {
  return theme === 'dark'
    ? { text: '#9aa4af', grid: 'rgba(255,255,255,.07)', border: 'rgba(255,255,255,.14)', line: '#60a5fa' }
    : { text: '#5c6773', grid: 'rgba(23,33,43,.06)', border: 'rgba(23,33,43,.12)', line: '#1d4ed8' };
}
function applyTheme(theme) {
  var t = theme === 'dark' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', t);
  if (chart) {
    var c = chartColors(t);
    chart.applyOptions({
      layout: { textColor: c.text },
      grid: { vertLines: { color: c.grid }, horzLines: { color: c.grid } },
      rightPriceScale: { borderColor: c.border },
      timeScale: { borderColor: c.border }
    });
    if (priceLine) priceLine.applyOptions({ color: c.line });
  }
}

// ── Chart (lightweight-charts) ────────────────────────────
function initChart() {
  var wrap = document.getElementById('chart-wrap');
  if (chart) { chart.remove(); chart = null; }
  var tc = chartColors(currentTheme());
  chart = LightweightCharts.createChart(wrap, {
    width: wrap.clientWidth,
    height: 320,
    layout: { background: { type: 'solid', color: 'rgba(0,0,0,0)' }, textColor: tc.text },
    localization: {
      // Render times in UTC so chart axis matches stream timestamps/KPI clock.
      timeFormatter: function(timestamp) {
        var d = new Date(timestamp * 1000);
        return String(d.getUTCHours()).padStart(2, '0') + ':' +
          String(d.getUTCMinutes()).padStart(2, '0') + ':' +
          String(d.getUTCSeconds()).padStart(2, '0');
      }
    },
    grid: { vertLines: { color: tc.grid }, horzLines: { color: tc.grid } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: tc.border },
    timeScale: { borderColor: tc.border, timeVisible: true, secondsVisible: true },
    handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: true },
    handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true }
  });
  priceLine = chart.addLineSeries({
    color: tc.line,
    lineWidth: 2,
    crosshairMarkerVisible: true,
    priceLineVisible: true,
    lastValueVisible: true,
    title: 'Actual Price'
  });
  lastBarClose = null;
  framePrices = {};
  lastRenderedChartFrame = -1;
  new ResizeObserver(function() { if (chart) chart.applyOptions({ width: wrap.clientWidth }); }).observe(wrap);
}

function toT(iso) {
  if (!iso) return 0;
  var normalized = /[zZ]$/.test(iso) ? iso : (iso + 'Z');
  return Math.floor(Date.parse(normalized) / 1000);
}
function fmt(v, d) {
  if (v == null || v !== v) return '—';
  return Number(v).toFixed(d == null ? 2 : d);
}

// ── Clear all display state (called on loop restart) ─────────────
function clearAll() {
  if (priceLine) priceLine.setData([]);
  lastBarClose = null;
  framePrices = {};
  lastRenderedChartFrame = -1;
  currentFrame = 0;
  totalFrames = 0;
  frameTrades = {};
  updateTimeline(0, 1, '00:00');
  tapeTbody.innerHTML = '';
  for (var i = 1; i <= 10; i++) {
    bk['bsz'+i].textContent = '';
    bk['bp'+i].textContent  = '';
    bk['ap'+i].textContent  = '';
    bk['asz'+i].textContent = '';
  }
}

// ── Per-frame update ─────────────────────────────────
function onFrame(f, trades, frameIdx, totalFrames) {
  if (frameIdx === 0 && totalFrames > 1) clearAll();
  updateTimeline(frameIdx, totalFrames, f.time.slice(11, 19));

  var close = Number(f.day_close);
  if (!(close === close)) close = Number(f.last_trade_price);
  if (!(close === close)) close = Number(f.market_price);

  var dayOpen = Number(f.day_open);
  if (!(dayOpen === dayOpen)) dayOpen = close;
  var dayHigh = Number(f.day_high);
  if (!(dayHigh === dayHigh)) dayHigh = close;
  var dayLow = Number(f.day_low);
  if (!(dayLow === dayLow)) dayLow = close;
  var dayVolume = Number(f.day_volume);
  if (!(dayVolume === dayVolume)) dayVolume = Number(f.trade_volume || 0);

  // KPIs — textContent only, no DOM creation
  document.getElementById('v-bid').textContent    = fmt(f.nbbo_bid);
  document.getElementById('v-ask').textContent    = fmt(f.nbbo_ask);
  document.getElementById('v-spread').textContent = fmt(f.spread, 3);
  document.getElementById('v-market').textContent = fmt(close);
  document.getElementById('v-tps').textContent    = String(f.trade_count || 0);
  document.getElementById('s-tps').textContent    = 'Vol ' + (f.trade_volume || 0).toLocaleString();
  document.getElementById('v-open').textContent   = fmt(dayOpen);
  document.getElementById('v-high').textContent   = fmt(dayHigh);
  document.getElementById('v-low').textContent    = fmt(dayLow);
  document.getElementById('v-close').textContent  = fmt(close);
  document.getElementById('v-volume').textContent = String(dayVolume.toLocaleString());
  document.getElementById('prog').textContent     = (frameIdx + 1) + ' / ' + totalFrames;
  if (meta) {
    var rl = agg ? ('avg ' + meta.runs_included + ' runs') : ('run ' + meta.run);
    document.getElementById('s-bid').textContent   = rl;
    document.getElementById('s-ask').textContent   = rl;
    document.getElementById('s-market').textContent = 'last actual trade';
    document.getElementById('clock').textContent   = meta.symbol + ' ' + meta.date + ' ' + f.time.slice(11);
  }

  // Order book — update pre-built cells in place
  for (var i = 1; i <= 10; i++) {
    var bp  = i === 1 ? f.nbbo_bid  : f['bid_price_' + i];
    var bs  = f['bid_size_' + i];
    var ap  = i === 1 ? f.nbbo_ask  : f['ask_price_' + i];
    var as_ = f['ask_size_' + i];
    if (bp == null && ap == null) {
      bk['row'+i].style.display = 'none';
    } else {
      bk['row'+i].style.display = '';
      bk['bsz'+i].textContent = bs  != null ? bs.toLocaleString()  : '';
      bk['bp' +i].textContent = bp  != null ? bp.toFixed(2)        : '';
      bk['ap' +i].textContent = ap  != null ? ap.toFixed(2)        : '';
      bk['asz'+i].textContent = as_ != null ? as_.toLocaleString() : '';
    }
  }

  // Chart — rebuild on backward/out-of-order navigation, append on forward playback
  var t = toT(f.time);
  if (priceLine && close === close) {
    framePrices[frameIdx] = { time: t, value: close };
    if (frameIdx <= lastRenderedChartFrame) {
      rebuildChartToFrame(frameIdx);
    } else {
      priceLine.update(framePrices[frameIdx]);
    }
    lastRenderedChartFrame = frameIdx;
    lastBarClose = close;
  }

  frameTrades[frameIdx] = trades && trades.length ? trades.slice() : [];
  renderTapeForFrame(frameIdx);
}

function rebuildChartToFrame(frameIdx) {
  if (!priceLine) return;
  var series = [];
  for (var idx = 0; idx <= frameIdx; idx++) {
    var point = framePrices[idx];
    if (point) series.push(point);
  }
  priceLine.setData(series);
}

function renderTapeForFrame(frameIdx) {
  var out = document.createDocumentFragment();
  var remaining = 25;

  for (var idx = frameIdx; idx >= 0 && remaining > 0; idx--) {
    var bucket = frameTrades[idx];
    if (!bucket || !bucket.length) continue;
    var sorted = bucket.slice().sort(function(a, b) { return b.time.localeCompare(a.time); });
    for (var j = 0; j < sorted.length && remaining > 0; j++) {
      var row = document.createElement('tr');
      var tr = sorted[j];
      if (agg) {
        row.innerHTML = '<td>' + tr.time.slice(11) + '</td><td>—</td><td>' +
          fmt(tr.trade_vwap) + '</td><td>' + (tr.trade_volume || 0).toLocaleString() +
          '</td><td class="dim">×' + (tr.trade_count || '') + '</td>';
      } else {
        row.className = tr.side || '';
        row.innerHTML = '<td>' + tr.time.slice(11, 23) + '</td><td>' + (tr.side || '—') +
          '</td><td>' + fmt(tr.price) + '</td><td>' + (tr.size || 0).toLocaleString() +
          '</td><td class="dim">' + (tr.order_id != null ? tr.order_id : '') + '</td>';
      }
      out.appendChild(row);
      remaining -= 1;
    }
  }

  tapeTbody.innerHTML = '';
  tapeTbody.appendChild(out);
}

// ── WebSocket ────────────────────────────────────
function readParams() {
  var u = new URL(window.location.href);
  var symbol = pickValid(u.searchParams.get('symbol'), paramCatalog.symbols, DEFAULT_SYMBOL);
  var date = pickValid(u.searchParams.get('date'), paramCatalog.dates, DEFAULT_DATE);
  var scenario = pickValid(u.searchParams.get('scenario'), paramCatalog.scenarios, 'flash_crash');
  var runRaw = u.searchParams.get('run') || '0';
  var run = pickValid(runRaw, paramCatalog.runs, '0');
  return {
    symbol:      symbol,
    date:        date,
    scenario:    scenario,
    run:         run,
    playback_ms: Number(u.searchParams.get('playback_ms') || 120)
  };
}
function syncUrl(p) {
  var u = new URL(window.location.href);
  u.searchParams.set('symbol',      p.symbol);
  u.searchParams.set('date',        p.date);
  u.searchParams.set('scenario',    p.scenario);
  u.searchParams.set('run',         p.run);
  u.searchParams.set('playback_ms', String(p.playback_ms));
  window.history.replaceState({}, '', u);
}

function connect(params) {
  if (ws) { ws.onclose = null; ws.onerror = null; ws.close(); ws = null; }
  clearAll();
  syncUrl(params);
  basePlaybackMs = Number(params.playback_ms) || 120;
  speedIndex = speedOptions.indexOf(defaultSpeed);
  if (speedIndex < 0) speedIndex = 0;
  paused = false;
  document.getElementById('btn-pause').textContent = 'Pause';
  document.getElementById('btn-ff').textContent = 'FF ' + speedOptions[speedIndex] + 'x';
  var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  var url   = proto + '//' + location.host + '/ws/simudyne_stream' +
    '?symbol='      + encodeURIComponent(params.symbol) +
    '&date='        + encodeURIComponent(params.date) +
    '&scenario='    + encodeURIComponent(params.scenario) +
    '&run='         + encodeURIComponent(params.run) +
    '&playback_ms=' + encodeURIComponent(params.playback_ms);
  document.getElementById('hdr-meta').textContent = 'Connecting…';
  ws = new WebSocket(url);
  ws.onmessage = function(ev) {
    var msg = JSON.parse(ev.data);
    if (msg.type === 'init') {
      meta = msg.meta;
      agg  = meta.mode === 'aggregate';
      totalFrames = msg.total_frames || 0;
      updateTimeline(0, Math.max(1, totalFrames), meta.start_time ? meta.start_time.slice(11, 19) : '00:00');
      applySpeed(speedOptions[speedIndex]);
      document.getElementById('hdr-meta').textContent =
        meta.scenario + ' / run ' + meta.run + ' · ' + meta.n_runs_available + ' runs';
      document.getElementById('book-mode').textContent = 'loaded run vs scenario avg';
      document.getElementById('tape-mode').textContent = 'loaded run vs scenario avg';
    } else if (msg.type === 'frame') {
      onFrame(msg.f, msg.t, msg.i, msg.n);
    } else if (msg.type === 'error') {
      document.getElementById('hdr-meta').textContent = 'Error: ' + msg.message;
    }
  };
  ws.onclose = function() { document.getElementById('hdr-meta').textContent = 'Disconnected'; };
  ws.onerror = function() { document.getElementById('hdr-meta').textContent = 'Connection error'; };
}

// ── Controls ────────────────────────────────────
document.getElementById('btn-pause').addEventListener('click', function() {
  paused = !paused;
  document.getElementById('btn-pause').textContent = paused ? 'Play' : 'Pause';
  sendWsAction({ action: paused ? 'pause' : 'resume' });
});
document.getElementById('btn-ff').addEventListener('click', function() {
  speedIndex = (speedIndex + 1) % speedOptions.length;
  var speed = speedOptions[speedIndex];
  applySpeed(speed);
});
document.getElementById('btn-rst').addEventListener('click', function() {
  sendWsAction({ action: 'restart' });
  updateTimeline(0, Math.max(1, totalFrames), '00:00');
});

var timeline = document.getElementById('timeline');
timeline.addEventListener('pointerdown', function() { isScrubbing = true; });
timeline.addEventListener('pointerup', function() {
  isScrubbing = false;
  var idx = Number(timeline.value || 0);
  updateTimeline(idx, Math.max(1, totalFrames), secToClock(idx));
  sendWsAction({ action: 'seek', index: idx });
});
timeline.addEventListener('input', function() {
  var idx = Number(timeline.value || 0);
  document.getElementById('timeline-now').textContent = secToClock(idx);
});
timeline.addEventListener('change', function() {
  var idx = Number(timeline.value || 0);
  sendWsAction({ action: 'seek', index: idx });
});

// ── OpenBB params ─────────────────────────────────
var OBB_TARGET = window.top || window.parent;

// Tables this widget exposes to Workspace (and the agent chat) over the iframe
// protocol: openbb-connect advertises them, openbb-request pulls their records.
var SUBWIDGETS = [
  { widgetId: 'simudyne_frames', name: 'Simudyne Frames (per-second)',
    description: 'Per-second NBBO, mid/spread, trade count/volume/VWAP, cumulative day OHLC + volume, and 10-level book for the loaded symbol/date/scenario/run.',
    category: 'Simudyne', dataType: 'table' },
  { widgetId: 'simudyne_trades', name: 'Simudyne Trades',
    description: 'Individual trade prints (time, side, price, size, order_id) for the loaded selection.',
    category: 'Simudyne', dataType: 'table' },
  { widgetId: 'simudyne_run_stats', name: 'Simudyne Run Stats',
    description: 'KPIs for the loaded run alongside the scenario (all-runs) average.',
    category: 'Simudyne', dataType: 'table' }
];

function selectionQuery() {
  var p = readParams();
  return new URLSearchParams({ symbol: p.symbol, date: p.date, scenario: p.scenario, run: p.run }).toString();
}

// Fetch the requested table(s) for the current selection and post them back as
// openbb-data. widgetId == null means "send everything".
async function sendWidgetData(widgetId) {
  if (OBB_TARGET === window) return;
  var ids = widgetId == null ? SUBWIDGETS.map(function(w) { return w.widgetId; }) : [widgetId];
  if (!ids.some(function(id) { return id.indexOf('simudyne_') === 0; })) return;
  var qs = selectionQuery();
  var stream = null, stats = null;
  var needStream = ids.indexOf('simudyne_frames') !== -1 || ids.indexOf('simudyne_trades') !== -1;
  try {
    if (needStream) {
      var r = await fetch('/simudyne_stream_data?' + qs);
      if (r.ok) stream = await r.json();
    }
    if (ids.indexOf('simudyne_run_stats') !== -1) {
      var s = await fetch('/simudyne_stats?' + qs);
      if (s.ok) stats = await s.json();
    }
  } catch (_) {}
  ids.forEach(function(id) {
    var data = [];
    if (id === 'simudyne_frames') data = (stream && stream.frames) || [];
    else if (id === 'simudyne_trades') data = (stream && stream.trade_rows) || [];
    else if (id === 'simudyne_run_stats') data = stats || [];
    else return;
    OBB_TARGET.postMessage({ type: 'openbb-data', widgetId: id, dataType: 'table', data: data }, '*');
  });
}

async function loadConfig() {
  try {
    var res = await fetch('/simudyne_config');
    if (!res.ok) return;
    var c = await res.json();
    if (c.default_symbol) DEFAULT_SYMBOL = c.default_symbol;
    if (c.default_date) DEFAULT_DATE = c.default_date;
    if (Array.isArray(c.scenarios) && c.scenarios.length) paramCatalog.scenarios = c.scenarios;
    paramCatalog.symbols = [DEFAULT_SYMBOL];
    paramCatalog.dates = [DEFAULT_DATE];
  } catch (_) {}
}

async function registerOpenBBParams() {
  try {
    var u0 = new URL(window.location.href);
    var sym0 = u0.searchParams.get('symbol') || DEFAULT_SYMBOL;
    var dt0 = u0.searchParams.get('date') || DEFAULT_DATE;
    var scn0 = u0.searchParams.get('scenario') || 'flash_crash';
    async function _opts(params) {
      var q = new URLSearchParams(params).toString();
      var res = await fetch('/simudyne_param_options' + (q ? '?' + q : ''));
      if (!res.ok) return [];
      var list = await res.json();
      return Array.isArray(list) ? list.map(function(o) { return o.value; }) : [];
    }
    var syms = await _opts({});
    var dts = await _opts({ symbol: sym0 });
    var scns = await _opts({ symbol: sym0, date: dt0 });
    var rns = await _opts({ symbol: sym0, date: dt0, scenario: scn0 });
    if (syms.length) paramCatalog.symbols = syms;
    if (dts.length) paramCatalog.dates = dts;
    if (scns.length) paramCatalog.scenarios = scns;
    if (rns.length) paramCatalog.runs = rns;
  } catch (_) {}

  if (OBB_TARGET !== window) {
    OBB_TARGET.postMessage({
      type: 'openbb-connect',
      widgets: [
        { widgetId: 'simudyne_market_stream_iframe', name: 'Simudyne Market Stream',
          description: 'Live order book, tick chart, and trade tape.', category: 'Simudyne', dataType: 'iframe' }
      ].concat(SUBWIDGETS),
      params: [
        { paramName: 'symbol', label: 'Symbol', type: 'text', description: 'Ticker symbol.', value: DEFAULT_SYMBOL },
        { paramName: 'date', label: 'Date', type: 'text', description: 'Trading date.', value: DEFAULT_DATE },
        { paramName: 'scenario', label: 'Scenario', type: 'text', description: 'Scenario.', value: 'flash_crash' },
        { paramName: 'run', label: 'Run', type: 'text', description: "Run index or 'all'.", value: '0' }
      ]
    }, '*');
  }
}
window.addEventListener('message', function(ev) {
  var d = ev.data;
  if (!d) return;
  // Theme can arrive on its own or embedded in a params update.
  var incomingTheme = d.theme || (d.params && d.params.theme);
  if (incomingTheme) applyTheme(incomingTheme);
  if (d.type === 'openbb-request') { sendWidgetData(d.widgetId != null ? d.widgetId : null); return; }
  if (d.type !== 'openbb-params-update') return;
  var p = d.params || {}, cur = readParams();
  connect({
    symbol:      p.symbol      || cur.symbol,
    date:        p.date        || cur.date,
    scenario:    p.scenario    || cur.scenario,
    run:         String(p.run  != null ? p.run : cur.run),
    playback_ms: Number(p.playback_ms != null ? p.playback_ms : cur.playback_ms)
  });
});

// ── Boot ────────────────────────────────────────
applyTheme(currentTheme());
loadConfig().finally(function() {
  initChart();
  registerOpenBBParams().finally(function() {
    connect(readParams());
  });
});
