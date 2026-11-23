'use strict';
(() => {
/* ============================================================
   Monitor de Condición de Motor — corriente, temperatura, vibración
   Fuente "En vivo": MQTT (mismo canal y formato que Monitor_ADXL345.ino)
   Fuente "Demo": simulador local, marcado como SIMULADO en los datos
   ============================================================ */
const $ = id => document.getElementById(id);
const fin = Number.isFinite;
const TOPIC = 'uni-lab/e7b914ca52/adxl345';
const TOPIC_I = TOPIC + '/corriente';
const BROKER = 'wss://test.mosquitto.org:8081/mqtt';
const MAX_ROWS = 300000;               // ~8 h a 10 muestras/s
const ESTADOS_FW = ['Calibrando','En reposo','Sin referencia','Fase incierta','Motor normal (estimado)',
  'Posible desalineación','Tomando referencia','Revisar corriente','Sin ADXL345'];
const FASES = ['Indeterminada','Estrella (Y)','Delta (Δ)'];
const DEF = {vibMed:10, vibAlt:25, axial:'X', ratio:0.5, tWarn:60, tHigh:75, tCrit:90, iNom:2, sound:false};

const store = {
  get(k){ try { return localStorage.getItem(k); } catch { return null; } },
  set(k,v){ try { localStorage.setItem(k,v); } catch {} }
};
let cfg = {...DEF};
try { Object.assign(cfg, JSON.parse(store.get('mc-cfg') || '{}')); } catch {}

/* ---------- niveles ---------- */
const RANK = {off:0, ok:1, warn:2, alarm:3, crit:4};
const VIB = {baja:['Baja','ok'], media:['Media','warn'], alta:['Alta','alarm']};
const TMP = {normal:['Normal','ok'], precaucion:['Precaución','warn'], alta:['Alta','alarm'], critica:['Crítica','crit']};
const CUR = {detenido:['Detenido','off'], normal:['Normal','ok'], sobre:['Sobre nominal','warn']};
const vibLevel = m => !fin(m) ? null : m >= cfg.vibAlt ? 'alta' : m >= cfg.vibMed ? 'media' : 'baja';
const tempLevel = T => !fin(T) ? null : T >= cfg.tCrit ? 'critica' : T >= cfg.tHigh ? 'alta' : T >= cfg.tWarn ? 'precaucion' : 'normal';
// Histéresis del 3 % al salir de "sobre nominal" para evitar avisos intermitentes cerca de In
const curLevel = (I,u) => !fin(I) || u !== 'A' ? null : I < 0.1 ? 'detenido' :
  (cfg.iNom > 0 && I > cfg.iNom * (st.lv.cur === 'sobre' ? 0.97 : 1)) ? 'sobre' : 'normal';

/* ---------- estado ---------- */
const rows = [], events = [];
const st = {
  mode:'live', paused:false, pauseAt:0, win:60000,
  vibEma:null, ax:[null,null,null], misCnt:0, mis:false,
  lv:{vib:null, temp:null, cur:null, mis:false},
  peak:null, tmax:null, lastAt:0, missing:0, lastSeq:-1, session:''
};
let dirty = true;
const hover = {};

/* ---------- utilidades ---------- */
const fmt = (v,d=1) => fin(v) ? v.toFixed(d) : '—';
const pad = (n,l=2) => String(n).padStart(l,'0');
const hms = ts => { const d = new Date(ts); return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`; };
const stamp = ts => { const d = new Date(ts);
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${hms(ts)}.${pad(d.getMilliseconds(),3)}`; };
const fileStamp = () => { const d = new Date();
  return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`; };
const cssVar = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
let C = {};
function readColors(){
  for (const k of ['ink','muted','grid','line','panel','cx','cy','cz','ct','ci','ok','warn','alarm','crit','plot','onda','envol']) C[k] = cssVar('--'+k);
}
function setBadge(el, txt, lv){ el.textContent = txt; el.className = 'badge lv-' + lv; }

/* ---------- ingesta común ---------- */
function ingest(s){
  const now = Date.now();
  const xyz = [s.x, s.y, s.z];
  const mag = xyz.every(fin) ? Math.hypot(...xyz) : null;
  if (fin(mag)) {
    st.vibEma = st.vibEma == null ? mag : st.vibEma + 0.3 * (mag - st.vibEma);
    xyz.forEach((v,i) => st.ax[i] = st.ax[i] == null ? v : st.ax[i] + 0.2 * (v - st.ax[i]));
  }
  // Desalineación: vibración axial predominante sobre la radial (criterio clásico de diagnóstico)
  // o aviso del firmware (estado 5). Histéresis para evitar parpadeo.
  const ai = 'XYZ'.indexOf(cfg.axial);
  let ratio = null;
  if (fin(mag) && st.ax.every(fin)) {
    const radial = Math.max(...st.ax.filter((_,i) => i !== ai));
    ratio = st.ax[ai] / Math.max(radial, 0.5);
  }
  const cond = (fin(ratio) && st.vibEma >= cfg.vibMed && ratio >= cfg.ratio) || s.estado === 5;
  st.misCnt = cond ? Math.min(st.misCnt + 1, 15) : Math.max(st.misCnt - 1, 0);
  if (!st.mis && st.misCnt >= 10) st.mis = true;
  else if (st.mis && st.misCnt === 0) st.mis = false;

  if (fin(s.I)) st.peak = Math.max(st.peak ?? 0, s.I);
  if (fin(s.T)) st.tmax = Math.max(st.tmax ?? -Infinity, s.T);

  const row = {
    ts:now, origen:s.origen, sesion:s.sess, seq:s.seq, tms:s.t,
    x:s.x, y:s.y, z:s.z, mag, vf: fin(mag) ? st.vibEma : null, ratio,
    I:s.I, Ipk:s.Ipk, unit:s.unit, T:s.T, V:s.V, ang:s.ang, fase:s.fase, estado:s.estado,
    vibLv: fin(mag) ? vibLevel(st.vibEma) : null, tLv: tempLevel(s.T), cLv: curLevel(s.I, s.unit), mis: st.mis
  };
  rows.push(row);
  if (rows.length > MAX_ROWS) rows.splice(0, rows.length - MAX_ROWS);
  st.lastAt = now;
  transitions(row);
  updateUI(row);
  dirty = true;
}

/* ---------- eventos / alarmas ---------- */
function logEvent(variable, lv, sev, msg){
  events.push({ts:Date.now(), variable, lv, sev, msg, ack: sev === 'ok' || sev === 'off'});
  if (events.length > 2000) events.shift();
  if (cfg.sound && RANK[sev] >= RANK.alarm) beep(sev === 'crit' ? 880 : 660);
  renderLog();
}
function transitions(r){
  const L = st.lv;
  if (r.vibLv && r.vibLv !== L.vib) {
    const [txt, sev] = VIB[r.vibLv];
    const msg = {baja:'Vibración baja — operación estable',
      media:`Vibración media (≥ ${cfg.vibMed} mg) — vigilar tendencia`,
      alta:`Vibración alta (≥ ${cfg.vibAlt} mg) — inspeccionar anclaje, rodamientos y balanceo`}[r.vibLv];
    if (L.vib !== null || sev !== 'ok') logEvent('Vibración', txt, sev, msg);
    L.vib = r.vibLv;
  }
  if (r.tLv && r.tLv !== L.temp) {
    const [txt, sev] = TMP[r.tLv];
    const msg = {normal:'Temperatura dentro del rango normal',
      precaucion:`Temperatura en precaución (≥ ${cfg.tWarn} °C) — vigilar carga y ventilación`,
      alta:`Temperatura alta (≥ ${cfg.tHigh} °C) — reducir carga / verificar ventilación`,
      critica:`Temperatura crítica (≥ ${cfg.tCrit} °C) — detener el motor`}[r.tLv];
    if (L.temp !== null || sev !== 'ok') logEvent('Temperatura', txt, sev, msg + ` · ${fmt(r.T)} °C`);
    L.temp = r.tLv;
  }
  if (r.cLv && r.cLv !== L.cur) {
    const [txt, sev] = CUR[r.cLv];
    if (L.cur !== null || r.cLv === 'sobre') {
      const msg = {detenido:'Motor detenido (corriente < 0.1 A)', normal:'Corriente en rango normal',
        sobre:`Corriente sobre la nominal (> ${cfg.iNom} A)`}[r.cLv];
      logEvent('Corriente', txt, sev, msg + ` · ${fmt(r.I,2)} A`);
    }
    L.cur = r.cLv;
  }
  if (r.mis !== L.mis) {
    if (r.mis) logEvent('Eje', 'Desalineado', 'alarm',
      `Eje desalineado — vibración axial (${cfg.axial}) predominante` + (fin(r.ratio) ? `, relación ${r.ratio.toFixed(2)}` : '') +
      (r.estado === 5 ? ' · confirmado por firmware' : ''));
    else logEvent('Eje', 'Alineado', 'ok', 'Alineación normalizada');
    L.mis = r.mis;
  }
}
let actx = null;
function beep(f){
  try {
    actx = actx || new (window.AudioContext || window.webkitAudioContext)();
    const o = actx.createOscillator(), g = actx.createGain();
    o.frequency.value = f; o.type = 'square'; g.gain.value = 0.05;
    o.connect(g); g.connect(actx.destination); o.start(); o.stop(actx.currentTime + 0.25);
  } catch {}
}
function renderLog(){
  const body = $('logBody');
  const list = events.slice(-200).reverse();
  if (!list.length) { body.innerHTML = '<tr class="empty"><td colspan="4">Sin eventos registrados.</td></tr>'; }
  else body.innerHTML = list.map(e =>
    `<tr class="${e.ack ? '' : 'unack'}" style="--c:var(--${e.sev})"><td>${hms(e.ts)}</td><td>${e.variable}</td>` +
    `<td><span class="badge lv-${e.sev}">${e.lv}</span></td><td>${e.msg}</td></tr>`).join('');
  const n = events.filter(e => !e.ack).length;
  $('unack').hidden = !n; $('unack').textContent = n;
  $('dlEv').disabled = !events.length;
}

/* ---------- color térmico del motor ---------- */
function tempStops(){
  return [[20,[47,127,216]], [(20+cfg.tWarn)/2,[46,190,120]], [cfg.tWarn,[236,196,40]], [cfg.tHigh,[245,128,32]], [cfg.tCrit,[222,40,40]]];
}
function tempColor(T){
  if (!fin(T)) return '#6b7a88';
  const s = tempStops();
  if (T <= s[0][0]) return `rgb(${s[0][1]})`;
  for (let i = 1; i < s.length; i++) if (T <= s[i][0]) {
    const [a, ca] = s[i-1], [b, cb] = s[i], k = (T - a) / Math.max(1e-6, b - a);
    return `rgb(${ca.map((c,j) => Math.round(c + (cb[j] - c) * k))})`;
  }
  return `rgb(${s.at(-1)[1]})`;
}
const tRange = () => [20, cfg.tCrit + 15];
function buildThermo(){
  const [lo, hi] = tRange(), p = t => ((t - lo) / (hi - lo) * 100).toFixed(1) + '%';
  $('tbar').style.background = 'linear-gradient(90deg,' + tempStops().map(([t,c]) => `rgb(${c}) ${p(t)}`).join(',') + `,rgb(150,20,20) 100%)`;
  $('tticks').innerHTML = [lo, cfg.tWarn, cfg.tHigh, cfg.tCrit, hi].map((t,i) =>
    `<span style="left:${p(t)};${i===0?'transform:none':i===4?'transform:translateX(-100%)':''}">${t} °C</span>`).join('');
}
function buildScale(){
  const max = cfg.vibAlt * 1.6, z = document.querySelectorAll('#vscale .z');
  z[0].style.flex = `0 0 ${cfg.vibMed / max * 100}%`;
  z[1].style.flex = `0 0 ${(cfg.vibAlt - cfg.vibMed) / max * 100}%`;
  z[2].style.flex = '1 1 auto';
  const p = v => (v / max * 100) + '%';
  $('vscalelbl').innerHTML = [0, cfg.vibMed, cfg.vibAlt, Math.round(max)].map((v,i) =>
    `<span style="left:${p(v)};${i===0?'transform:none':i===3?'transform:translateX(-100%)':''}">${v} mg</span>`).join('');
  ['X','Y','Z'].forEach(a => $('ax'+a).textContent = a === cfg.axial ? 'AXIAL' : 'RADIAL');
  $('vInom').textContent = cfg.iNom > 0 ? cfg.iNom + ' A' : '—';
  $('vTcrit').textContent = cfg.tCrit + ' °C';
}

/* ---------- actualización de la interfaz ---------- */
function updateUI(r){
  // Corriente
  const unit = r.unit || 'A';
  $('vI').textContent = fmt(r.I, 2); $('uI').textContent = unit === '%' ? '%' : 'A'; $('svI').textContent = `${fmt(r.I,2)} ${unit === '%' ? '%' : 'A'}`;
  $('vIpk').textContent = fin(st.peak) ? st.peak.toFixed(2) + ' ' + (unit === '%' ? '%' : 'A') : '—';
  const cl = r.cLv ? CUR[r.cLv] : ['Sin dato','off'];
  setBadge($('bI'), cl[0], cl[1]); $('kpiI').dataset.lv = cl[1];
  const iScale = unit === '%' ? 100 : Math.max(cfg.iNom * 1.5, 0.1);
  $('mI').style.width = fin(r.I) ? Math.min(100, r.I / iScale * 100) + '%' : '0';

  // Temperatura
  const tl = r.tLv ? TMP[r.tLv] : ['Sin dato','off'];
  $('vT').textContent = fmt(r.T); $('svT').textContent = fmt(r.T) + ' °C'; $('svPlate').textContent = fmt(r.T) + ' °C';
  $('vTmax').textContent = fin(st.tmax) ? st.tmax.toFixed(1) + ' °C' : '—';
  setBadge($('bT'), tl[0], tl[1]); setBadge($('bT2'), 'Térmico: ' + tl[0], tl[1]); $('kpiT').dataset.lv = tl[1];
  const [lo, hi] = tRange(), tp = fin(r.T) ? Math.min(100, Math.max(0, (r.T - lo) / (hi - lo) * 100)) : 0;
  $('mT').style.width = tp + '%'; $('tmark').style.left = tp + '%';

  // Motor
  const m = $('motor');
  m.style.setProperty('--motor', tempColor(r.T));
  m.style.setProperty('--glow', {normal:.14, precaucion:.32, alta:.5, critica:.75}[r.tLv] ?? .08);
  m.classList.toggle('hot-crit', r.tLv === 'critica');
  m.classList.toggle('running', unit === 'A' ? fin(r.I) && r.I >= 0.1 : fin(r.vf) && r.vf > 1);
  m.classList.toggle('vib-media', r.vibLv === 'media');
  m.classList.toggle('vib-alta', r.vibLv === 'alta');
  m.classList.toggle('mis', r.mis);

  // Vibración
  const vl = r.vibLv ? VIB[r.vibLv] : ['Sin dato','off'];
  $('vV').textContent = fmt(r.vf); $('svV').textContent = fmt(r.vf) + ' mg';
  setBadge($('bV'), vl[0], vl[1]); setBadge($('bV2'), 'Severidad ' + vl[0], vl[1]);
  $('kpiV').dataset.lv = r.mis && RANK[vl[1]] < RANK.alarm ? 'alarm' : vl[1];
  $('chipMis').hidden = !r.mis;
  const vmax = cfg.vibAlt * 1.6;
  $('mV').style.width = fin(r.vf) ? Math.min(100, r.vf / vmax * 100) + '%' : '0';
  $('vneedle').style.left = fin(r.vf) ? Math.min(100, r.vf / vmax * 100) + '%' : '0';
  ['X','Y','Z'].forEach((a,i) => {
    const v = st.ax[i];
    $('f'+a).style.width = fin(v) ? Math.min(100, v / vmax * 100) + '%' : '0';
    $('t'+a).textContent = fin(v) ? v.toFixed(1) + ' mg' : '—';
  });
  $('vRatio').textContent = fin(r.ratio) ? `${r.ratio.toFixed(2)}  (umbral ${cfg.ratio})` : '—';
  $('vAlign').textContent = r.mis ? '⚠ Eje desalineado' : fin(r.ratio) ? (r.vibLv === 'baja' ? 'Correcto (vibración baja)' : 'Alineación correcta') : 'Sin datos';
  $('diag').dataset.lv = r.mis ? 'alarm' : fin(r.ratio) ? 'ok' : 'off';
  $('vAng').textContent = r.ang && r.ang.some(fin) ? r.ang.map(a => fin(a) ? a.toFixed(1) + '°' : '—').join(' · ') : '—';

  // Firmware
  const est = fin(r.estado) ? ESTADOS_FW[r.estado] : null;
  $('vF').textContent = est || (r.origen === 'SIMULADO' ? 'Simulación' : 'Sin estado');
  const fl = r.estado === 5 ? 'alarm' : r.estado === 7 || r.estado === 8 ? 'warn' : r.estado === 4 ? 'ok' : 'off';
  setBadge($('bF'), r.origen, r.origen === 'SIMULADO' ? 'off' : 'ok'); $('kpiF').dataset.lv = fl;
  $('vFase').textContent = fin(r.fase) ? FASES[r.fase] ?? '—' : '—';
  $('vLazo').textContent = fin(r.V) ? `${(r.V * 1000 / 119).toFixed(2)} mA · ${r.V.toFixed(3)} V` : '—';

  updateBanner(r);
  $('nRows').textContent = rows.length.toLocaleString('es') + ' muestras';
  $('nLoss').textContent = st.missing ? `Muestras perdidas en transmisión: ${st.missing}` : '';
  ['dlCsv','dlJson'].forEach(id => $(id).disabled = !rows.length);
}

function updateBanner(r){
  const issues = [];
  if (r.tLv && r.tLv !== 'normal') issues.push([TMP[r.tLv][1], `Temperatura ${TMP[r.tLv][0].toLowerCase()} (${fmt(r.T)} °C)`]);
  if (r.mis) issues.push(['alarm', 'Eje desalineado']);
  if (r.vibLv && r.vibLv !== 'baja') issues.push([VIB[r.vibLv][1], `Vibración ${VIB[r.vibLv][0].toLowerCase()} (${fmt(r.vf)} mg)`]);
  if (r.cLv === 'sobre') issues.push(['warn', `Corriente sobre la nominal (${fmt(r.I,2)} A)`]);
  const b = $('banner');
  if (!issues.length) {
    b.className = 'banner ok';
    $('bannerTitle').textContent = 'Operación normal';
    $('bannerText').textContent = 'Corriente, temperatura y vibración dentro de los umbrales configurados.';
    return;
  }
  issues.sort((a,b) => RANK[b[0]] - RANK[a[0]]);
  const sev = issues[0][0];
  b.className = 'banner ' + sev;
  $('bannerTitle').textContent = {warn:'Precaución', alarm:'Alarma', crit:'ALARMA CRÍTICA'}[sev] + ' — ' + issues[0][1];
  $('bannerText').textContent = issues.length > 1 ? 'También: ' + issues.slice(1).map(i => i[1]).join(' · ') : 'Revise el registro de eventos.';
}

/* ---------- gráficas (canvas, sin librerías) ---------- */
function lowerBound(arr, t){ let a = 0, b = arr.length; while (a < b) { const m = (a + b) >> 1; arr[m].ts < t ? a = m + 1 : b = m; } return a; }
function windowRange(){
  const t1 = st.paused ? st.pauseAt : Date.now();
  const t0 = st.win === 'all' ? (rows[0]?.ts ?? t1 - 60000) : t1 - st.win;
  return [t0, Math.max(t1, t0 + 1000)];
}
function niceTick(v){ return Math.abs(v) >= 100 ? v.toFixed(0) : Number(v.toFixed(1)).toString(); }

function drawChart(cv, o){
  const dpr = devicePixelRatio || 1, w = cv.clientWidth, h = cv.clientHeight;
  if (!w || !h) return;
  if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); }
  const g = cv.getContext('2d'); g.setTransform(dpr,0,0,dpr,0,0); g.clearRect(0,0,w,h);
  const {t0, t1, lo, hi, data} = o;
  // tipografía y márgenes escalados al ancho de la gráfica
  const big = w >= 560, fs = big ? 14 : 12, ft = big ? 15 : 13, NY = 5;
  g.font = `${fs}px system-ui, sans-serif`;
  let wl = 0;
  for (let i = 0; i <= NY; i++) wl = Math.max(wl, g.measureText(niceTick(lo + (hi - lo) * i / NY) + (o.unit ? ' ' + o.unit : '')).width);
  const L = Math.max(50, Math.ceil(wl) + 14), R = w - 14, T = 12, B = h - (ft + 20);
  const X = t => L + (t - t0) * (R - L) / (t1 - t0);
  const Y = v => B - (Math.min(hi, Math.max(lo, v)) - lo) * (B - T) / (hi - lo);
  // zonas de severidad
  for (const z of o.zones || []) {
    const a = Math.max(z.from, lo), b = Math.min(z.to, hi); if (b <= a) continue;
    g.fillStyle = z.color; g.globalAlpha = 0.1; g.fillRect(L, Y(b), R - L, Y(a) - Y(b));
  }
  g.globalAlpha = 1;
  // rejilla
  g.font = `${fs}px system-ui, sans-serif`; g.lineWidth = 1; g.textAlign = 'right';
  for (let i = 0; i <= NY; i++) {
    const v = lo + (hi - lo) * i / NY, y = Math.round(Y(v)) + .5;
    g.strokeStyle = C.grid; g.beginPath(); g.moveTo(L, y); g.lineTo(R, y); g.stroke();
    g.fillStyle = C.muted; g.fillText(niceTick(v) + (o.unit ? ' ' + o.unit : ''), L - 8, y + fs * 0.35);
  }
  // líneas umbral
  for (const ln of o.lines || []) {
    if (ln.v < lo || ln.v > hi) continue;
    const y = Math.round(Y(ln.v)) + .5;
    g.strokeStyle = ln.color; g.setLineDash([5,4]); g.beginPath(); g.moveTo(L, y); g.lineTo(R, y); g.stroke(); g.setLineDash([]);
    g.fillStyle = ln.color; g.textAlign = 'left'; g.font = `600 ${fs}px system-ui, sans-serif`; g.fillText(ln.label, L + 6, y - 6);
    g.font = `${fs}px system-ui, sans-serif`; g.textAlign = 'right';
  }
  // eje de tiempo: hora grande con marcas verticales
  const NT = w >= 900 ? 6 : big ? 4 : 2;
  g.font = `600 ${ft}px system-ui, sans-serif`;
  for (let i = 0; i <= NT; i++) {
    const t = t0 + (t1 - t0) * i / NT, x = Math.round(X(t)) + .5;
    if (i > 0 && i < NT) { g.strokeStyle = C.grid; g.beginPath(); g.moveTo(x, T); g.lineTo(x, B); g.stroke(); }
    g.strokeStyle = C.line; g.beginPath(); g.moveTo(x, B); g.lineTo(x, B + 5); g.stroke();
    g.textAlign = i === 0 ? 'left' : i === NT ? 'right' : 'center';
    g.fillStyle = i === NT ? C.ink : C.muted;
    g.fillText(i === NT && st.paused ? hms(t) + ' (pausa)' : hms(t), x, h - 8);
  }
  g.font = `${fs}px system-ui, sans-serif`;
  // series
  const step = Math.max(1, Math.floor(data.length / ((R - L) * 1.5)));
  g.save(); g.beginPath(); g.rect(L, T - 2, R - L, B - T + 4); g.clip();
  for (const s of o.series) {
    g.strokeStyle = s.color; g.lineWidth = big ? 2.6 : 2.1; g.lineJoin = 'round'; g.lineCap = 'round'; g.beginPath();
    let pen = false, prev = 0;
    for (let i = 0; i < data.length; i += step) {
      const r = data[i], v = s.get(r);
      if (!fin(v)) { pen = false; continue; }
      if (pen && r.ts - prev > 1500 * step) pen = false;          // hueco de transmisión
      const x = X(r.ts), y = Y(v);
      pen ? g.lineTo(x, y) : g.moveTo(x, y); pen = true; prev = r.ts;
    }
    g.stroke();
  }
  g.restore();
  // marco
  g.strokeStyle = C.line; g.beginPath(); g.moveTo(L + .5, T); g.lineTo(L + .5, B + .5); g.lineTo(R, B + .5); g.stroke();
  // cursor e información
  const hx = hover[cv.id];
  if (hx != null && hx >= L && hx <= R && data.length) {
    const t = t0 + (hx - L) * (t1 - t0) / (R - L);
    let k = lowerBound(data, t); if (k >= data.length) k = data.length - 1;
    if (k > 0 && Math.abs(data[k-1].ts - t) < Math.abs(data[k].ts - t)) k--;
    const r = data[k], x = X(r.ts);
    g.strokeStyle = C.muted; g.setLineDash([3,3]); g.beginPath(); g.moveTo(x, T); g.lineTo(x, B); g.stroke(); g.setLineDash([]);
    const lines = [stamp(r.ts).slice(11, 23)];
    for (const s of o.series) {
      const v = s.get(r);
      lines.push(`${s.label}: ${fin(v) ? v.toFixed(s.dec ?? 2) : '—'} ${o.unit || ''}`);
      if (fin(v)) { g.fillStyle = s.color; g.beginPath(); g.arc(x, Y(v), 4.5, 0, 7); g.fill(); }
    }
    g.font = `600 ${fs}px system-ui, sans-serif`;
    const lh = fs + 6, bw = Math.max(...lines.map(l => g.measureText(l).width)) + 20, bh = lines.length * lh + 10;
    let bx = x + 10; if (bx + bw > R) bx = x - bw - 10;
    g.fillStyle = C.panel; g.strokeStyle = C.line; g.globalAlpha = .95;
    g.beginPath(); g.roundRect ? g.roundRect(bx, T + 4, bw, bh, 6) : g.rect(bx, T + 4, bw, bh); g.fill(); g.stroke(); g.globalAlpha = 1;
    g.textAlign = 'left';
    lines.forEach((l,i) => { g.fillStyle = i === 0 ? C.muted : (o.series[i-1].color); g.fillText(l, bx + 10, T + 4 + lh + i * lh); });
  }
}

/* ============================================================
   Corriente: lotes del JXK-10 cada 2 ms y onda sinusoidal equivalente
   El JXK-10 entrega el valor RMS (lazo 4-20 mA en DC). La onda que se dibuja
   es i(t) = √2 · I_RMS(t) · sen(2π·f·t): amplitud medida, forma sinusoidal ideal.
   ============================================================ */
const W = {pts:[], cap:null, caps:[], quiet:0, lastT:-1, llegada:0, vista:'arranque', ven:1000, f:60};
function onLote(t0, dt, vals){
  if (W.lastT >= 0 && t0 < W.lastT - 1000) { W.pts.length = 0; W.quiet = 0; W.lastT = -1; W.cap = null; }   // reinicio del ESP32 / cambio de fuente
  vals.forEach((v, k) => {
    const t = t0 + k * dt; if (t <= W.lastT) return;
    W.pts.push({t, v}); W.lastT = t;
    // disparo automático: motor quieto ≥ 200 ms y la corriente supera 0,6 A
    if (fin(v) && v < 0.3) W.quiet += dt; else if (fin(v) && v >= 0.6 && W.quiet >= 200 && !W.cap) iniciarCaptura(t, true);
    if (fin(v) && v >= 0.3) W.quiet = 0;
    if (W.cap) { if (t <= W.cap.fin) W.cap.pts.push({t, v}); else cerrarCaptura(); }
  });
  while (W.pts.length && W.pts[0].t < W.lastT - 60000) W.pts.shift();
  W.llegada = performance.now(); dirty = true;
}
function iniciarCaptura(t, auto){
  const pre = 10;
  W.cap = {trig:t, ini:t - pre, fin:t + 3000, auto, ts:Date.now(), pts:W.pts.filter(p => p.t >= t - pre && p.t <= t)};
  setConn(auto ? '● Arranque detectado · capturando corriente' : '● Capturando corriente', 'live');
}
function cerrarCaptura(){
  const c = W.cap; W.cap = null;
  if (st.mode === 'demo') setConn('● Simulación', 'demo'); else status();
  const v = c.pts.map(p => p.v).filter(fin);
  if (!v.length) return;
  const reg = c.pts.filter(p => p.t >= c.fin - 500).map(p => p.v).filter(fin);
  c.imax = Math.max(...v); c.ireg = reg.length ? reg.reduce((a, b) => a + b, 0) / reg.length : NaN;
  c.tmax = c.pts.find(p => p.v === c.imax).t - c.trig;
  W.caps.unshift(c); if (W.caps.length > 6) W.caps.pop();
  pintarCapturas(0);
  logEvent('Corriente', c.auto ? 'Arranque' : 'Captura', 'ok',
    `${c.auto ? 'Arranque registrado' : 'Captura manual'}: I RMS máx ${c.imax.toFixed(2)} A a ${c.tmax.toFixed(0)} ms` + (fin(c.ireg) ? ` · régimen ${c.ireg.toFixed(2)} A` : ''));
}
function pintarCapturas(sel){
  $('ondaCap').innerHTML = W.caps.length ? W.caps.map((c, i) => `<option value="${i}">${hms(c.ts)} · ${c.auto ? 'arranque' : 'manual'} · ${c.imax.toFixed(2)} A</option>`).join('') : '<option>Sin capturas</option>';
  $('ondaCap').value = String(sel); $('dlOnda').disabled = !W.caps.length; mostrarStats(); dirty = true;
}
function mostrarStats(){
  const c = W.caps[Number($('ondaCap').value) || 0];
  $('wPico').textContent = c ? (Math.SQRT2 * c.imax).toFixed(2) + ' A' : '—';
  $('wMax').textContent = c ? `${c.imax.toFixed(2)} A · ${c.tmax.toFixed(0)} ms` : '—';
  $('wReg').textContent = c && fin(c.ireg) ? c.ireg.toFixed(2) + ' A' : '—';
  $('wRel').textContent = c && fin(c.ireg) && c.ireg > 0.05 ? (c.imax / c.ireg).toFixed(1) + ' ×' : '—';
}
function irmsEn(pts, t){                       // interpolación lineal del RMS medido
  let a = 0, b = pts.length - 1;
  if (b < 0 || t < pts[0].t || t > pts[b].t) return NaN;
  while (b - a > 1) { const m = (a + b) >> 1; pts[m].t <= t ? a = m : b = m; }
  const p = pts[a], q = pts[b];
  if (!fin(p.v) || !fin(q.v)) return NaN;
  return q.t === p.t ? p.v : p.v + (q.v - p.v) * (t - p.t) / (q.t - p.t);
}
function pasoBonito(r){ const e = Math.pow(10, Math.floor(Math.log10(r))), m = r / e; return (m < 1.5 ? 1 : m < 3.5 ? 2 : m < 7.5 ? 5 : 10) * e; }
function dibujarOnda(cv){
  const dpr = devicePixelRatio || 1, w = cv.clientWidth, h = cv.clientHeight;
  if (!w || !h) return;
  if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); }
  const g = cv.getContext('2d'); g.setTransform(dpr, 0, 0, dpr, 0, 0); g.clearRect(0, 0, w, h);
  const big = w >= 560, fs = big ? 14 : 12, ft = big ? 15 : 13;
  // datos de la vista
  let pts, a, b, ref, rel;
  if (W.vista === 'vivo') {
    pts = W.pts; if (!pts.length) return vacio(g, w, h, fs, 'Esperando lotes de corriente del ESP32…');
    // desplazamiento suave: se dibuja con 120 ms de retardo respecto al último lote
    b = Math.min(W.lastT, W.lastT - 120 + (performance.now() - W.llegada)); a = b - W.ven; ref = 0; rel = b;
  } else {
    const c = W.cap && W.vista === 'arranque' && !W.caps.length ? W.cap : W.caps[Number($('ondaCap').value) || 0] || W.cap;
    if (!c) return vacio(g, w, h, fs, 'Esperando un arranque del motor… (o pulse «Capturar ahora»)');
    pts = c.pts; a = c.trig - 10; b = c.trig + W.ven; ref = c.trig; rel = c.trig;
  }
  const N = Math.min(24000, Math.max(800, Math.ceil((b - a) / (1000 / W.f / 24))));   // ≥ 24 puntos por ciclo
  const ts = new Float64Array(N + 1), ys = new Float64Array(N + 1), env = new Float64Array(N + 1);
  let amp = 0;
  for (let k = 0; k <= N; k++) {
    const t = a + (b - a) * k / N, r = irmsEn(pts, t);
    ts[k] = t; env[k] = fin(r) ? Math.SQRT2 * r : NaN;
    ys[k] = fin(r) ? env[k] * Math.sin(2 * Math.PI * W.f * (t - ref) / 1000) : NaN;
    if (fin(env[k])) amp = Math.max(amp, env[k]);
  }
  const hi = Math.max(1, Math.ceil(amp * 1.12 / pasoBonito(amp * 1.12 / 3 || 1)) * pasoBonito(amp * 1.12 / 3 || 1)), lo = -hi;
  g.font = `${fs}px system-ui, sans-serif`;
  const L = Math.max(54, g.measureText(`-${hi} A`).width + 16), R = w - 16, T = 14, B = h - (ft + 22);
  const X = t => L + (t - a) * (R - L) / (b - a), Y = v => B - (v - lo) * (B - T) / (hi - lo);
  // área de trazado
  g.fillStyle = C.plot; g.fillRect(L, T, R - L, B - T);
  const pasoY = pasoBonito((hi - lo) / 6);
  g.lineWidth = 1; g.textAlign = 'right';
  for (let v = Math.ceil(lo / pasoY) * pasoY; v <= hi + 1e-9; v += pasoY) {
    const y = Math.round(Y(v)) + .5;
    g.strokeStyle = Math.abs(v) < 1e-9 ? C.muted : C.grid; g.lineWidth = Math.abs(v) < 1e-9 ? 1.5 : 1;
    g.beginPath(); g.moveTo(L, y); g.lineTo(R, y); g.stroke();
    g.fillStyle = C.muted; g.fillText(niceTick(v), L - 8, y + fs * .35);
  }
  g.fillStyle = C.ink; g.font = `700 ${ft}px system-ui, sans-serif`; g.textAlign = 'left'; g.fillText('A', 8, T + ft);
  const pasoX = pasoBonito((b - a) / (big ? 12 : 6));
  g.font = `600 ${ft}px system-ui, sans-serif`;
  for (let t = Math.ceil((a - rel) / pasoX) * pasoX; t <= b - rel + 1e-9; t += pasoX) {
    const x = Math.round(X(t + rel)) + .5;
    g.strokeStyle = C.grid; g.lineWidth = 1; g.beginPath(); g.moveTo(x, T); g.lineTo(x, B); g.stroke();
    g.fillStyle = C.muted; g.textAlign = 'center'; g.fillText(String(Math.round(t)), x, h - 10);
  }
  g.fillStyle = C.ink; g.textAlign = 'right'; g.fillText('ms', R, T - 2 > ft ? T - 2 : h - 10 - ft - 2);
  g.strokeStyle = C.line; g.lineWidth = 1; g.strokeRect(L + .5, T + .5, R - L - 1, B - T - 1);
  // envolvente ±√2·I_RMS medida
  g.save(); g.beginPath(); g.rect(L, T, R - L, B - T); g.clip();
  g.setLineDash([6, 5]); g.strokeStyle = C.envol; g.lineWidth = 1.4;
  for (const s of [1, -1]) { g.beginPath(); let pen = false;
    for (let k = 0; k <= N; k++) { if (!fin(env[k])) { pen = false; continue; } const x = X(ts[k]), y = Y(s * env[k]); pen ? g.lineTo(x, y) : g.moveTo(x, y); pen = true; } g.stroke(); }
  g.setLineDash([]);
  // onda
  g.strokeStyle = C.onda; g.lineWidth = big ? 2.8 : 2.2; g.lineJoin = 'round'; g.beginPath(); let pen = false;
  for (let k = 0; k <= N; k++) { if (!fin(ys[k])) { pen = false; continue; } const x = X(ts[k]), y = Y(ys[k]); pen ? g.lineTo(x, y) : g.moveTo(x, y); pen = true; }
  g.stroke(); g.restore();
  // cursor
  const hx = hover[cv.id];
  if (hx != null && hx >= L && hx <= R) {
    const t = a + (hx - L) * (b - a) / (R - L), r = irmsEn(pts, t), i = fin(r) ? Math.SQRT2 * r * Math.sin(2 * Math.PI * W.f * (t - ref) / 1000) : NaN;
    g.strokeStyle = C.muted; g.setLineDash([3, 3]); g.beginPath(); g.moveTo(hx, T); g.lineTo(hx, B); g.stroke(); g.setLineDash([]);
    if (fin(i)) { g.fillStyle = C.onda; g.beginPath(); g.arc(hx, Y(i), 4.5, 0, 7); g.fill(); }
    const txt = [`${(t - rel).toFixed(1)} ms`, `i equivalente: ${fmt(i, 2)} A`, `I RMS medida: ${fmt(r, 2)} A`];
    g.font = `600 ${fs}px system-ui, sans-serif`;
    const lh = fs + 6, bw = Math.max(...txt.map(l => g.measureText(l).width)) + 20, bh = txt.length * lh + 10;
    let bx = hx + 12; if (bx + bw > R) bx = hx - bw - 12;
    g.globalAlpha = .95; g.fillStyle = C.panel; g.strokeStyle = C.line; g.beginPath(); g.roundRect ? g.roundRect(bx, T + 6, bw, bh, 6) : g.rect(bx, T + 6, bw, bh); g.fill(); g.stroke(); g.globalAlpha = 1;
    g.textAlign = 'left'; txt.forEach((l, k) => { g.fillStyle = k === 0 ? C.muted : k === 1 ? C.onda : C.ink; g.fillText(l, bx + 10, T + 6 + lh + k * lh); });
  }
}
function vacio(g, w, h, fs, txt){ g.fillStyle = C.muted; g.font = `${fs + 2}px system-ui, sans-serif`; g.textAlign = 'center'; g.fillText(txt, w / 2, h / 2); }

function drawAll(){
  const [t0, t1] = windowRange();
  const data = rows.slice(lowerBound(rows, t0 - 1000), lowerBound(rows, t1 + 1));
  const ext = (get) => { let mn = Infinity, mx = -Infinity; for (const r of data) { const v = get(r); if (fin(v)) { if (v < mn) mn = v; if (v > mx) mx = v; } } return [mn, mx]; };

  const [, vMax] = ext(r => Math.max(r.x ?? -1, r.y ?? -1, r.z ?? -1, r.vf ?? -1));
  drawChart($('cV'), {t0, t1, data, lo:0, hi:Math.max(cfg.vibAlt * 1.4, fin(vMax) ? vMax * 1.1 : 0), unit:'mg',
    zones:[{from:0,to:cfg.vibMed,color:C.ok},{from:cfg.vibMed,to:cfg.vibAlt,color:C.warn},{from:cfg.vibAlt,to:1e9,color:C.alarm}],
    series:[{label:'X',color:C.cx,get:r=>r.x,dec:1},{label:'Y',color:C.cy,get:r=>r.y,dec:1},{label:'Z',color:C.cz,get:r=>r.z,dec:1}]});

  const [tMin, tMax] = ext(r => r.T);
  const tlo = Math.min(20, fin(tMin) ? Math.floor(tMin / 5) * 5 : 20);
  const thi = Math.max(cfg.tCrit + 10, fin(tMax) ? Math.ceil(tMax / 5) * 5 + 5 : 0);
  drawChart($('cT'), {t0, t1, data, lo:tlo, hi:thi, unit:'°C',
    zones:[{from:-1e9,to:cfg.tWarn,color:C.ok},{from:cfg.tWarn,to:cfg.tHigh,color:C.warn},{from:cfg.tHigh,to:cfg.tCrit,color:C.alarm},{from:cfg.tCrit,to:1e9,color:C.crit}],
    lines:[{v:cfg.tCrit,color:C.crit,label:`Crítica ${cfg.tCrit} °C`}],
    series:[{label:'T',color:C.ct,get:r=>r.T,dec:1}]});
  $('tStats').textContent = fin(tMin) ? `mín ${tMin.toFixed(1)} · máx ${tMax.toFixed(1)} °C` : '';

  const [iMin, iMax] = ext(r => r.I);
  const pct = data.some(r => r.unit === '%');
  if (W.vista !== 'rms') dibujarOnda($('cW'));
  else drawChart($('cI'), {t0, t1, data, lo:0, hi: pct ? 100 : Math.max(cfg.iNom * 1.5, fin(iMax) ? iMax * 1.15 : 1), unit: pct ? '%' : 'A',
    lines: pct || !(cfg.iNom > 0) ? [] : [{v:cfg.iNom,color:C.warn,label:`Nominal ${cfg.iNom} A`}],
    series:[{label:'I',color:C.ci,get:r=>r.I,dec:2}]});
  $('iStats').textContent = fin(iMin) ? `mín ${iMin.toFixed(2)} · máx ${iMax.toFixed(2)} ${pct ? '%' : 'A'}` : '';
}
let lastDraw = 0;
function frame(ts){
  // tiempo real: ~30 cuadros por segundo con desplazamiento continuo del eje de tiempo
  if ((dirty || (!st.paused && (rows.length || W.pts.length))) && ts - lastDraw > 30) { dirty = false; lastDraw = ts; drawAll(); }
  requestAnimationFrame(frame);
}
for (const id of ['cV','cT','cI','cW']) {
  const cv = $(id);
  cv.addEventListener('pointermove', e => { hover[id] = e.offsetX; dirty = true; });
  cv.addEventListener('pointerleave', () => { delete hover[id]; dirty = true; });
}

/* ---------- MQTT mínimo sobre WebSocket (igual al firmware) ---------- */
let ws = null, wsWanted = false, subscribed = false;
const enc = new TextEncoder(), dec = new TextDecoder();
const field = s => { const b = enc.encode(s); return [b.length >> 8, b.length & 255, ...b]; };
function packet(type, body){ let n = body.length; const head = [type];
  do { let k = n % 128; n = Math.floor(n / 128); head.push(k | (n ? 128 : 0)); } while (n);
  return Uint8Array.from([...head, ...body]); }
function setConn(txt, cls){ const p = $('conn'); p.textContent = txt; p.className = 'pill ' + (cls || ''); }
function mqttConnect(){
  wsWanted = true; subscribed = false; setConn('Conectando al broker MQTT…');
  const sock = new WebSocket(BROKER, 'mqtt'); ws = sock; sock.binaryType = 'arraybuffer';
  let pending = new Uint8Array(0);
  sock.onopen = () => sock.send(packet(16, [...field('MQTT'), 4, 2, 0, 30, ...field('mon-' + Math.random().toString(16).slice(2, 14))]));
  sock.onmessage = e => {
    const b = new Uint8Array(e.data), a = new Uint8Array(pending.length + b.length); a.set(pending); a.set(b, pending.length);
    let p = 0;
    while (p < a.length) {
      const start = p, hd = a[p++]; let size = 0, mul = 1, v;
      do { if (p >= a.length) { pending = a.slice(start); return; } v = a[p++]; size += (v & 127) * mul; mul *= 128; } while (v & 128);
      if (p + size > a.length) { pending = a.slice(start); return; }
      const d = a.subarray(p, p + size); p += size;
      const type = hd >> 4;
      if (type === 2) { if (d[1] !== 0) { sock.close(); return; } sock.send(packet(130, [0, 1, ...field(TOPIC), 0, ...field(TOPIC_I), 0])); }
      if (type === 9) { if (d[2] === 128) { sock.close(); return; } subscribed = true; setConn('Esperando al ESP32…', 'warn'); }
      if (type === 3 && !(hd & 1)) {
        const n = (d[0] << 8) + d[1], tp = dec.decode(d.subarray(2, 2 + n)), txt = dec.decode(d.subarray(2 + n));
        if (tp === TOPIC) parseMsg(txt); else if (tp === TOPIC_I) parseLote(txt);
      }
    }
    pending = new Uint8Array(0);
  };
  sock.onerror = () => sock.close();
  sock.onclose = () => { if (ws !== sock) return; subscribed = false; ws = null;
    if (wsWanted) { setConn('Broker desconectado · reintentando…', 'warn'); setTimeout(() => wsWanted && !ws && mqttConnect(), 3000); } };
}
function mqttStop(){ wsWanted = false; const s = ws; ws = null; if (s) try { s.close(); } catch {} }
setInterval(() => { if (ws?.readyState === 1) ws.send(packet(192, [])); }, 12000);

function parseMsg(s){
  const f = s.trim().split(',');
  if (![6, 11, 17].includes(f.length) || !/^[a-f0-9]{8}$/.test(f[0])) return;
  const n = v => (v === '' || /nan|inf/i.test(v)) ? null : (fin(Number(v)) ? Number(v) : null);
  const seq = n(f[1]), t = n(f[2]);
  if (!Number.isSafeInteger(seq) || !fin(t) || seq < 0) return;
  const multi = f.length >= 11, full = f.length === 17;
  if (multi && !['A','P'].includes(f[10])) return;
  if (f[0] !== st.session) { st.session = f[0]; st.lastSeq = -1; }
  if (seq <= st.lastSeq) return;
  if (st.lastSeq >= 0) st.missing += seq - st.lastSeq - 1;
  st.lastSeq = seq;
  if (st.mode !== 'live') return;
  ingest({origen:'ESP32', sess:f[0], seq, t, x:n(f[3]), y:n(f[4]), z:n(f[5]),
    I: multi ? n(f[6]) : null, Ipk: multi ? n(f[8]) : null, unit: multi ? (f[10] === 'A' ? 'A' : '%') : '',
    T: multi ? n(f[9]) : null, V: full ? n(f[11]) : null,
    ang: full ? [n(f[12]), n(f[13]), n(f[14])] : [null, null, null],
    fase: full ? n(f[15]) : null, estado: full ? n(f[16]) : null});
}

// lote de corriente: sesion,seq,t0_ms,dt_ms,i1,i2,...
function parseLote(s){
  if (st.mode !== 'live') return;
  const f = s.trim().split(',');
  if (f.length < 5 || !/^[a-f0-9]{8}$/.test(f[0])) return;
  const t0 = Number(f[2]), dt = Number(f[3]);
  if (!fin(t0) || !(dt > 0)) return;
  onLote(t0, dt, f.slice(4).map(v => /nan|inf/i.test(v) || v === '' ? NaN : Number(v)));
}

/* ---------- simulador (modo Demo) ---------- */
const SCEN = {
  normal:        {I:1.6, T:48,  v:[3.0, 5.0, 2.4],  est:4, txt:'Operación normal'},
  desalineacion: {I:1.8, T:55,  v:[15,  9.0, 5.0],  est:5, txt:'Eje desalineado'},
  vibracion:     {I:1.9, T:58,  v:[7.5, 26,  12],   est:3, txt:'Vibración alta'},
  calentamiento: {I:2.3, T:108, v:[4.0, 6.0, 3.0],  est:4, txt:'Sobrecalentamiento'},
  parada:        {I:0,   T:26,  v:[0,   0,   0],    est:1, txt:'Motor detenido'}
};
const AUTO = ['normal','desalineacion','vibracion','calentamiento','parada'];
const sim = {timer:null, t:0, seq:0, T:30, I:0, v:[0,0,0], peak:0, scen:'auto'};
const gauss = () => { let u = 0, v = 0; while (!u) u = Math.random(); while (!v) v = Math.random(); return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); };
function simStep(){
  sim.t += 100; sim.seq++;
  const key = sim.scen === 'auto' ? AUTO[Math.floor(sim.t / 20000) % AUTO.length] : sim.scen;
  const P = SCEN[key];
  $('scenNow').textContent = sim.scen === 'auto' ? `Fase actual: ${P.txt}` : '';
  sim.T += (P.T - sim.T) * (key === 'calentamiento' ? 0.012 : 0.006) + gauss() * 0.04;
  // corriente cada 2 ms con arranque directo: pico ≈ 6 × régimen que decae (τ ≈ 160 ms)
  const parado = P.I === 0 || sim.t < (sim.forzarParo || 0), lote = [];
  for (let k = 0; k < 50; k++) {
    const tt = sim.t - 100 + k * 2;
    if (parado) { sim.on = false; sim.Ic = (sim.Ic || 0) * Math.exp(-2 / 40); }
    else {
      if (!sim.on) { sim.on = true; sim.tOn = tt; }
      const e = tt - sim.tOn;
      sim.Ic = P.I * (1 + 0.02 * Math.sin(tt / 700)) + 5 * P.I * Math.exp(-e / 160) * (1 - Math.exp(-e / 10)) + gauss() * 0.02;
    }
    lote.push(Math.max(0, sim.Ic));
  }
  onLote(sim.t - 100, 2, lote);
  const I = lote.reduce((a, b) => a + b, 0) / lote.length;
  sim.v = sim.v.map((v,i) => v + (P.v[i] - v) * 0.1);
  const xyz = sim.v.map(v => v > 0.05 ? Math.max(0, v * (1 + 0.12 * gauss())) : 0);
  sim.peak = Math.max(sim.peak, I);
  ingest({origen:'SIMULADO', sess:'demo', seq:sim.seq, t:sim.t, x:xyz[0], y:xyz[1], z:xyz[2],
    I, Ipk:sim.peak, unit:'A', T:sim.T, V:(4 + 16 * I / 25) * 119 / 1000,
    ang:[gauss()*0.2, gauss()*0.2, gauss()*0.2], fase: I > 0.1 ? 1 : 0, estado: I > 0.1 ? P.est : 1});
}

/* ---------- modo ---------- */
function resetFilters(){
  st.vibEma = null; st.ax = [null,null,null]; st.misCnt = 0; st.mis = false;
  st.lv = {vib:null, temp:null, cur:null, mis:false}; st.peak = null; st.tmax = null;
}
function setMode(mode){
  st.mode = mode; resetFilters();
  document.querySelectorAll('[data-mode]').forEach(b => b.classList.toggle('on', b.dataset.mode === mode));
  $('demoBar').hidden = mode !== 'demo';
  clearInterval(sim.timer); sim.timer = null;
  W.pts.length = 0; W.lastT = -1; W.quiet = 0; W.cap = null;
  if (mode === 'demo') { mqttStop(); setConn('● Simulación', 'demo'); sim.forzarParo = sim.t + 800; sim.timer = setInterval(simStep, 100); }
  else { st.lastAt = 0; mqttConnect(); }
}
document.querySelectorAll('[data-mode]').forEach(b => b.onclick = () => st.mode !== b.dataset.mode && setMode(b.dataset.mode));
$('scenario').onchange = e => { sim.scen = e.target.value; if (sim.scen !== 'auto') { sim.t = 0; sim.forzarParo = 800; } };

$('simArranque').onclick = () => { sim.forzarParo = sim.t + 1200; };   // parada de 1,2 s y nuevo arranque

/* ---------- controles de la onda de corriente ---------- */
document.querySelectorAll('[data-iv]').forEach(b => b.onclick = () => {
  W.vista = b.dataset.iv;
  document.querySelectorAll('[data-iv]').forEach(x => x.classList.toggle('on', x === b));
  $('cW').hidden = W.vista === 'rms'; $('cI').hidden = W.vista !== 'rms';
  $('ondaOpc').hidden = W.vista === 'rms'; $('ondaCapBox').hidden = W.vista !== 'arranque';
  const v = $('ondaVen'); if (W.vista === 'vivo' && W.ven > 500) { v.value = '200'; W.ven = 200; }
  if (W.vista === 'arranque' && W.ven < 200) { v.value = '1000'; W.ven = 1000; }
  dirty = true;
});
$('ondaVen').onchange = e => { W.ven = Number(e.target.value); dirty = true; };
$('ondaF').onchange = e => { W.f = Number(e.target.value); dirty = true; };
$('ondaCap').onchange = () => { mostrarStats(); dirty = true; };
$('capturar').onclick = () => { if (W.lastT >= 0 && !W.cap) iniciarCaptura(W.lastT, false); };
$('dlOnda').onclick = () => {
  const c = W.caps[Number($('ondaCap').value) || 0]; if (!c) return;
  const out = ['﻿t_ms;I_RMS_medida_A;i_equivalente_A;nota'];
  c.pts.forEach((p, k) => out.push([ (p.t - c.trig).toFixed(0), fin(p.v) ? p.v.toFixed(3) : '',
    fin(p.v) ? (Math.SQRT2 * p.v * Math.sin(2 * Math.PI * W.f * (p.t - c.trig) / 1000)).toFixed(3) : '',
    k === 0 ? `"i_equivalente = raiz(2)*I_RMS*sen(2*pi*${W.f}*t): onda calculada, el JXK-10 mide valor RMS"` : ''].join(';')));
  download(`motor_arranque_${fileStamp()}.csv`, out.join('\r\n') + '\r\n', 'text/csv;charset=utf-8');
};

/* ---------- controles ---------- */
document.querySelectorAll('[data-win]').forEach(b => b.onclick = () => {
  st.win = b.dataset.win === 'all' ? 'all' : Number(b.dataset.win);
  document.querySelectorAll('[data-win]').forEach(x => x.classList.toggle('on', x === b)); dirty = true;
});
$('pauseBtn').onclick = () => {
  st.paused = !st.paused; st.pauseAt = Date.now();
  $('pauseBtn').textContent = st.paused ? '▶ Reanudar gráficas' : '❚❚ Pausar gráficas';
  $('pauseBtn').classList.toggle('active', st.paused); dirty = true;
};
$('ackBtn').onclick = () => { events.forEach(e => e.ack = true); renderLog(); };
$('printBtn').onclick = () => print();
$('clearBtn').onclick = () => {
  if (!rows.length && !events.length) return;
  if (!confirm('¿Borrar todas las muestras y eventos de esta sesión? Descargue los datos antes si los necesita.')) return;
  rows.length = 0; events.length = 0; st.missing = 0; resetFilters(); renderLog();
  W.pts.length = 0; W.caps.length = 0; W.cap = null; W.lastT = -1; pintarCapturas(0);
  $('nRows').textContent = '0 muestras'; $('nLoss').textContent = ''; ['dlCsv','dlJson'].forEach(id => $(id).disabled = true); dirty = true;
};
function applyTheme(t){ document.documentElement.dataset.theme = t; store.set('mc-theme', t); readColors(); dirty = true; }
$('themeBtn').onclick = () => applyTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark');

/* configuración */
const form = $('cfg');
function fillCfg(){ form.querySelectorAll('[data-k]').forEach(i => i.type === 'checkbox' ? i.checked = !!cfg[i.dataset.k] : i.value = cfg[i.dataset.k]); }
form.addEventListener('change', () => {
  const next = {...cfg};
  form.querySelectorAll('[data-k]').forEach(i => {
    const k = i.dataset.k;
    next[k] = i.type === 'checkbox' ? i.checked : i.tagName === 'SELECT' ? i.value : Number(i.value);
  });
  const msg = $('cfgMsg');
  const nums = ['vibMed','vibAlt','ratio','tWarn','tHigh','tCrit','iNom'];
  let err = nums.some(k => !fin(next[k])) ? 'Complete todos los campos numéricos.' :
    !(next.vibMed > 0 && next.vibMed < next.vibAlt) ? 'La vibración media debe ser mayor que 0 y menor que la alta.' :
    !(next.tWarn < next.tHigh && next.tHigh < next.tCrit) ? 'Las temperaturas deben cumplir: precaución < alta < crítica.' :
    !(next.ratio > 0) ? 'La relación axial/radial debe ser mayor que 0.' : '';
  if (err) { msg.textContent = err; msg.className = 'note err'; return; }
  cfg = next; store.set('mc-cfg', JSON.stringify(cfg));
  msg.textContent = 'Umbrales aplicados y guardados en este navegador.'; msg.className = 'note';
  st.lv = {vib:null, temp:null, cur:null, mis:st.mis};
  buildThermo(); buildScale(); if (rows.length) updateUI(rows.at(-1)); dirty = true;
});
$('cfgReset').onclick = () => { cfg = {...DEF}; store.set('mc-cfg', JSON.stringify(cfg)); fillCfg(); buildThermo(); buildScale(); dirty = true;
  $('cfgMsg').textContent = 'Umbrales restablecidos a los valores de referencia.'; $('cfgMsg').className = 'note'; };

/* ---------- descargas ---------- */
function download(name, content, type){
  const url = URL.createObjectURL(new Blob([content], {type}));
  const a = document.createElement('a'); a.href = url; a.download = name; document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 2000);
}
const c = (v,d=3) => fin(v) ? String(+v.toFixed(d)) : '';
$('dlCsv').onclick = () => {
  const head = ['fecha_hora_local','timestamp_ms','origen','sesion','secuencia','t_dispositivo_ms',
    'vib_X_mg','vib_Y_mg','vib_Z_mg','vib_global_mg','vib_global_filtrada_mg','nivel_vibracion','relacion_axial_radial','eje_desalineado',
    'corriente','unidad_corriente','corriente_pico','estado_corriente','temperatura_C','nivel_temperatura',
    'GPIO4_V','inclin_X_deg','inclin_Y_deg','inclin_Z_deg','fase_estimada','estado_firmware'];
  const out = ['﻿' + head.join(';')];
  for (const r of rows) out.push([stamp(r.ts), r.ts, r.origen, r.sesion, r.seq ?? '', r.tms ?? '',
    c(r.x), c(r.y), c(r.z), c(r.mag), c(r.vf), r.vibLv ? VIB[r.vibLv][0] : '', c(r.ratio), r.mis ? 'SI' : 'NO',
    c(r.I), r.unit, c(r.Ipk), r.cLv ? CUR[r.cLv][0] : '', c(r.T,2), r.tLv ? TMP[r.tLv][0] : '',
    c(r.V), c(r.ang?.[0],2), c(r.ang?.[1],2), c(r.ang?.[2],2),
    fin(r.fase) ? FASES[r.fase] ?? '' : '', fin(r.estado) ? ESTADOS_FW[r.estado] ?? '' : ''].join(';'));
  download(`motor_datos_${fileStamp()}.csv`, out.join('\r\n') + '\r\n', 'text/csv;charset=utf-8');
};
$('dlEv').onclick = () => {
  const out = ['﻿fecha_hora_local;variable;nivel;severidad;mensaje;reconocido'];
  for (const e of events) out.push([stamp(e.ts), e.variable, e.lv, e.sev, '"' + e.msg.replace(/"/g, '""') + '"', e.ack ? 'SI' : 'NO'].join(';'));
  download(`motor_alarmas_${fileStamp()}.csv`, out.join('\r\n') + '\r\n', 'text/csv;charset=utf-8');
};
$('dlJson').onclick = () => {
  download(`motor_sesion_${fileStamp()}.json`, JSON.stringify({
    generado: new Date().toISOString(), umbrales: cfg, muestras_perdidas: st.missing,
    eventos: events.map(e => ({...e, fecha: stamp(e.ts)})),
    muestras: rows.map(r => ({...r, fecha: stamp(r.ts)}))
  }, null, 1), 'application/json');
};

/* ---------- reloj y vigilancia de conexión ---------- */
setInterval(() => {
  $('clock').textContent = new Date().toLocaleString('es', {dateStyle:'medium', timeStyle:'medium'});
  if (st.mode !== 'live') return;
  const stale = !st.lastAt || Date.now() - st.lastAt > 3000;
  document.querySelectorAll('.kpi').forEach(k => k.classList.toggle('stale', stale && rows.length > 0));
  if (!subscribed) return;
  if (!st.lastAt) setConn('Esperando al ESP32… (pruebe el modo Demo)', 'warn');
  else if (stale) setConn('Sin muestras recientes', 'warn');
  else setConn('● Recibiendo en vivo', 'live');
}, 1000);

/* ---------- inicio ---------- */
applyTheme(store.get('mc-theme') || 'dark');
fillCfg(); buildThermo(); buildScale(); renderLog();
addEventListener('resize', () => dirty = true);
requestAnimationFrame(frame);
setMode('live');
})();
