#!/usr/bin/env node
// AI Advisor — generates sales recommendations via Claude CLI and saves to Supabase
// Run: node ai-advisor.js
// Schedule: every hour via Windows Task Scheduler

const https = require('https');
const { execFileSync } = require('child_process');

const SUPABASE_URL = 'https://bdyakgmeibpdkisbiykt.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJkeWFrZ21laWJwZGtpc2JpeWt0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIyMTQzOTMsImV4cCI6MjA4Nzc5MDM5M30.nA187grJR6XFQRmTP6WOM-6-1dZK1EzYNNP2JH9aAMg';

function sbFetch(path, opts = {}) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${SUPABASE_URL}/rest/v1/${path}`);
    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: opts.method || 'GET',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        ...(opts.headers || {})
      }
    };
    const req = https.request(options, res => {
      res.setEncoding('utf8');
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch { resolve(data); }
      });
    });
    req.on('error', reject);
    if (opts.body) req.write(opts.body, 'utf8');
    req.end();
  });
}

function fmt(n) {
  return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
}

function getMonthKey(date) {
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const yy = String(date.getFullYear());
  return `${mm}.${yy}`;
}

function fmtDate(d) {
  return `${String(d.getDate()).padStart(2,'0')}.${String(d.getMonth()+1).padStart(2,'0')}.${d.getFullYear()}`;
}

function getDatesInRange(from, to) {
  const dates = [];
  const [d1,m1,y1] = from.split('.').map(Number);
  const [d2,m2,y2] = to.split('.').map(Number);
  const start = new Date(y1, m1-1, d1);
  const end = new Date(y2, m2-1, d2);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate()+1)) {
    dates.push(fmtDate(d));
  }
  return dates;
}

async function main() {
  console.log('[AI Advisor] Starting...');

  // 1. Read data from Supabase
  const rows = await sbFetch('weglow_data?id=eq.1&select=data');
  if (!rows || !rows[0] || !rows[0].data) {
    console.error('[AI Advisor] No data in Supabase');
    process.exit(1);
  }
  const d = rows[0].data;
  const MANAGERS = d.MANAGERS || {};
  const ROP_PLANS = d.ROP_PLANS || {};
  const MGR_TO_ROP = d.MGR_TO_ROP || {};
  const AD_SPEND = d.AD_SPEND || {};

  // ── Merge archive data (same logic as dashboard) ──
  const _isArchiveDate = dt => { const [dd,mm,yy]=dt.split('.').map(Number); return yy<2026||(yy===2026&&mm<4); };
  function _normName(n) { return n.toLowerCase().replace(/\ufffd/g,'').replace(/\s+/g,' ').trim(); }
  function _nameKey(n) { return n.toLowerCase().replace(/\ufffd/g,'').replace(/^пин\s+\d+\s+/i,'').replace(/\s+/g,' ').trim(); }

  if (d.ARCHIVE_MANAGERS) {
    for (const [acc, archMgrs] of Object.entries(d.ARCHIVE_MANAGERS)) {
      if (!MANAGERS[acc]) MANAGERS[acc] = [];
      const liveExact = {}, liveNorm = {}, liveNameKey = {};
      MANAGERS[acc].forEach(m => {
        liveExact[m.name] = m;
        liveNorm[_normName(m.name)] = m;
        const nk = _nameKey(m.name);
        if (nk.length > 2) liveNameKey[nk] = m;
      });
      for (const am of archMgrs) {
        const match = liveExact[am.name] || liveNorm[_normName(am.name)] || liveNameKey[_nameKey(am.name)];
        if (match) {
          if (!match.daily) match.daily = {};
          for (const [date, vals] of Object.entries(am.daily || {})) {
            if (_isArchiveDate(date)) match.daily[date] = vals;
            else if (!match.daily[date]) match.daily[date] = vals;
          }
          let tl=0,td=0,tb=0;
          for (const vals of Object.values(match.daily)) { tl+=vals[0]||0; td+=vals[1]||0; tb+=vals[2]||0; }
          match.leads=tl; match.deals=td; match.budget=tb;
        } else {
          MANAGERS[acc].push(am);
        }
      }
    }
  }

  // ── Split Ummi by date: ≤ 23.03.2026 → Ербол, ≥ 24.03.2026 → Ummi (KIDS) ──
  if (!MANAGERS['Ummi']) MANAGERS['Ummi'] = [];
  const _erbolCutoff = new Date(2026, 2, 23);
  MANAGERS['Ербол'] = [];
  const kidsUmmi = [];
  for (const m of (MANAGERS['Ummi'] || [])) {
    const erbDaily = {}, kidsDaily = {};
    for (const [date, vals] of Object.entries(m.daily || {})) {
      const [dd,mm,yy] = date.split('.').map(Number);
      const dt = new Date(yy, mm-1, dd);
      if (dt <= _erbolCutoff) erbDaily[date] = vals;
      else kidsDaily[date] = vals;
    }
    if (Object.keys(erbDaily).length) {
      let l=0,dl=0,b=0;
      for (const v of Object.values(erbDaily)) { l+=v[0]||0; dl+=v[1]||0; b+=v[2]||0; }
      MANAGERS['Ербол'].push({ name: m.name, leads:l, deals:dl, budget:b, daily: erbDaily });
    }
    if (Object.keys(kidsDaily).length) {
      let l=0,dl=0,b=0;
      for (const v of Object.values(kidsDaily)) { l+=v[0]||0; dl+=v[1]||0; b+=v[2]||0; }
      kidsUmmi.push({ name: m.name, leads:l, deals:dl, budget:b, daily: kidsDaily });
    }
  }
  MANAGERS['Ummi'] = kidsUmmi;

  // ── Fix MGR_TO_ROP encoding + auto-map by person name ──
  for (const [k,v] of Object.entries(MGR_TO_ROP)) {
    if (v.includes('\ufffd')) {
      const lower = v.replace(/\ufffd+/g, '').toLowerCase().trim();
      if (lower.includes('айдана')) MGR_TO_ROP[k] = 'РОП Айдана';
      else if (lower.includes('аслиддин')) MGR_TO_ROP[k] = 'РОП Аслиддин';
      else if (lower.includes('нурдаулет')) MGR_TO_ROP[k] = 'РОП Нурдаулет';
      else if (lower.includes('диас')) MGR_TO_ROP[k] = 'РОП Диас KIDS';
      else delete MGR_TO_ROP[k];
    }
  }
  const _ropByName = {};
  for (const [k, v] of Object.entries(MGR_TO_ROP)) {
    const nk = _nameKey(k);
    if (nk.length > 2 && !_ropByName[nk]) _ropByName[nk] = v;
  }
  for (const crm of ['Ummi', 'Коллаген', 'Ербол']) {
    for (const m of (MANAGERS[crm] || [])) {
      if (!MGR_TO_ROP[m.name]) {
        const nk = _nameKey(m.name);
        if (nk.length > 2 && _ropByName[nk]) MGR_TO_ROP[m.name] = _ropByName[nk];
      }
    }
  }

  // ── Virtual CRMs: Коллаген aggregate includes Ummi + Ербол ──
  const _VIRTUAL_CRMS = { 'Коллаген': ['Ummi', 'Ербол'] };

  console.log('[AI Advisor] Data loaded — CRMs:', Object.keys(MANAGERS).join(', '),
    '| Kol mgrs:', (MANAGERS['Коллаген']||[]).length,
    '| Ummi mgrs:', (MANAGERS['Ummi']||[]).length);

  // 2. Compute metrics for current month
  const now = new Date();
  const monthKey = getMonthKey(now);
  const monthFrom = `01.${String(now.getMonth()+1).padStart(2,'0')}.${now.getFullYear()}`;
  const monthTo = fmtDate(now);
  const yesterday = new Date(now); yesterday.setDate(yesterday.getDate()-1);
  const yestStr = fmtDate(yesterday);
  const dates = getDatesInRange(monthFrom, monthTo);

  // Aggregate per ROP — simple approach: iterate all CRM managers, group by MGR_TO_ROP
  let totalLeads=0, totalDeals=0, totalBudget=0;
  let yestLeads=0, yestDeals=0, yestBudget=0;
  const ropStats = {};

  for (const [crm, mgrs] of Object.entries(MANAGERS)) {
    for (const m of mgrs) {
      if (!m.daily) continue;
      // Determine ROP for this manager
      let rop = MGR_TO_ROP[m.name] || 'unknown';
      // For KIDS CRM (Ummi), managers map to KIDS ROP variant
      if (crm === 'Ummi' && !rop.includes('KIDS')) {
        if (rop.startsWith('РОП ')) rop = rop + ' KIDS';
      }
      if (!ropStats[rop]) ropStats[rop] = { leads:0, deals:0, budget:0 };

      for (const date of dates) {
        if (m.daily[date]) {
          const [l,dl,b] = m.daily[date];
          totalLeads += l||0; totalDeals += dl||0; totalBudget += b||0;
          ropStats[rop].leads += l||0;
          ropStats[rop].deals += dl||0;
          ropStats[rop].budget += b||0;
        }
      }
      if (m.daily[yestStr]) {
        const [l,dl,b] = m.daily[yestStr];
        yestLeads += l||0; yestDeals += dl||0; yestBudget += b||0;
      }
    }
  }

  // Ad spend for current month
  let adTotal = 0;
  for (const [key, val] of Object.entries(AD_SPEND)) {
    if (key.endsWith(monthKey)) {
      adTotal += val || 0;
    }
  }

  const conv = totalLeads > 0 ? +(totalDeals/totalLeads*100).toFixed(1) : 0;
  const avgCheck = totalDeals > 0 ? Math.round(totalBudget/totalDeals) : 0;
  const drr = totalBudget > 0 ? +(adTotal/totalBudget*100).toFixed(1) : 0;

  // ROP breakdown
  let ropLines = '';
  for (const [rop, st] of Object.entries(ropStats)) {
    if (rop === 'unknown') continue;
    const plan = ROP_PLANS[rop]?.[monthKey] || 0;
    const rConv = st.leads > 0 ? +(st.deals/st.leads*100).toFixed(1) : 0;
    ropLines += `\n- ${rop}: план ${plan ? fmt(plan)+'₸' : '—'}, факт ${fmt(st.budget)}₸, лидов ${st.leads}, сделок ${st.deals}, конв ${rConv}%`;
  }

  const metrics = `Период: ${monthFrom} — ${monthTo}
Месяц: продажи ${fmt(totalBudget)}₸, лидов ${totalLeads}, сделок ${totalDeals}, конверсия ${conv}%, средний чек ${fmt(avgCheck)}₸
ДРР: ${drr}%, расход на рекламу ${fmt(adTotal)}₸
Вчера: продажи ${fmt(yestBudget)}₸, лидов ${yestLeads}, сделок ${yestDeals}
Группы:${ropLines}`;

  console.log('[AI Advisor] Metrics:\n' + metrics);

  // 3. Call Claude CLI
  const prompt = `Ты — ИИ-советник для отдела продаж WeGlow (Казахстан, косметика/БАД). Проанализируй метрики и дай 3-5 кратких конкретных рекомендаций на русском. Формат: маркированный список, каждый пункт — конкретное действие. Без вступлений.\n\nТекущие метрики:\n${metrics}`;

  let advice;
  try {
    // Get raw Buffer to avoid Windows code page corruption, then decode as UTF-8
    const buf = execFileSync('claude', ['--print'], {
      timeout: 120000,
      input: Buffer.from(prompt, 'utf-8'),
      windowsHide: true,
      maxBuffer: 10 * 1024 * 1024,
      env: { ...process.env, LANG: 'en_US.UTF-8', PYTHONIOENCODING: 'utf-8' }
    });
    advice = buf.toString('utf-8').trim();
    // Strip any remaining replacement characters from encoding issues
    advice = advice.replace(/\ufffd/g, '');
  } catch (e) {
    console.error('[AI Advisor] Claude CLI error:', e.message);
    process.exit(1);
  }

  console.log('[AI Advisor] Advice:\n' + advice);

  // 4. Save to Supabase — re-read fresh data to avoid stale/corrupted overwrites
  const freshRows = await sbFetch('weglow_data?id=eq.1&select=data', {});
  const currentData = freshRows[0].data;
  currentData.AI_ADVICE = { text: advice, ts: Date.now() };

  const patchBody = JSON.stringify({ data: currentData, updated_at: new Date().toISOString() });
  console.log(`[AI Advisor] PATCH body size: ${(patchBody.length/1024/1024).toFixed(2)} MB`);

  const patchRes = await sbFetch('weglow_data?id=eq.1', {
    method: 'PATCH',
    headers: { 'Prefer': 'return=minimal' },
    body: patchBody
  });
  console.log('[AI Advisor] PATCH response:', JSON.stringify(patchRes).substring(0, 500));
  console.log('[AI Advisor] Done!');
}

main().catch(e => { console.error(e); process.exit(1); });
