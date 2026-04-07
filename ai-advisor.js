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
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch { resolve(data); }
      });
    });
    req.on('error', reject);
    if (opts.body) req.write(opts.body);
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

  // 2. Compute metrics for current month
  const now = new Date();
  const monthKey = getMonthKey(now);
  const monthFrom = `01.${String(now.getMonth()+1).padStart(2,'0')}.${now.getFullYear()}`;
  const monthTo = fmtDate(now);
  const yesterday = new Date(now); yesterday.setDate(yesterday.getDate()-1);
  const yestStr = fmtDate(yesterday);
  const dates = getDatesInRange(monthFrom, monthTo);

  // Build ROP groups from MGR_TO_ROP
  const ropGroups = {};
  for (const [mgr, rop] of Object.entries(MGR_TO_ROP)) {
    if (!ropGroups[rop]) ropGroups[rop] = [];
    ropGroups[rop].push(mgr);
  }

  // Aggregate per ROP
  let totalLeads=0, totalDeals=0, totalBudget=0;
  let yestLeads=0, yestDeals=0, yestBudget=0;
  const ropStats = {};

  for (const [crm, mgrs] of Object.entries(MANAGERS)) {
    for (const m of mgrs) {
      if (!m.daily) continue;
      let rop = MGR_TO_ROP[m.name] || 'unknown';
      // Fix corrupted encoding entries
      if (rop.includes('\ufffd')) {
        const clean = rop.replace(/\ufffd+/g, '').toLowerCase().trim();
        if (clean.includes('айдана')) rop = 'РОП Айдана';
        else if (clean.includes('аслиддин')) rop = 'РОП Аслиддин';
        else if (clean.includes('нурдаулет')) rop = 'РОП Нурдаулет';
        else if (clean.includes('диас')) rop = 'РОП Диас';
        else rop = 'unknown';
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
    // Use execFileSync with input to pass UTF-8 prompt via stdin (no shell encoding issues)
    advice = execFileSync('claude', ['--print'], {
      encoding: 'utf-8',
      timeout: 120000,
      input: prompt,
      windowsHide: true
    }).trim();
  } catch (e) {
    console.error('[AI Advisor] Claude CLI error:', e.message);
    process.exit(1);
  }

  console.log('[AI Advisor] Advice:\n' + advice);

  // 4. Save to Supabase — merge AI_ADVICE into existing data
  const currentData = rows[0].data;
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
