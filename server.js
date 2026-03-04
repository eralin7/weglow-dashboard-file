const https = require('https');
const http  = require('http');

const SUPABASE_URL = 'https://bdyakgmeibpdkisbiykt.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJkeWFrZ21laWJwZGtpc2JpeWt0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIyMTQzOTMsImV4cCI6MjA4Nzc5MDM5M30.nA187grJR6XFQRmTP6WOM-6-1dZK1EzYNNP2JH9aAMg';

const ACCOUNTS = [
  { name:'Коллаген', domain:'weglow.amocrm.ru',    token:'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjIyODdkZTdmMzY4OWEzYTE0Y2Q2MmUxZDI4ZTRjZDkyOWEwMjYyNWJhNmJmZWYwYWQzOTRmZjhkY2M3MzFmNTM2ZDQ5YzdkZGQ5YmU2NDZkIn0.eyJhdWQiOiI4NWRhNjc3ZC0xOGUyLTQ5ZjktYjQ1NC1jYTNhMmVhZTlmMWIiLCJqdGkiOiIyMjg3ZGU3ZjM2ODlhM2ExNGNkNjJlMWQyOGU0Y2Q5MjlhMDI2MjViYTZiZmVmMGFkMzk0ZmY4ZGNjNzMxZjUzNmQ0OWM3ZGRkOWJlNjQ2ZCIsImlhdCI6MTc3MjY1NzYyNCwibmJmIjoxNzcyNjU3NjI0LCJleHAiOjE4OTg5ODU2MDAsInN1YiI6IjExNTQ2MzQ2IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMxOTYyMzI2LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiZjZmNDFlM2EtZGE4Yy00YWE3LWFhMGEtZTEyN2IzODE2NzVjIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.IAfT5CDTKXg_UTQKcVFfrS-ZlF3ZSq7a3Mdf--jy8Z2C1YvprTwcQg2SH2b2FsEVAKDqrSgx8Lc-YblLqv4c--vLzgB5lsr-xafuI9af6QWmrsdk_NXypl7CWhv4N84StT_14icwn_AK2k9xknvagNucqrIssW57ua9tmgddFP3x71mCbia8sFmUdTNW8wB2HJNU7jo6drEmo6VxWSGUxzohV3ux3D4ZjhGsDvEngGfLI4AHrbPsf2WmWFn9mATN0kd4b742Bu1iOELKDnZeu45a63p7AQBd2XUIwxfHCbwmTOMMB-Ea-7HkmDBv-buThHWU-Vcj0krLxx3U0NFuwA' },
  { name:'Ummi',     domain:'erbbolx.amocrm.ru',   token:'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImFjZjgwYjY5MzFhMTNmNzRkOGE3ZTdlZWVhODQ2MjU2ZDQzM2Q3N2QxZmUxZTRjYjMxM2FhYTMxMmRhY2NjNDM1YzE3NTdkMjI1NTM1ZjM4In0.eyJhdWQiOiI5OGI5MDQ1OC1jMThiLTRjYTItYWRmMS1mNzFhMDc0ZGZiYzMiLCJqdGkiOiJhY2Y4MGI2OTMxYTEzZjc0ZDhhN2U3ZWVlYTg0NjI1NmQ0MzNkNzdkMWZlMWU0Y2IzMTNhYWEzMTJkYWNjYzQzNWMxNzU3ZDIyNTUzNWYzOCIsImlhdCI6MTc3MjY1NzY4OSwibmJmIjoxNzcyNjU3Njg5LCJleHAiOjE4OTkwNzIwMDAsInN1YiI6IjEzMjU1OTE0IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyNzgzNjE0LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiMWFhNzhkNzEtYTIxMC00ODVhLWI1NmUtY2JiMDYyNjA3YjJmIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.kBk7CpSC3CsMsD-xfDsP-4cDiTNqG9FnbVisIMgsK9pvEfwf-QKuimb2JAOEhsrJXjO-iswi8dTZJL6uee0PHhpjZMRj03-ieO190VpzR1nbCjUffjhC6TNVvHmSsfJNs8wQKu-3TVXldnNT1sQyejCPHDdvLom4GXFePTNsClOt_Q3jZadxvcAKTt4VOjgKm2WTLfZWt3afjsj1GHPBW0KncvLpZvbXWcrfyaH2GusEuGP5hFfU4CXN5kUo03CQmc8TH1LkIaL8uHNIzbhJBZdg4sc4jDmLDjRw6vV-1J8Cas9TvYgEkJ7Rle4PSOLhce-Cnywch_a-bd7DbRKkPg' },
  { name:'Кофе',     domain:'mushrooms.amocrm.ru',  token:'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImJlZmVhMzE1OGNiMjc4ZjgxYmIwYTllZDcyNDRkMDFjNDdkMDJiMWY1Zjk3NzcyOGJlZGNjY2Q5ODBhMGU4YmRiYzVkNGNlNzYzOTcwZjRjIn0.eyJhdWQiOiJmNDliNzY0ZC1iM2EwLTQzZjQtODczYi0yYzk2ZGEwYmEyNmMiLCJqdGkiOiJiZWZlYTMxNThjYjI3OGY4MWJiMGE5ZWQ3MjQ0ZDAxYzQ3ZDAyYjFmNWY5Nzc3MjhiZWRjY2NkOTgwYTBlOGJkYmM1ZDRjZTc2Mzk3MGY0YyIsImlhdCI6MTc3MjY1Nzc0MSwibmJmIjoxNzcyNjU3NzQxLCJleHAiOjE4OTkwNzIwMDAsInN1YiI6IjIzOTc4NjUiLCJncmFudF90eXBlIjoiIiwiYWNjb3VudF9pZCI6MzI4ODY4NzAsImJhc2VfZG9tYWluIjoiYW1vY3JtLnJ1IiwidmVyc2lvbiI6Miwic2NvcGVzIjpbInB1c2hfbm90aWZpY2F0aW9ucyIsImZpbGVzIiwiY3JtIiwibm90aWZpY2F0aW9ucyJdLCJoYXNoX3V1aWQiOiIwMTI3ODQ4NS00NjQ2LTRkMGEtOTQ4Ni1kZDZiNmJmM2M1YTYiLCJhcGlfZG9tYWluIjoiYXBpLWIuYW1vY3JtLnJ1In0.hHlVdsC4TpTfqdeefNyn4OFXdRMwzEuq7c3QPgrK86gHom2aypje6tx4WLbbwZJ8Jm5aEqctQH9zZF4CliB9oB9bghAn66ElAHSmmhfnxIsrfXWecPErPN9WiD6edBlpyPHaoP6JjhBKmJ2mkBWaeV0U52L50aoglTy5nPRbdKa3kXBFAqZQo3L8_sN5jhvbBwsieAr6F_CAfjYJani_qEAQ9egSeoE8xJBv5S1ll6U28F2NPeRqMYqjPUAAKNtje2eTuRWXk5IsP4OsGaLi4AKctRBvanMvfUmQu-5GJ6XwdOaSuShWw36ryVVijrIQ4mdmbRnZLsZT7Th53ipiYg' },
];

const VALID_STAGES = new Set(['заказ','заказ на подтверждение','курьерская доставка','успешно реализовано']);
const SYNC_INTERVAL_MS = 60 * 1000;

// ── HTTP helper ──────────────────────────────────────────────────
function fetchJSON(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const p = new URL(url);
    const req = https.request({
      hostname: p.hostname,
      path:     p.pathname + p.search,
      method:   opts.method || 'GET',
      headers:  opts.headers || {},
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode === 204) { resolve({}); return; }
        if (res.statusCode >= 400) { reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0,200)}`)); return; }
        try { resolve(JSON.parse(data)); } catch(e) { resolve({}); }
      });
    });
    req.on('error', reject);
    if (opts.body) req.write(opts.body);
    req.end();
  });
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── AmoCRM helpers ───────────────────────────────────────────────
const amoGet = (domain, token, path) =>
  fetchJSON(`https://${domain}${path}`, { headers: { Authorization: `Bearer ${token}` } });

async function loadPipelines(acc) {
  const map = {};
  try {
    const d = await amoGet(acc.domain, acc.token, '/api/v4/leads/pipelines?limit=250');
    for (const p of (d?._embedded?.pipelines || []))
      for (const s of (p._embedded?.statuses || []))
        map[s.id] = s.name;
    console.log(`[${acc.name}] ${Object.keys(map).length} statuses loaded`);
  } catch(e) { console.error(`[${acc.name}] pipelines:`, e.message); }
  return map;
}

async function findOrderField(acc) {
  try {
    const d = await amoGet(acc.domain, acc.token, '/api/v4/leads/custom_fields?limit=250');
    const fields = d?._embedded?.custom_fields || [];
    console.log(`[${acc.name}] Custom fields:`);
    fields.forEach(f => console.log(`  ${f.id}: "${f.name}" (${f.type})`));
    const f = fields.find(f => f.name.toLowerCase().includes('дата заказа') && !f.name.toLowerCase().includes('первого'))
           || fields.find(f => f.name.toLowerCase().includes('заказа') && f.type === 'date');
    if (f) { console.log(`[${acc.name}] ✅ orderField: ${f.id} "${f.name}"`); return f.id; }
    console.log(`[${acc.name}] ⚠ No order date field found`);
    return null;
  } catch(e) { console.error(`[${acc.name}] findOrderField:`, e.message); return null; }
}

async function loadUsers(acc) {
  const map = {};
  try {
    const d = await amoGet(acc.domain, acc.token, '/api/v4/users?limit=250');
    for (const u of (d?._embedded?.users || [])) map[u.id] = u.name;
    console.log(`[${acc.name}] ${Object.keys(map).length} users loaded`);
  } catch(e) { console.error(`[${acc.name}] users:`, e.message); }
  return map;
}

async function fetchAllLeads(acc) {
  const leads = []; let page = 1;
  while (true) {
    try {
      const d = await amoGet(acc.domain, acc.token, `/api/v4/leads?limit=250&page=${page}`);
      const items = d?._embedded?.leads || [];
      if (!items.length) break;
      leads.push(...items);
      if (items.length < 250) break;
      page++;
      await sleep(150);
    } catch(e) { console.error(`[${acc.name}] page ${page}:`, e.message); break; }
  }
  console.log(`[${acc.name}] ${leads.length} leads fetched`);
  return leads;
}

// ── Parse leads ──────────────────────────────────────────────────
const fmtDate = ts => {
  const d = new Date(Number(ts) * 1000);
  return `${String(d.getDate()).padStart(2,'0')}.${String(d.getMonth()+1).padStart(2,'0')}.${d.getFullYear()}`;
};

const getField = (lead, id) => id
  ? (lead.custom_fields_values || []).find(x => x.field_id === id)?.values?.[0]?.value ?? null
  : null;

function parseLeads(leads, acc, statusMap, userMap) {
  const daily = {}, mgrs = {};
  for (const lead of leads) {
    const stage      = (statusMap[lead.status_id] || '').toLowerCase();
    const validStage = VALID_STAGES.has(stage);
    const mgrName    = userMap[lead.responsible_user_id] || '';
    const budget     = lead.price || 0;
    const cDate      = lead.created_at ? fmtDate(lead.created_at) : null;
    const orderVal   = getField(lead, acc.orderDateFieldId);
    const oDate      = (orderVal && validStage) ? fmtDate(orderVal) : null;

    if (cDate) {
      if (!daily[cDate]) daily[cDate] = [0,0,0];
      daily[cDate][0]++;
      if (mgrName) {
        if (!mgrs[mgrName]) mgrs[mgrName] = {leads:0,deals:0,budget:0,daily:{}};
        mgrs[mgrName].leads++;
        if (!mgrs[mgrName].daily[cDate]) mgrs[mgrName].daily[cDate] = [0,0,0];
        mgrs[mgrName].daily[cDate][0]++;
      }
    }
    if (oDate) {
      if (!daily[oDate]) daily[oDate] = [0,0,0];
      daily[oDate][1]++; daily[oDate][2] += budget;
      if (mgrName) {
        if (!mgrs[mgrName]) mgrs[mgrName] = {leads:0,deals:0,budget:0,daily:{}};
        mgrs[mgrName].deals++; mgrs[mgrName].budget += budget;
        if (!mgrs[mgrName].daily[oDate]) mgrs[mgrName].daily[oDate] = [0,0,0];
        mgrs[mgrName].daily[oDate][1]++; mgrs[mgrName].daily[oDate][2] += budget;
      }
    }
  }
  const managers = Object.entries(mgrs).map(([name,v]) => ({
    name, leads:v.leads, deals:v.deals, budget:Math.round(v.budget),
    conv: v.leads>0 ? +(v.deals/v.leads*100).toFixed(1) : 0,
    avgCheck: v.deals>0 ? Math.round(v.budget/v.deals) : 0,
    daily: v.daily,
  })).sort((a,b) => b.budget-a.budget);
  return { daily, managers };
}

// ── Supabase ─────────────────────────────────────────────────────
const sbHeaders = {
  'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`,
  'Content-Type': 'application/json', 'Prefer': 'return=minimal',
};
const sbGet  = path => fetchJSON(`${SUPABASE_URL}/rest/v1/${path}`, { headers: sbHeaders });
const sbDel  = path => fetchJSON(`${SUPABASE_URL}/rest/v1/${path}`, { method:'DELETE', headers: sbHeaders });
const sbPost = (path, body) => fetchJSON(`${SUPABASE_URL}/rest/v1/${path}`, { method:'POST', headers: sbHeaders, body: JSON.stringify(body) });

async function sbSave(payload) {
  try { await sbDel('weglow_data?id=eq.1'); } catch(e) {}
  await sbPost('weglow_data', { id:1, data:payload, updated_at: new Date().toISOString() });
}

// ── Caches ───────────────────────────────────────────────────────
const statusCache = {}, fieldCache = {}, userCache = {};

// ── Main sync ────────────────────────────────────────────────────
let lastSync='', syncStatus='Ожидание...', syncErrors=[], isSyncing=false;

async function syncAll() {
  if (isSyncing) { console.log('[SKIP] Already syncing'); return; }
  isSyncing = true;
  const t0 = Date.now();
  console.log(`\n[${new Date().toISOString()}] ══ SYNC START ══`);
  syncErrors = [];
  const RAW = {}, MANAGERS = {};

  for (const acc of ACCOUNTS) {
    try {
      if (!statusCache[acc.name]) { statusCache[acc.name] = await loadPipelines(acc); await sleep(200); }
      if (fieldCache[acc.name] === undefined) { fieldCache[acc.name] = await findOrderField(acc); await sleep(200); }
      if (!userCache[acc.name])  { userCache[acc.name]  = await loadUsers(acc);     await sleep(200); }
      acc.orderDateFieldId = fieldCache[acc.name];

      const leads = await fetchAllLeads(acc);
      const { daily, managers } = parseLeads(leads, acc, statusCache[acc.name], userCache[acc.name]);
      RAW[acc.name] = daily; MANAGERS[acc.name] = managers;

      const deals  = managers.reduce((s,m) => s+m.deals, 0);
      const budget = managers.reduce((s,m) => s+m.budget, 0);
      console.log(`[${acc.name}] ✅ ${Object.keys(daily).length} дней | ${deals} сделок | ${(budget/1e6).toFixed(1)}M ₸`);
    } catch(e) {
      console.error(`[${acc.name}] ❌`, e.message);
      syncErrors.push(`${acc.name}: ${e.message}`);
      delete statusCache[acc.name]; delete fieldCache[acc.name]; delete userCache[acc.name];
    }
  }

  // Preserve AD_SPEND from ad.html
  let AD_SPEND = {};
  try { const r = await sbGet('weglow_data?id=eq.1&select=data'); if (r[0]?.data?.AD_SPEND) AD_SPEND = r[0].data.AD_SPEND; } catch(e) {}

  await sbSave({ RAW, MANAGERS, AD_SPEND, updatedAt: new Date().toISOString() });

  const elapsed = ((Date.now()-t0)/1000).toFixed(1);
  lastSync = new Date().toISOString();
  syncStatus = syncErrors.length ? `Частично за ${elapsed}с (${syncErrors.length} ошибок)` : `✅ OK за ${elapsed}с`;
  console.log(`[DONE] ${syncStatus}\n`);
  isSyncing = false;
}

// ── HTTP Server ──────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (req.url === '/sync') {
    syncAll().catch(console.error);
    res.end(JSON.stringify({ message: 'Sync triggered' }));
    return;
  }
  if (req.url === '/clear-cache') {
    ACCOUNTS.forEach(a => { delete statusCache[a.name]; delete fieldCache[a.name]; delete userCache[a.name]; });
    res.end(JSON.stringify({ message: 'Cache cleared' }));
    return;
  }

  res.end(JSON.stringify({
    status: isSyncing ? '⏳ syncing' : '✅ idle',
    lastSync, syncStatus, syncErrors,
    nextSync: lastSync ? new Date(new Date(lastSync).getTime() + SYNC_INTERVAL_MS).toISOString() : 'soon',
    accounts: ACCOUNTS.map(a => ({
      name: a.name, domain: a.domain,
      orderDateFieldId: fieldCache[a.name] ?? 'не найдено',
      statuses: Object.keys(statusCache[a.name]||{}).length,
      users:    Object.keys(userCache[a.name]||{}).length,
    })),
    endpoints: { status:'GET /', sync:'GET /sync', clearCache:'GET /clear-cache' }
  }, null, 2));
}).listen(PORT, () => {
  console.log(`🚀 WeGlow AutoSync | port ${PORT} | interval ${SYNC_INTERVAL_MS/1000}s`);
  syncAll();
  setInterval(syncAll, SYNC_INTERVAL_MS);
});
