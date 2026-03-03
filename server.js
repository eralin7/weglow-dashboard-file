require('dotenv').config();
const express = require('express');
const cron    = require('node-cron');
const axios   = require('axios');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(express.json());

// ════════════════════════════════════════════════════════════════
// КОНФИГУРАЦИЯ
// ════════════════════════════════════════════════════════════════
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

const ACCOUNTS = [
  {
    name:   'Коллаген',
    domain: 'weglow.amocrm.ru',
    token:  'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImRjNjU5MGI2MmExNjJjMjBiMjY0Mzk2ZjY2NTRjMWQyYWQzNjY4Yzc3MjdmOTQ2YTcwZDAyN2I0MjkyYWQ3Yzk0ZjAwNGEzMzQyZmJjNmM5In0.eyJhdWQiOiI4NWRhNjc3ZC0xOGUyLTQ5ZjktYjQ1NC1jYTNhMmVhZTlmMWIiLCJqdGkiOiJkYzY1OTBiNjJhMTYyYzIwYjI2NDM5NmY2NjU0YzFkMmFkMzY2OGM3NzI3Zjk0NmE3MGQwMjdiNDI5MmFkN2M5NGYwMDRhMzM0MmZiYzZjOSIsImlhdCI6MTc3MjQwMzA5OCwibmJmIjoxNzcyNDAzMDk4LCJleHAiOjE4ODM4NjU2MDAsInN1YiI6IjExNTQ2MzQ2IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMxOTYyMzI2LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiNDI5ZTFjZTgtMDdlZC00MjkyLWIwZTYtZGUzYTE3ZDJlMDlhIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.Kc8HmfCitHEXupyER6UgPlmm1tj47fd1eUBUIkRy9irdKrBhXnLkD7qCgKVipG-QG5Uv4213zu-8s5qYzjoRtNRt8LSmnvx_jLm3igTo2iXRdnPDVlc488oiH8EVKlwuZmpE85eg2t-oJKLm9OuLiVS67SfTedXQLJpDGiUHS1WopOZFIEec9pT173UL4IEAeDz5kZXAxAbz2i0DxQ2m4pESkk3oO_b22YsMrxXRf40b34oPnRaJsM9xcLZ_xxfbnZkNE36j4chGNTkyi0nAwwnnFsuZyAU0JJTev4Byb88q20ellOadvXO-MZEhSfe7SS7sUbSVSjvABoAvTEMrEA',
  },
  {
    name:   'Ummi',
    domain: 'erbbolx.amocrm.ru',
    token:  'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImM5NDFiOTIxMzQ0MmM2NThiYWNhZjg0MjBmNjZmYmE3MmRmYWNjNGQ0YTRjODZlOWUzOGM0NWRkNGUwMTQ1ZGQ1NjFiNTQ4N2UyNzkzNDhlIn0.eyJhdWQiOiI5OGI5MDQ1OC1jMThiLTRjYTItYWRmMS1mNzFhMDc0ZGZiYzMiLCJqdGkiOiJjOTQxYjkyMTM0NDJjNjU4YmFjYWY4NDIwZjY2ZmJhNzJkZmFjYzRkNGE0Yzg2ZTllMzhjNDVkZDRlMDE0NWRkNTYxYjU0ODdlMjc5MzQ4ZSIsImlhdCI6MTc3MjQwMzE4MiwibmJmIjoxNzcyNDAzMTgyLCJleHAiOjE4ODEzNjAwMDAsInN1YiI6IjEzMjU1OTE0IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyNzgzNjE0LCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJmaWxlcyIsImNybSIsIm5vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiNDNiZGQyNTktMTBmZS00MDI5LTlkZTMtNGRlYzM0ZmUxYjM3IiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.ZvZObqcilIqFbQZdujidfvDqs-AnEQ9i29SrZBRF2lZ2Dz_Em3RGeM4iwMVXkZl0m2O0JSKoJaUtFK1PGC8ArFBZPbpNz0PPcMILZ_xQiKj5eUnuQAyO-09RSupxvQ9k3pbfl1VFC2edZsr0M918B5S4aDLCUUB6zFvZ3C_n9JRzy85D93b_9gWTuEgSOtXezdyvOmEOK9Qwdczwnjyy94cA6k6aEAKZpJK6HJSGvBn5Eyfo9zt4oFIbJK2GTC9ckUx2eaOgQVgqav-0mw3j3XEv0W3A2WZs-e4hg0SjdA6mEhKUjsxjzEwb-ljuOz7r2qFlD8HO5FtAF2HPKKiTRg',
  },
  {
    name:   'Кофе',
    domain: 'mushrooms.amocrm.ru',
    token:  'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6Ijg5OGJjZTBmNTU2NjY2NWY2N2E2MzI4ZWM2OTE3ZjMzOWM5Zjk3YTEzZDdlMDJhMmZiOTFjZjBlNzU4NTdkZmM4YWJiODc2YmIxN2EwMTZhIn0.eyJhdWQiOiJmNDliNzY0ZC1iM2EwLTQzZjQtODczYi0yYzk2ZGEwYmEyNmMiLCJqdGkiOiI4OThiY2UwZjU1NjY2NjVmNjdhNjMyOGVjNjkxN2YzMzljOWY5N2ExM2Q3ZTAyYTJmYjkxY2YwZTc1ODU3ZGZjOGFiYjg3NmJiMTdhMDE2YSIsImlhdCI6MTc3MjQwMzI3NiwibmJmIjoxNzcyNDAzMjc2LCJleHAiOjE4NzU4MzA0MDAsInN1YiI6IjIzOTc4NjUiLCJncmFudF90eXBlIjoiIiwiYWNjb3VudF9pZCI6MzI4ODY4NzAsImJhc2VfZG9tYWluIjoiYW1vY3JtLnJ1IiwidmVyc2lvbiI6Miwic2NvcGVzIjpbInB1c2hfbm90aWZpY2F0aW9ucyIsImZpbGVzIiwiY3JtIiwibm90aWZpY2F0aW9ucyJdLCJoYXNoX3V1aWQiOiJiYTViNzEwMi0yNjdmLTQ0ZDQtODFkZi05OTRkZGUzYzM3MDUiLCJhcGlfZG9tYWluIjoiYXBpLWIuYW1vY3JtLnJ1In0.CaKuHGL2AumUUfY4bN5_OLJ2TivVQmPMapSkRAa5ltcfAGACt443RWFTvl64uSFG57CBnpy5Zu3tsHaatJIA2_0lXKLao2svbsL9a99QbUTiTN34FPgiQLuzXc19UTmnP7FE73ULlZ4CfTvAZ1dVGL5VlytmsEwtgDobK0vzhBIWSvcVd5VPRP5EkxMFZT8qH3vevJjhEsCKFvoomXLDn7zWSBmN8Lw7TnRUZ9N8bYMrUeehjPVu5zsEJsT4HfQQKdnEqB5LXk8WngTLbrv9rl3NKGeQBKlNYfre_b0O2iEZ2PNf8d0Q8MBOhNlIrUSPJeASk9shTABo8709VJ3ezg',
  },
];

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ════════════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════════════
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function tsToDate(ts) {
  if (!ts) return null;
  const d = new Date(ts * 1000);
  const dd = String(d.getDate()).padStart(2,'0');
  const mm = String(d.getMonth()+1).padStart(2,'0');
  const yy = d.getFullYear();
  return `${dd}.${mm}.${yy}`;
}

// ════════════════════════════════════════════════════════════════
// FETCH ALL LEADS
// ════════════════════════════════════════════════════════════════
async function fetchAllLeads(account) {
  const { domain, token, name } = account;
  const headers = { 'Authorization': `Bearer ${token}` };
  let allLeads = [], page = 1;

  console.log(`[${name}] Начинаем загрузку сделок...`);

  while (true) {
    try {
      const res = await axios.get(`https://${domain}/api/v4/leads`, {
        headers,
        params: { page, limit: 250, with: 'contacts', order: 'created_at' },
        timeout: 30000,
      });
      const leads = res.data?._embedded?.leads || [];
      if (!leads.length) break;
      allLeads = allLeads.concat(leads);
      console.log(`[${name}] Страница ${page}: ${leads.length} сделок (всего: ${allLeads.length})`);
      if (leads.length < 250) break;
      page++;
      await sleep(300);
    } catch (err) {
      if (err.response?.status === 204 || err.response?.status === 404) break;
      if (err.response?.status === 429) { await sleep(5000); continue; }
      console.error(`[${name}] Ошибка загрузки стр.${page}:`, err.message);
      break;
    }
  }
  return allLeads;
}

// ════════════════════════════════════════════════════════════════
// FETCH USERS
// ════════════════════════════════════════════════════════════════
async function fetchUsers(account) {
  try {
    const res = await axios.get(`https://${account.domain}/api/v4/users`, {
      headers: { 'Authorization': `Bearer ${account.token}` },
      timeout: 15000,
    });
    const map = {};
    (res.data?._embedded?.users || []).forEach(u => { map[u.id] = u.name; });
    return map;
  } catch (e) {
    console.warn('fetchUsers error:', e.message);
    return {};
  }
}

// ════════════════════════════════════════════════════════════════
// FIND ORDER DATE FIELD
// ════════════════════════════════════════════════════════════════
async function findOrderDateFieldId(account) {
  try {
    const res = await axios.get(`https://${account.domain}/api/v4/leads/custom_fields`, {
      headers: { 'Authorization': `Bearer ${account.token}` },
      timeout: 15000,
    });
    const fields = res.data?._embedded?.custom_fields || [];
    const field = fields.find(f =>
      f.name?.toLowerCase().includes('заказ') ||
      f.name?.toLowerCase().includes('order date') ||
      f.name?.toLowerCase().includes('дата заказа')
    );
    if (field) {
      console.log(`[${account.name}] Поле "Дата заказа": ID=${field.id}, name="${field.name}"`);
      return field.id;
    }
    const dateFields = fields.filter(f => f.field_type === 'date' || f.field_type === 'date_time');
    console.log(`[${account.name}] Дата-поля: ${dateFields.map(f => `${f.id}:"${f.name}"`).join(', ')}`);
    return null;
  } catch (e) {
    console.warn(`[${account.name}] fetchCustomFields error:`, e.message);
    return null;
  }
}

// ════════════════════════════════════════════════════════════════
// PROCESS LEADS
// ════════════════════════════════════════════════════════════════
function processLeads(leads, userMap, orderDateFieldId) {
  const daily    = {};
  const managers = {};

  for (const lead of leads) {
    const mgr    = userMap[lead.responsible_user_id] || 'Неизвестно';
    const budget = lead.price || 0;

    const cDate = tsToDate(lead.created_at);
    if (cDate) {
      if (!daily[cDate]) daily[cDate] = [0, 0, 0];
      daily[cDate][0]++;
      if (!managers[mgr]) managers[mgr] = { leads:0, deals:0, budget:0 };
      managers[mgr].leads++;
    }

    let oDate = null;
    if (orderDateFieldId && lead.custom_fields_values) {
      const field = lead.custom_fields_values.find(f => f.field_id === orderDateFieldId);
      if (field?.values?.[0]?.value) {
        const raw = field.values[0].value;
        if (typeof raw === 'number') oDate = tsToDate(raw);
        else if (typeof raw === 'string' && raw.match(/^\d{2}\.\d{2}\.\d{4}/)) oDate = raw.slice(0,10);
        else if (typeof raw === 'string' && raw.match(/^\d{4}-\d{2}-\d{2}/)) {
          const [y,m,d] = raw.split('-');
          oDate = `${d}.${m}.${y}`;
        }
      }
    }

    if (!oDate && lead.closed_at && budget > 0) {
      oDate = tsToDate(lead.closed_at);
    }

    if (oDate && budget > 0) {
      if (!daily[oDate]) daily[oDate] = [0, 0, 0];
      daily[oDate][1]++;
      daily[oDate][2] += budget;
      if (!managers[mgr]) managers[mgr] = { leads:0, deals:0, budget:0 };
      managers[mgr].deals++;
      managers[mgr].budget += budget;
    }
  }

  const mgrList = Object.entries(managers)
    .map(([name, v]) => ({
      name,
      leads:    v.leads,
      deals:    v.deals,
      budget:   Math.round(v.budget),
      conv:     v.leads ? +(v.deals / v.leads * 100).toFixed(1) : 0,
      avgCheck: v.deals ? Math.round(v.budget / v.deals) : 0,
    }))
    .sort((a, b) => b.budget - a.budget);

  return { daily, mgrList };
}

// ════════════════════════════════════════════════════════════════
// MAIN SYNC
// ════════════════════════════════════════════════════════════════
let isSyncing = false;

async function syncAll() {
  if (isSyncing) { console.log('Already syncing, skip'); return; }
  isSyncing = true;

  console.log('\n════════════════════════════════════════════════');
  console.log(`SYNC START: ${new Date().toISOString()}`);
  console.log('════════════════════════════════════════════════');

  const RAW = {}, MANAGERS = {};
  let totalLeads = 0;

  for (const account of ACCOUNTS) {
    try {
      const [userMap, orderDateFieldId] = await Promise.all([
        fetchUsers(account),
        findOrderDateFieldId(account),
      ]);
      const leads = await fetchAllLeads(account);
      totalLeads += leads.length;
      const { daily, mgrList } = processLeads(leads, userMap, orderDateFieldId);
      RAW[account.name]      = daily;
      MANAGERS[account.name] = mgrList;
      const totalBudget = Object.values(daily).reduce((s, v) => s + v[2], 0);
      const totalDeals  = Object.values(daily).reduce((s, v) => s + v[1], 0);
      console.log(`[${account.name}] ✓ ${leads.length} сделок → ${totalDeals} с бюджетом ${(totalBudget/1e6).toFixed(2)}М ₸`);
    } catch (err) {
      console.error(`[${account.name}] ОШИБКА:`, err.message);
    }
    await sleep(500);
  }

  console.log('\nСохраняем в Supabase...');
  const { error } = await supabase.from('wg_data').upsert({
    id: 1, raw: RAW, managers: MANAGERS,
    updated_at: new Date().toISOString(), updated_by: 'auto-sync',
  }, { onConflict: 'id' });

  if (error) {
    console.error('Supabase error:', error);
  } else {
    console.log(`✓ Данные сохранены в Supabase (${totalLeads} сделок)`);
  }

  console.log(`SYNC END: ${new Date().toISOString()}\n`);
  isSyncing = false;
}

// ════════════════════════════════════════════════════════════════
// ROUTES
// ════════════════════════════════════════════════════════════════
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  next();
});

app.get('/', (req, res) => res.json({
  status: 'WeGlow Sync Server running',
  time: new Date().toISOString(),
  syncing: isSyncing,
  accounts: ACCOUNTS.map(a => ({ name: a.name, domain: a.domain })),
}));

app.get('/sync/trigger', (req, res) => {
  res.json({ message: 'Sync triggered', time: new Date().toISOString() });
  syncAll().catch(console.error);
});

app.get('/status', async (req, res) => {
  try {
    const { data } = await supabase.from('wg_data').select('updated_at').eq('id', 1).single();
    res.json({ last_sync: data?.updated_at, syncing: isSyncing, server_time: new Date().toISOString() });
  } catch (e) { res.json({ error: e.message }); }
});

app.get('/data', async (req, res) => {
  try {
    const { data, error } = await supabase.from('wg_data').select('raw,managers,ad_spend,updated_at').eq('id', 1).single();
    if (error) throw new Error(error.message);
    res.json({ ok: true, ...data });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/debug-fields/:acc', async (req, res) => {
  const map = { col: ACCOUNTS[0], ummi: ACCOUNTS[1], coffee: ACCOUNTS[2] };
  const acc = map[req.params.acc];
  if (!acc) return res.json({ error: 'Use: col, ummi, coffee' });
  try {
    const r = await axios.get(`https://${acc.domain}/api/v4/leads/custom_fields`, {
      headers: { Authorization: `Bearer ${acc.token}` }, params: { limit: 100 },
    });
    res.json({ fields: (r.data?._embedded?.custom_fields || []).map(f => ({ id: f.id, name: f.name, type: f.field_type })) });
  } catch (e) { res.status(500).json({ error: e.message, status: e.response?.status }); }
});

// ════════════════════════════════════════════════════════════════
// CRON — каждый час
// ════════════════════════════════════════════════════════════════
cron.schedule('0 * * * *', () => {
  console.log('⏰ Cron: hourly sync');
  syncAll().catch(console.error);
});

// Keep-alive
setInterval(() => {
  axios.get(`http://localhost:${process.env.PORT || 3000}/status`).catch(() => {});
}, 14 * 60 * 1000);

// ════════════════════════════════════════════════════════════════
// START
// ════════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`\n🚀 WeGlow Sync Server запущен на порту ${PORT}`);
  console.log(`Аккаунты: ${ACCOUNTS.map(a => a.name).join(', ')}`);
  await sleep(2000);
  syncAll().catch(console.error);
});
