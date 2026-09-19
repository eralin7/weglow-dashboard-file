#!/usr/bin/env node
/**
 * WeGlow — сервис ИИ-советника для дашборда (officeweglow.kz).
 *
 * Работает на Windows Server (или любой ОС) под Node 18+, слушает только
 * 127.0.0.1:8787; наружу по HTTPS его публикует Caddy (см. Caddyfile).
 *
 *   POST /advice              { metrics, period } → { ts, sections, text, model, cached }
 *   GET  /advice?selftest=1   проверка установки (ключ не показывается)
 *
 * Конфиг: advice-config.json рядом со скриптом (см. advice-config.example.json)
 * либо переменная окружения DEEPSEEK_API_KEY. Одинаковые метрики отвечаются
 * из кэша 30 минут; лимиты по IP / час / сутки берегут баланс DeepSeek.
 */
'use strict';
const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');

const ROOT = __dirname;
let LOG_FILE = path.join(ROOT, 'advice.log');

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  try { fs.appendFileSync(LOG_FILE, line + '\n'); } catch (e) { /* лог не критичен */ }
}

function readConfig() {
  for (const f of [process.env.ADVICE_CONFIG, path.join(ROOT, 'advice-config.json')]) {
    if (!f || !fs.existsSync(f)) continue;
    try { return JSON.parse(fs.readFileSync(f, 'utf8').replace(/^﻿/, '')); }
    catch (e) { log(`конфиг ${f} не читается: ${e.message}`); }
  }
  return {};
}

const CONFIG = Object.assign({
  deepseekKey:    '',
  model:          'deepseek-chat',
  host:           '127.0.0.1',
  port:           8787,
  allowedOrigins: ['https://officeweglow.kz', 'https://www.officeweglow.kz', 'http://localhost:8765', 'http://127.0.0.1:8765'],
  cacheTtlMin:    30,     // кэш ответа для одинакового набора метрик
  limitIp10min:   12,     // запросов с одного IP за 10 минут
  limitHour:      60,     // всего запросов к DeepSeek за час
  limitDay:       400,    // всего за сутки
  timeoutSec:     55,     // ожидание ответа DeepSeek
  logFile:        LOG_FILE,
}, readConfig());
if (!CONFIG.deepseekKey) CONFIG.deepseekKey = process.env.DEEPSEEK_API_KEY || '';
LOG_FILE = CONFIG.logFile || LOG_FILE;
const MOCK = !!process.env.ADVICE_MOCK;   // только для локальной проверки клиента

// ── Промпт и разбор ответа ────────────────────────────────────────────────
const SYSTEM_PROMPT = 'Ты — ИИ-советник отдела продаж WeGlow (Казахстан, косметика и БАД). '
  + 'Отвечай по-русски, коротко, без вступлений. Каждый пункт — конкретное действие с опорой на цифры из данных.';
const userPrompt = metrics => `ДАННЫЕ ДАШБОРДА:
${metrics}

Дай рекомендации по 5 блокам. Формат строго такой, без другого текста:

[KPI]
- 2-3 рекомендации по ключевым показателям (продажи, конверсия, средний чек, ДРР)

[PLAN]
- 2-3 рекомендации по выполнению плана группами (кто отстаёт, что делать)

[DYNAMICS]
- 2-3 рекомендации по динамике продаж и товарному миксу

[LEADS]
- 2-3 рекомендации по лидам и конверсии (качество лидов, воронка, дожим)

[SUMMARY]
- 2-3 рекомендации по итогам периода (прогноз, риски, приоритеты)`;

function parseSections(raw) {
  const out = {};
  const blocks = String(raw).split(/\[([A-Z]+)\]/g);
  for (let i = 1; i < blocks.length; i += 2) {
    const key = blocks[i].trim(), text = (blocks[i + 1] || '').trim();
    if (key && text) out[key] = text;
  }
  return out;
}

async function askDeepSeek(metrics) {
  if (MOCK) {
    return { text: '', sections: { KPI: '- (проверка) конверсия 12,7%: дожать открытые сделки', PLAN: '- (проверка) группа отстаёт от плана', DYNAMICS: '- (проверка) динамика', LEADS: '- (проверка) лиды', SUMMARY: '- (проверка) итоги' } };
  }
  const r = await fetch('https://api.deepseek.com/chat/completions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${CONFIG.deepseekKey}` },
    body: JSON.stringify({
      model: CONFIG.model, temperature: 0.4, max_tokens: 1200,
      messages: [{ role: 'system', content: SYSTEM_PROMPT }, { role: 'user', content: userPrompt(metrics) }],
    }),
    signal: AbortSignal.timeout(CONFIG.timeoutSec * 1000),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) {
    const detail = j.error && j.error.message ? ': ' + j.error.message : '';
    const human = r.status === 401 ? 'ключ DeepSeek не принят'
      : r.status === 402 ? 'на балансе DeepSeek нет средств'
      : r.status === 429 ? 'DeepSeek ограничил частоту запросов'
      : `DeepSeek ответил ошибкой ${r.status}`;
    throw new Error(human + detail);
  }
  const text = ((((j.choices || [])[0] || {}).message || {}).content || '').trim();
  const sections = parseSections(text);
  if (!Object.keys(sections).length) throw new Error('DeepSeek вернул ответ без разделов');
  return { text, sections };
}

// ── Кэш и лимиты (в памяти процесса) ──────────────────────────────────────
const cache    = new Map();   // hash → { ts, sections, text, model }
const inflight = new Map();   // hash → Promise
const hits     = { ip: new Map(), hour: [], day: [] };

function bump(arr, windowMs) {
  const now = Date.now();
  while (arr.length && arr[0] <= now - windowMs) arr.shift();
  arr.push(now);
  return arr.length;
}
function bumpIp(ip) {
  if (hits.ip.size > 5000) hits.ip.clear();
  let a = hits.ip.get(ip);
  if (!a) { a = []; hits.ip.set(ip, a); }
  return bump(a, 10 * 60 * 1000);
}

function getAdvice(metrics) {
  const hash = crypto.createHash('sha1').update(metrics + '|' + CONFIG.model).digest('hex');
  const hit = cache.get(hash);
  if (hit && Date.now() - hit.ts < CONFIG.cacheTtlMin * 60 * 1000) return Promise.resolve({ ...hit, cached: true });
  if (inflight.has(hash)) return inflight.get(hash);
  const p = askDeepSeek(metrics).then(({ text, sections }) => {
    const out = { ts: Date.now(), sections, text, model: MOCK ? 'mock' : CONFIG.model, cached: false };
    cache.set(hash, out);
    if (cache.size > 300) cache.delete(cache.keys().next().value);
    return out;
  }).finally(() => inflight.delete(hash));
  inflight.set(hash, p);
  return p;
}

// ── HTTP ──────────────────────────────────────────────────────────────────
const allowed = o => !!o && CONFIG.allowedOrigins.includes(o);
const corsHeaders = origin => ({
  'Access-Control-Allow-Origin': origin, 'Vary': 'Origin',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Max-Age': '86400',
});
function send(res, status, data, corsOrigin) {
  const body = JSON.stringify(data);
  const headers = { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Length': Buffer.byteLength(body) };
  if (corsOrigin) Object.assign(headers, corsHeaders(corsOrigin));
  res.writeHead(status, headers);
  res.end(body);
}
const startedAt = Date.now();

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://local');
  const origin = req.headers.origin || '';
  const corsOrigin = allowed(origin) ? origin : '';

  if (req.method === 'OPTIONS') { res.writeHead(204, corsOrigin ? corsHeaders(corsOrigin) : {}); res.end(); return; }
  if (url.pathname !== '/advice' && url.pathname !== '/advice.php') { send(res, 404, { error: 'не найдено' }, corsOrigin); return; }

  if (req.method === 'GET') {
    if (url.searchParams.has('selftest')) {
      send(res, 200, {
        ok: !!CONFIG.deepseekKey || MOCK, node: process.version, key_configured: !!CONFIG.deepseekKey, mock: MOCK,
        model: CONFIG.model, allowed_origins: CONFIG.allowedOrigins,
        limits: { per_ip_10min: CONFIG.limitIp10min, per_hour: CONFIG.limitHour, per_day: CONFIG.limitDay },
        cache_entries: cache.size, uptime_min: Math.round((Date.now() - startedAt) / 60000),
      }, corsOrigin);
      return;
    }
    send(res, 405, { error: 'нужен POST с JSON {metrics}' }, corsOrigin); return;
  }
  if (req.method !== 'POST') { send(res, 405, { error: 'нужен POST с JSON {metrics}' }, corsOrigin); return; }

  let refOrigin = '';
  try { refOrigin = new URL(req.headers.referer || '').origin; } catch (e) { /* нет referer */ }
  if (!corsOrigin && !allowed(refOrigin)) { send(res, 403, { error: 'запросы принимаются только с дашборда' }); return; }
  if (!CONFIG.deepseekKey && !MOCK) { send(res, 503, { error: 'на сервере не задан ключ DeepSeek (advice-config.json)' }, corsOrigin); return; }

  let body = '', done = false;
  req.on('data', c => {
    body += c;
    if (body.length > 200000 && !done) { done = true; send(res, 413, { error: 'слишком большой запрос' }, corsOrigin); req.destroy(); }
  });
  req.on('end', async () => {
    if (done) return;
    done = true;
    let metrics = '';
    try { const j = JSON.parse(body || '{}'); if (typeof j.metrics === 'string') metrics = j.metrics.trim().slice(0, 12000); } catch (e) { /* не JSON */ }
    if (!metrics) { send(res, 400, { error: 'нужно поле metrics' }, corsOrigin); return; }

    // Кэш-попадания не считаются в лимиты
    const hash = crypto.createHash('sha1').update(metrics + '|' + CONFIG.model).digest('hex');
    const hit = cache.get(hash);
    if (!(hit && Date.now() - hit.ts < CONFIG.cacheTtlMin * 60 * 1000) && !inflight.has(hash)) {
      const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '?';
      if (bumpIp(ip) > CONFIG.limitIp10min) { send(res, 429, { error: 'слишком много запросов, попробуйте через несколько минут' }, corsOrigin); return; }
      if (bump(hits.hour, 3600 * 1000) > CONFIG.limitHour) { send(res, 429, { error: 'часовой лимит советника исчерпан' }, corsOrigin); return; }
      if (bump(hits.day, 86400 * 1000) > CONFIG.limitDay) { send(res, 429, { error: 'суточный лимит советника исчерпан' }, corsOrigin); return; }
    }
    try {
      const out = await getAdvice(metrics);
      send(res, 200, out, corsOrigin);
      log(`advice ok (${out.cached ? 'кэш' : 'DeepSeek'}, ${metrics.length} симв.)`);
    } catch (e) {
      log(`advice error: ${e.message}`);
      send(res, 502, { error: e.name === 'TimeoutError' ? 'DeepSeek не ответил вовремя' : e.message }, corsOrigin);
    }
  });
});

process.on('unhandledRejection', e => log(`unhandledRejection: ${e && e.message}`));
process.on('uncaughtException', e => log(`uncaughtException: ${e && e.message}`));

server.listen(CONFIG.port, CONFIG.host, () => {
  log(`советник слушает http://${CONFIG.host}:${CONFIG.port}/advice · модель ${CONFIG.model} · ключ ${CONFIG.deepseekKey ? 'задан' : 'НЕ ЗАДАН'}${MOCK ? ' · режим проверки (mock)' : ''}`);
});
