/* WeGlow — таблица «На увольнение»: общий блок для dashboard.html и rop.html.
 *
 * Сам список считает и хранит база Era CRM (ТЗ от 23.11.2026; миграция era-crm
 * 2026-11-24-dashboard-dismissal.sql): правила, фиксация статусов 1-го числа,
 * история, отметки РОПа, настройки. Наружу его отдаёт только RPC
 * get_dashboard_dismissal — директору и админу все отделы, РОПу свой, остальным
 * ничего. Здесь нет ни правил, ни цифр: только показ, фильтры, отметка
 * «Исключение» и форма настроек. Поэтому без входа в Era CRM блок пуст.
 *
 *   const w = WGDismiss.create({
 *     el,                      // куда рисовать
 *     url, anonKey,            // Supabase Era CRM
 *     getToken: () => '...',   // токен входа Era CRM (или null)
 *     login: (email, pass) => Promise,   // необязательно: форма входа прямо в блоке
 *     onDenied: token => {},   // CRM не приняла токен (устарел)
 *     group: 'Айша',           // необязательно: отдел, выбранный по умолчанию (если видны все)
 *   });
 *   w.refresh();   // показать / обновить (в сеть — не чаще раза в минуту)
 *   w.clear();     // выход: стереть список со страницы и из памяти
 *   w.setGroup(g); // сменить отдел по умолчанию
 *
 * Блок сам владеет содержимым el: снаружи его не трогать (стирать — через clear()).
 */
(function () {
  'use strict';

  var MONTHS = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];
  var MONTHS_GEN = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'];
  var STATUS = { fire: 'Увольнение', review: 'Разбор РОПом', warn: 'Предупреждение', ok: 'Норма', exception: 'Исключение' };
  var ORDER = { fire: 0, review: 1, warn: 2 };
  var REASON = {
    low_sales: 'Мало продаж',
    not_fixed: 'Не исправился после разбора',
    low_conv_2: 'Низкая конверсия 2 мес.',
    good_conv: 'Хорошая конверсия, мало продаж',
    few_leads: 'Мало лидов',
    low_conv_1: 'Низкая конверсия 1 мес.'
  };
  var SET_KEYS = ['min_sales', 'min_sales_senior', 'tenure_from', 'tenure_senior', 'conv_share', 'min_leads'];
  var REFETCH_MS = 60000;

  var CSS = [
    '.ds-root{--ds-blue:#1f56a3;--ds-blue-l:#e6effb;color:var(--ink,#1d1d1f)}',
    'html[data-theme="glow"] .ds-root,html[data-theme="forest"] .ds-root{--ds-blue:#9cc3ff;--ds-blue-l:rgba(140,180,255,.16)}',
    '.ds-hdr{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:flex-end;gap:14px 24px;margin-bottom:20px}',
    '.ds-title{font-family:var(--font-display,inherit);font-size:28px;font-weight:600;letter-spacing:-.3px;margin:0}',
    '.ds-subt{font-size:13px;color:var(--muted,#6e6e73);margin:4px 0 0;max-width:780px;line-height:1.45}',
    '.ds-filters{display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end}',
    '.ds-filters label{display:flex;flex-direction:column;gap:5px;font-size:11px;font-weight:600;color:var(--muted,#6e6e73)}',
    '.ds-sel,.ds-inp{min-height:38px;padding:0 12px;border:1.5px solid var(--line,#e5e5ea);border-radius:10px;background:var(--menu-bg,var(--white,#fff));color:var(--ink,#1d1d1f);font-family:inherit;font-size:13px}',
    '.ds-sel{cursor:pointer}',
    '.ds-btn{min-height:38px;padding:0 14px;border:1.5px solid var(--line,#e5e5ea);border-radius:10px;background:var(--menu-bg,var(--white,#fff));color:var(--ink,#1d1d1f);font-family:inherit;font-size:13px;font-weight:600;cursor:pointer}',
    '.ds-btn:hover{border-color:var(--acc,#0071e3)}',
    '.ds-btn.sm{min-height:28px;padding:0 10px;font-size:12px;border-radius:8px;font-weight:500}',
    '.ds-btn.primary{background:var(--ink,#1d1d1f);border-color:var(--ink,#1d1d1f);color:var(--white,#fff)}',
    'html[data-theme] .ds-btn.primary{background:var(--pill-bg,var(--ink));border-color:var(--pill-bg,var(--ink));color:var(--pill-fg,#fff)}',
    '.ds-btn[disabled]{opacity:.55;cursor:default}',
    '.ds-sel:focus-visible,.ds-inp:focus-visible,.ds-btn:focus-visible,.ds-tab:focus-visible,.ds-root textarea:focus-visible,.ds-root a:focus-visible{outline:2px solid var(--acc,#0071e3);outline-offset:2px}',
    '.ds-tabs{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px}',
    '.ds-tab{cursor:pointer;text-align:left;padding:14px 16px;border-radius:14px;background:var(--white,#fff);border:2px solid var(--line,#e5e5ea);display:flex;flex-direction:column;gap:4px;font-family:inherit;color:var(--ink,#1d1d1f)}',
    '.ds-tab.active{border-color:var(--acc,#0071e3)}',
    '.ds-tab .k{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted,#6e6e73)}',
    '.ds-tab .n{font-size:26px;font-weight:800;letter-spacing:-.02em;font-variant-numeric:tabular-nums;line-height:1.1}',
    '.ds-tab.fire .n{color:var(--red,#d70015)}.ds-tab.review .n{color:var(--ds-blue)}.ds-tab.warn .n{color:var(--gold,#b25000)}.ds-tab.exc .n{color:var(--muted,#6e6e73)}',
    '.ds-card{background:var(--white,#fff);border:1px solid var(--line,#e5e5ea);border-radius:var(--r,18px);box-shadow:var(--shadow,none);overflow:hidden;margin-bottom:18px;transition:opacity .15s}',
    '.ds-root.busy .ds-card,.ds-root.busy .ds-tabs{opacity:.5}',
    '.ds-scroll{overflow-x:auto;-webkit-overflow-scrolling:touch}',
    '.ds-grid{min-width:1126px}.ds-grid.nofc{min-width:1026px}',
    '.ds-row{display:grid;grid-template-columns:minmax(150px,1.3fr) 90px 160px 52px 84px 110px 90px minmax(150px,1.4fr) 128px;gap:10px;padding:12px 16px;align-items:center;border-bottom:1px solid var(--line2,#f2f2f7);font-size:13px;font-variant-numeric:tabular-nums}',
    '.ds-grid.nofc .ds-row{grid-template-columns:minmax(150px,1.3fr) 90px 160px 52px 84px 110px minmax(150px,1.4fr) 128px}',
    '.ds-grid.nofc .ds-c-fc{display:none}',
    '.ds-row:last-child{border-bottom:0}',
    '.ds-row.ds-head{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted,#6e6e73);border-bottom:1px solid var(--line,#e5e5ea);padding-top:11px;padding-bottom:11px}',
    '.ds-row:not(.ds-head):hover{background:var(--bg,#f5f5f7)}',
    '.ds-row.ds-gone>.ds-c:not(.ds-c-note):not(.ds-c-name),.ds-row.ds-gone>.ds-c-name>*{opacity:.55}',
    '.ds-c{min-width:0}',
    '.ds-sub{font-size:11px;color:var(--muted,#6e6e73);font-weight:500;line-height:1.3}',
    '.ds-c-name{display:flex;flex-direction:column;justify-content:center;gap:2px;align-self:stretch}',
    '@media (min-width:761px) and (max-width:1420px){.ds-c-name{position:sticky;left:0;z-index:1;background:var(--menu-bg,var(--white,#fff));box-shadow:1px 0 0 var(--line2,#f2f2f7)}.ds-row:not(.ds-head):hover .ds-c-name{background:linear-gradient(var(--bg,#f5f5f7),var(--bg,#f5f5f7)),var(--menu-bg,var(--white,#fff))}}',
    '.ds-c-name b{font-weight:600;overflow-wrap:anywhere}',
    '.ds-c-ten,.ds-c-fc{display:flex;flex-direction:column;gap:2px}',
    '.ds-c-sales{display:flex;flex-direction:column;gap:5px}',
    '.ds-sl{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:baseline;gap:2px 6px}.ds-sl b,.ds-sl .ds-sub{white-space:nowrap}',
    '.ds-bar{display:block;height:6px;background:var(--line,#e5e5ea);border-radius:3px;overflow:hidden}',
    '.ds-bar i{display:block;height:100%;border-radius:3px;background:var(--green,#1f8a3b)}.ds-bar i.lo{background:var(--red,#d70015)}',
    '.ds-c-num{text-align:right}',
    '.ds-conv{font-weight:600}.ds-conv.lo{color:var(--red,#d70015)}.ds-conv.mid{color:var(--gold,#b25000)}.ds-conv.ok{color:var(--green,#1f8a3b)}.ds-conv.na{color:var(--muted,#6e6e73);font-weight:400;font-size:12px}',
    '.ds-c-fc b.lo{color:var(--red,#d70015)}',
    '.ds-c-why{display:flex;flex-direction:column;gap:3px;line-height:1.3}.ds-c-why span+span{font-size:12px;color:var(--muted,#6e6e73)}',
    '.ds-c-st{display:flex;flex-direction:column;align-items:flex-start;gap:6px}',
    '.ds-pill{display:inline-block;padding:4px 10px;border-radius:999px;font-weight:700;font-size:12px;white-space:nowrap}',
    '.ds-pill.fire{background:var(--red-l,#ffe9ea);color:var(--red,#d70015)}.ds-pill.review{background:var(--ds-blue-l);color:var(--ds-blue)}.ds-pill.warn{background:var(--gold-l,#fff1e0);color:var(--gold,#b25000)}.ds-pill.exc,.ds-pill.none{background:var(--line2,#f2f2f7);color:var(--muted,#6e6e73)}',
    '.ds-c-note{grid-column:1/-1;display:flex;flex-direction:column;gap:8px}',
    '.ds-cmt{font-size:12.5px;line-height:1.4;padding:8px 12px;border-radius:10px;background:var(--bg,#f5f5f7);white-space:pre-wrap;overflow-wrap:anywhere}',
    '.ds-row:not(.ds-head):hover .ds-cmt{background:var(--white,#fff)}',
    '.ds-cmt .who{color:var(--muted,#6e6e73);font-size:11px;white-space:nowrap}',
    '.ds-edit{display:flex;flex-direction:column;gap:8px;padding:12px;border:1.5px solid var(--line,#e5e5ea);border-radius:12px;background:var(--menu-bg,var(--white,#fff))}',
    '.ds-edit textarea{width:100%;min-height:58px;resize:vertical;padding:8px 10px;border:1.5px solid var(--line,#e5e5ea);border-radius:8px;font-family:inherit;font-size:13px;background:var(--white,#fff);color:var(--ink,#1d1d1f);box-sizing:border-box}',
    '.ds-chk{display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer}',
    '.ds-row-actions{display:flex;flex-wrap:wrap;align-items:center;gap:8px}',
    '.ds-err{color:var(--red,#d70015);font-size:12.5px}',
    '.ds-empty{padding:26px 18px;font-size:13px;color:var(--muted,#6e6e73);line-height:1.5}',
    '.ds-note{font-size:12px;color:var(--muted,#6e6e73);line-height:1.5;margin:0 2px 10px;max-width:980px}',
    '.ds-note.warn{color:var(--gold,#b25000)}',
    '.ds-note a,.ds-empty a{color:var(--acc,#0071e3);cursor:pointer;text-decoration:underline}',
    '.ds-settings{padding:16px 18px;margin-bottom:16px;border:1.5px solid var(--line,#e5e5ea);border-radius:14px;background:var(--white,#fff)}',
    '.ds-settings h3{font-size:14px;font-weight:700;margin:0 0 4px}',
    '.ds-set-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:12px 0;align-items:end}',
    '.ds-set-grid label{display:flex;flex-direction:column;gap:5px;font-size:12px;color:var(--muted,#6e6e73)}',
    '.ds-login{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;margin-top:12px}',
    '.ds-login label{display:flex;flex-direction:column;gap:5px;font-size:11px;font-weight:600;color:var(--muted,#6e6e73)}',
    '@media (max-width:768px){.ds-hdr{align-items:stretch}.ds-title{font-size:20px}}',
    '@media (max-width:760px){',
    '.ds-grid,.ds-grid.nofc{min-width:0}.ds-row.ds-head{display:none}',
    '.ds-row,.ds-grid.nofc .ds-row{grid-template-columns:1fr 1fr;gap:10px 14px;padding:14px 12px;align-items:start}',
    '.ds-c-name{grid-column:1;grid-row:1}.ds-c-st{grid-column:2;grid-row:1;align-items:flex-end}',
    '.ds-c-why{grid-column:1/-1;grid-row:2}.ds-c-sales{grid-column:1/-1;grid-row:3}.ds-c-num{text-align:left}',
    '.ds-c-why:empty{display:none}',
    '.ds-c[data-l]::before{content:attr(data-l);display:block;font-size:9.5px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--muted,#6e6e73);margin-bottom:3px}',
    '.ds-filters{width:100%}.ds-filters label{flex:1 1 140px}.ds-tab{padding:11px 12px}.ds-tab .n{font-size:22px}',
    '.ds-login label{flex:1 1 100%}.ds-login .ds-inp{width:100%;box-sizing:border-box}',
    '}'
  ].join('\n');

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function num(n) { return Math.round(Number(n) || 0).toLocaleString('ru-RU'); }
  // «2,48 млн» / «840 тыс» — всегда вниз, чтобы недобор не выглядел выполнением
  function money(v) {
    v = Math.max(0, Number(v) || 0);
    if (v >= 1e6) return String(Math.floor(v / 1e4) / 100).replace('.', ',') + ' млн';
    if (v >= 1000) return num(Math.floor(v / 1000)) + ' тыс';
    return num(v) + ' ₸';
  }
  function pctN(v, n) { return Number(v).toFixed(n).replace('.', ',') + '%'; }
  function p2(n) { return String(n).padStart(2, '0'); }
  function dmy(iso) { var a = String(iso || '').slice(0, 10).split('-'); return a.length === 3 ? a[2] + '.' + a[1] + '.' + a[0] : ''; }
  function monthIdx(iso) { var m = +String(iso || '').split('-')[1]; return m >= 1 && m <= 12 ? m - 1 : 0; }
  function monthName(iso) { return MONTHS[monthIdx(iso)] + ' ' + String(iso || '').slice(0, 4); }
  // момент времени → дата и время по Алматы (UTC+5 круглый год), где бы ни открыли страницу
  function almaty(ts) {
    var t = new Date(ts).getTime();
    if (isNaN(t)) return null;
    var d = new Date(t + 5 * 3600000);
    return { date: p2(d.getUTCDate()) + '.' + p2(d.getUTCMonth() + 1) + '.' + d.getUTCFullYear(), time: p2(d.getUTCHours()) + ':' + p2(d.getUTCMinutes()) };
  }
  function setVal(v) { return Number(v).toLocaleString('ru-RU', { maximumFractionDigits: 4 }); }

  function create(opts) {
    var el = opts.el;
    var st = {
      month: null, rop: opts.group || 'all', status: 'all', showGone: false,
      data: null, state: 'idle', fetchedAt: 0, fetchedMonth: undefined, token: null, denied: null,
      seq: 0, busy: false, stale: false, html: null,
      edit: null, draft: null, editErr: '', saving: false,
      settings: false, setDraft: null, setErr: '',
      loginErr: '', loginBusy: false, loginEmail: ''
    };
    if (!document.getElementById('wg-ds-css')) {
      var tag = document.createElement('style');
      tag.id = 'wg-ds-css';
      tag.textContent = CSS;
      document.head.appendChild(tag);
    }

    function token() {
      var t = null;
      try { t = opts.getToken ? opts.getToken() : null; } catch (e) { t = null; }
      return t && t !== st.denied ? t : null;
    }
    function rpc(name, body, tok) {
      return fetch(opts.url + '/rest/v1/rpc/' + name, {
        method: 'POST',
        headers: { 'apikey': opts.anonKey, 'Authorization': 'Bearer ' + tok, 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
    }
    function denied(tok) {
      st.denied = tok;
      try { if (opts.onDenied) opts.onDenied(tok); } catch (e) {}
    }
    function forget() {          // список чужого входа со страницы и из памяти
      st.data = null; st.edit = null; st.draft = null; st.settings = false; st.setDraft = null;
      st.busy = false; st.stale = false; st.saving = false; st.editErr = ''; st.setErr = '';
    }
    // Пишем в страницу только когда разметка изменилась: не сбрасываем фокус,
    // открытый список и прокрутку при фоновых обновлениях.
    function paint(html) {
      if (html === st.html) return;
      st.html = html;
      el.innerHTML = html;
    }

    function load(force) {
      var tok = token();
      if (!tok) { forget(); st.state = 'auth'; st.token = null; render(); return; }
      var fresh = st.state === 'ok' && st.token === tok && st.fetchedMonth === st.month && Date.now() - st.fetchedAt < REFETCH_MS;
      if (fresh && !force) { render(); return; }
      if (st.token !== tok) forget();                    // другой вход — прежний список не его
      var seq = ++st.seq, month = st.month;
      st.token = tok;
      if (!st.data) st.state = 'loading';
      else st.busy = !!force || month !== st.data.month;
      render();
      rpc('get_dashboard_dismissal', { p_month: month }, tok).then(function (r) {
        if (r.status === 401 || r.status === 403) { var e = new Error('auth'); e.auth = true; throw e; }
        if (r.status === 404) { var m = new Error('missing'); m.missing = true; throw m; }
        if (!r.ok) throw new Error('http ' + r.status);
        return r.json();
      }).then(function (d) {
        if (seq !== st.seq) return;
        if (d === null) { forget(); st.state = 'noaccess'; render(); return; }
        if (!d || !Array.isArray(d.rows)) throw new Error('shape');
        st.data = d; st.state = 'ok'; st.busy = false; st.stale = false; st.loginErr = '';
        st.fetchedAt = Date.now();
        st.month = d.month || null;
        st.fetchedMonth = st.month;
        render();
      }).catch(function (e) {
        if (seq !== st.seq) return;
        if (e && e.auth) { denied(tok); forget(); st.state = 'auth'; st.token = null; }
        else if (e && e.missing) { forget(); st.state = 'missing'; }
        else if (!st.data) { st.state = 'error'; }
        else {                                           // обновление не прошло — оставляем прежний список
          st.busy = false; st.stale = true;
          st.month = st.data.month || null;
          st.html = null;                                // вернуть выпадающий список месяца на место
        }
        render();
      });
    }

    // ── строки ─────────────────────────────────────────────────────────────
    function isExc(x) { return !!(x.note && x.note.exception); }
    function pct(x) { return x.assessed && x.min > 0 ? x.sales / x.min : (x.fc && x.fc.min > 0 ? x.fc.forecast / x.fc.min : 0); }
    function sorted(rows) {
      return rows.slice().sort(function (a, b) {
        var oa = ORDER[a.status], ob = ORDER[b.status];
        oa = oa == null ? 9 : oa; ob = ob == null ? 9 : ob;
        return (oa - ob) || (pct(a) - pct(b)) || String(a.name).localeCompare(String(b.name), 'ru');
      });
    }
    function tenureTxt(x, d) {
      if (x.tenure == null || x.tenure < 0) return '—';
      if (x.tenure_floor) return num(d.settings.tenure_senior) + '+ мес';
      var t = Math.floor(Number(x.tenure) * 10 + 1e-9) / 10;
      return (t % 1 ? t.toFixed(1).replace('.', ',') : String(t)) + ' мес';
    }
    function reasonTxt(code, x, d) {
      if (code === 'forecast') {
        var m = d.forecast ? MONTHS_GEN[monthIdx(d.forecast.month)] : '';
        return 'Прогноз ' + m + ' ' + money(x.fc ? x.fc.forecast : 0) + ' — ниже минимума';
      }
      return REASON[code] || code;
    }
    function convHTML(x, d) {
      var c = x.conv == null ? null : Number(x.conv);
      if (!x.assessed) return '<span class="ds-conv na">' + (c == null ? '—' : pctN(c, 1)) + '</span>';
      if (!x.conv_known) {
        return '<span class="ds-conv na" title="' + (c == null ? 'лидов не было' : 'по факту ' + pctN(c, 1)) + '">&lt; '
          + num(d.settings.min_leads) + ' лидов</span>';
      }
      var avg = Number(d.avg_conv), min = Number(d.min_conv);
      var one = pctN(c, 1);
      var clash = (one === pctN(avg, 1) && Math.abs(c - avg) > 1e-9) || (one === pctN(min, 1) && Math.abs(c - min) > 1e-9);
      var cls = x.low_conv ? 'lo' : c < avg - 1e-9 ? 'mid' : 'ok';
      return '<span class="ds-conv ' + cls + '" title="' + pctN(c, 2) + ' · средняя ' + pctN(avg, 2) + ', минимум ' + pctN(min, 2) + '">'
        + (clash ? pctN(c, 2) : one) + '</span>';
    }
    function rowHTML(x, d, fcHead) {
      var gone = x.gone, exc = isExc(x), can = !!d.role;
      var sub = esc(x.group || 'без отдела') + (x.group_then ? ' · тогда: ' + esc(x.group_then) : '')
        + (x.fired ? ' · уволен' + (x.fired_on ? ' ' + dmy(x.fired_on).slice(0, 5) : '') : gone ? ' · неактивен' : '');
      var startTxt = x.start ? 'с ' + dmy(x.start)
        + (x.start_kind === 'lead' ? ' · по первому лиду' : x.start_kind === 'acct' ? ' · по дате учётки' : '')
        + (x.vet ? ' · раньше данных нет' : '') : '';
      var sales;
      if (x.assessed && x.min > 0) {
        var p = Math.min(999, Math.floor(x.sales * 100 / x.min));
        sales = '<span class="ds-sl"><b title="' + num(x.sales) + ' ₸">' + money(x.sales) + '</b><span class="ds-sub">из ' + money(x.min) + ' · ' + p + '%</span></span>'
          + '<span class="ds-bar"><i class="' + (x.low_sales ? 'lo' : '') + '" style="width:' + Math.min(100, p) + '%"></i></span>';
      } else {
        sales = '<span class="ds-sl"><b title="' + num(x.sales) + ' ₸">' + money(x.sales) + '</b></span><span class="ds-sub">'
          + (x.has_row ? 'первый месяц — не оцениваем' : 'за месяц ни лидов, ни продаж') + '</span>';
      }
      var fc = '';
      if (d.forecast) {
        fc = !d.forecast.on ? '<span class="ds-sub">с ' + esc(d.forecast.from_day) + '-го числа</span>'
          : !x.fc ? '<span class="ds-sub">первый месяц</span>'
          : '<b class="' + (x.fc.low ? 'lo' : '') + '" title="' + num(x.fc.forecast) + ' ₸ · касса с 1-го числа ' + num(x.fc.fact) + ' ₸">'
            + money(x.fc.forecast) + '</b><span class="ds-sub">из ' + money(x.fc.min) + '</span>';
      }
      var pill = exc ? '<span class="ds-pill exc">Исключение</span>' + (STATUS[x.status] ? '<span class="ds-sub">было бы: ' + STATUS[x.status] + '</span>' : '')
        : ORDER[x.status] != null ? '<span class="ds-pill ' + x.status + '">' + STATUS[x.status] + '</span>'
        : '<span class="ds-pill none">' + (x.assessed ? 'Норма' : 'Не оценивался') + '</span>';
      var note = '';
      if (x.note && (x.note.comment || x.note.exception)) {
        var at = x.note.at ? almaty(x.note.at) : null;
        note += '<div class="ds-cmt">' + esc(x.note.comment)
          + ' <span class="who">— ' + esc(x.note.by || 'Era CRM') + (at ? ', ' + at.date.slice(0, 5) : '') + '</span></div>';
      }
      if (st.edit === x.id) {
        var n = st.draft || { comment: (x.note && x.note.comment) || '', exception: !!(x.note && x.note.exception) };
        note += '<div class="ds-edit">'
          + '<textarea maxlength="1000" data-ds="cmt" aria-label="Комментарий" placeholder="Комментарий: что решили или почему исключение (отпуск, больничный, неполный месяц)">' + esc(n.comment) + '</textarea>'
          + '<label class="ds-chk"><input type="checkbox" data-ds="exc"' + (n.exception ? ' checked' : '') + '> Исключение — не учитывать этот месяц (нужен комментарий)</label>'
          + '<div class="ds-row-actions"><button type="button" class="ds-btn sm primary" data-act="save"' + (st.saving ? ' disabled' : '') + '>Сохранить</button>'
          + '<button type="button" class="ds-btn sm" data-act="cancel">Отмена</button>'
          + (st.editErr ? '<span class="ds-err" role="alert">' + esc(st.editErr) + '</span>' : '') + '</div></div>';
      }
      return '<div class="ds-row' + (gone ? ' ds-gone' : '') + '" role="row" data-id="' + esc(x.id) + '">'
        + '<div class="ds-c ds-c-name" role="cell"><b>' + esc(x.name) + '</b><span class="ds-sub">' + sub + '</span></div>'
        + '<div class="ds-c ds-c-ten" role="cell" data-l="Стаж"><span>' + tenureTxt(x, d) + '</span><span class="ds-sub">' + startTxt + '</span></div>'
        + '<div class="ds-c ds-c-sales" role="cell" data-l="Продажи / минимум">' + sales + '</div>'
        + '<div class="ds-c ds-c-num" role="cell" data-l="Перв. лиды">' + num(x.leads) + '</div>'
        + '<div class="ds-c ds-c-num" role="cell" data-l="Конв. перв.">' + convHTML(x, d) + '</div>'
        + '<div class="ds-c ds-c-prev" role="cell" data-l="Прошлый месяц">' + (STATUS[x.prev] || '—') + '</div>'
        + '<div class="ds-c ds-c-fc" role="cell" data-l="' + esc(fcHead) + '">' + fc + '</div>'
        + '<div class="ds-c ds-c-why" role="cell" data-l="Причина">' + (x.reasons || []).map(function (r) { return '<span>' + esc(reasonTxt(r, x, d)) + '</span>'; }).join('') + '</div>'
        + '<div class="ds-c ds-c-st" role="cell">' + pill
        + (can && st.edit !== x.id ? '<button type="button" class="ds-btn sm" data-act="edit">' + (x.note ? 'Изменить отметку' : 'Исключение') + '</button>' : '') + '</div>'
        + (note ? '<div class="ds-c ds-c-note" role="cell">' + note + '</div>' : '')
        + '</div>';
    }

    // ── экран ──────────────────────────────────────────────────────────────
    function shell(inner) {
      paint('<div class="ds-root"><div class="ds-hdr"><div><h2 class="ds-title">На увольнение</h2></div></div>'
        + '<div class="ds-card"><div class="ds-empty">' + inner + '</div></div></div>');
    }
    function render() {
      if (st.state === 'idle') { paint(''); return; }
      if (st.state === 'loading') { shell('Загружаем список из Era CRM…'); return; }
      if (st.state === 'auth') {
        var why = 'Список хранится в Era CRM и открывается только директору, админу и РОПу своего отдела.';
        if (!opts.login) { shell(why + ' Вход устарел — войдите заново.'); return; }
        shell(why + ' Войдите учёткой Era CRM.'
          + '<form class="ds-login" data-ds="login"><label>Почта Era CRM<input class="ds-inp" type="email" data-ds="email" autocomplete="username" required value="' + esc(st.loginEmail) + '"></label>'
          + '<label>Пароль<input class="ds-inp" type="password" data-ds="pass" autocomplete="current-password" required></label>'
          + '<button type="submit" class="ds-btn primary"' + (st.loginBusy ? ' disabled' : '') + '>' + (st.loginBusy ? 'Входим…' : 'Войти') + '</button>'
          + (st.loginErr ? '<span class="ds-err" role="alert">' + esc(st.loginErr) + '</span>' : '') + '</form>');
        return;
      }
      if (st.state === 'noaccess') {
        shell('У этой учётки Era CRM нет доступа к списку: его видят директор, админ и РОП своего отдела.'
          + (opts.login ? ' <a data-act="relogin" role="button" tabindex="0">Войти другой учёткой</a>' : ''));
        return;
      }
      if (st.state === 'missing') { shell('Список появится после обновления Era CRM. <a data-act="retry" role="button" tabindex="0">Проверить ещё раз</a>'); return; }
      if (st.state === 'error' || !st.data) { shell('Не удалось загрузить список. <a data-act="retry" role="button" tabindex="0">Повторить</a>'); return; }

      var d = st.data, S = d.settings || {};
      var months = d.months || [];
      if (!d.month) { shell('В Era CRM пока нет ни одного оценённого месяца.'); return; }
      var all = d.rows || [];
      var groups = [];
      all.forEach(function (x) { var g = x.group || ''; if (g && groups.indexOf(g) < 0) groups.push(g); });
      groups.sort(function (a, b) { return a.localeCompare(b, 'ru'); });
      // выбранный отдел помним, даже если в этом месяце в нём никого нет
      var rop = d.all_groups && st.rop !== 'all' && groups.indexOf(st.rop) >= 0 ? st.rop : 'all';
      var inRop = rop === 'all' ? all : all.filter(function (x) { return (x.group || '') === rop; });
      var excRows = inRop.filter(isExc);
      var live = inRop.filter(function (x) { return !isExc(x); });
      var goneN = live.filter(function (x) { return x.gone; }).length;
      var list = st.showGone ? live : live.filter(function (x) { return !x.gone; });
      if (['all', 'fire', 'review', 'warn', 'exc'].indexOf(st.status) < 0) st.status = 'all';
      var rows = st.status === 'exc' ? excRows : st.status === 'all' ? list : list.filter(function (x) { return x.status === st.status; });
      rows = sorted(rows);
      var cnt = function (s) { return list.filter(function (x) { return x.status === s; }).length; };
      var flaggedN = cnt('fire') + cnt('review') + cnt('warn');
      var plainN = list.length - flaggedN;                // отметка РОПа есть, статуса нет

      var fcGen = d.forecast ? MONTHS_GEN[monthIdx(d.forecast.month)] : '';
      var fcHead = 'Прогноз ' + fcGen;
      var avgTxt = d.avg_conv == null ? '—' : pctN(d.avg_conv, 1), minTxt = d.min_conv == null ? '—' : pctN(d.min_conv, 1);
      var frozen = d.frozen_at ? almaty(d.frozen_at) : null;
      var nm = monthIdx(d.month) === 11 ? 1 : monthIdx(d.month) + 2;
      var sub = 'Оценка за ' + esc(monthName(d.month).toLowerCase()) + ' · '
        + (frozen ? 'статусы зафиксированы ' + frozen.date + ' в ' + frozen.time
                  : 'предварительно: статусы зафиксируются 01.' + p2(nm) + ' в ' + esc(d.freeze_after || '23:30') + ' по Алматы')
        + '. Средняя конверсия в первичную продажу по РОПам ' + avgTxt + ' → минимум ' + minTxt + '.'
        + (!d.forecast ? '' : d.forecast.on ? ' Прогноз ' + fcGen + ' — на ' + dmy(d.today).slice(0, 5) + '.'
          : ' Прогноз ' + fcGen + ' появится с ' + esc(d.forecast.from_day) + '-го числа.');

      var tabs = [['all', 'Всего в списке', '', list.length],
        ['fire', STATUS.fire, 'fire', cnt('fire')],
        ['review', STATUS.review, 'review', cnt('review')],
        ['warn', STATUS.warn, 'warn', cnt('warn')],
        ['exc', 'Исключения', 'exc', excRows.length]];

      var head = '<div class="ds-row ds-head" role="row"><span role="columnheader" class="ds-c-name">Менеджер</span><span role="columnheader">Стаж</span>'
        + '<span role="columnheader">Продажи / минимум</span>'
        + '<span role="columnheader" class="ds-c-num" title="Ещё не покупали: как «Перв. лиды» в таблице плана по группам">Перв. лиды</span>'
        + '<span role="columnheader" class="ds-c-num" title="Первичные продажи ÷ первичные лиды">Конв. перв.</span>'
        + '<span role="columnheader">Прошлый месяц</span>'
        + '<span role="columnheader" class="ds-c-fc" title="Касса с 1-го числа ÷ прошедшие дни × дней в месяце">' + esc(fcHead) + '</span>'
        + '<span role="columnheader">Причина</span><span role="columnheader">Статус</span></div>';
      var body = rows.map(function (x) { return rowHTML(x, d, fcHead); }).join('');
      var emptyTxt = st.status === 'exc' ? 'Исключений за этот месяц нет.'
        : st.status === 'all' && rop === 'all' && !goneN ? 'За этот месяц в списке никого нет.' : 'По этому фильтру никого нет.';

      var setHTML = '';
      if (st.settings && d.can_settings && d.settings_now) {
        var now = st.setDraft || {};
        var f = function (key, label) {
          var v = now[key] != null ? now[key] : setVal(d.settings_now[key]);
          return '<label>' + label + '<input class="ds-inp" inputmode="decimal" data-set="' + key + '" value="' + esc(v) + '"></label>';
        };
        setHTML = '<div class="ds-settings"><h3>Настройки оценки</h3>'
          + '<div class="ds-sub">Действуют на месяцы, которые ещё не зафиксированы, и на прогноз. Уже зафиксированные месяцы не пересчитываются.</div>'
          + '<div class="ds-set-grid">'
          + f('min_sales', 'Минимум продаж в начале, ₸ в месяц')
          + f('min_sales_senior', 'Минимум продаж для опытных, ₸ в месяц')
          + f('tenure_from', 'Оцениваем со стажа, месяцев')
          + f('tenure_senior', 'Опытный — со стажа, месяцев')
          + f('conv_share', 'Минимум конверсии, % от средней')
          + f('min_leads', 'Лидов в месяц для оценки конверсии')
          + '</div><div class="ds-row-actions"><button type="button" class="ds-btn primary" data-act="set-save"' + (st.saving ? ' disabled' : '') + '>Сохранить</button>'
          + '<button type="button" class="ds-btn" data-act="set-cancel">Отмена</button>'
          + (st.setErr ? '<span class="ds-err" role="alert">' + esc(st.setErr) + '</span>' : '') + '</div></div>';
      }

      var nth = function (k) { return (k === 2 ? 'со ' : 'с ') + k + '-го'; };
      var tf = Number(S.tenure_from), ts = Number(S.tenure_senior);
      var note = 'Минимум продаж: ' + money(S.min_sales) + ' ₸ в месяц ' + nth(Math.floor(tf) + 1) + ' по ' + ts + '-й месяц работы, '
        + money(S.min_sales_senior) + ' ₸ ' + nth(Math.floor(ts) + 1) + (tf <= 0 ? '. ' : tf === 1 ? '; первый месяц не оцениваем. ' : '; первые ' + tf + ' мес. не оцениваем. ')
        + 'Стаж — от первой продажи (продаж ещё не было — от первого лида, но не раньше появления учётки), на 1-е число оцениваемого месяца. '
        + 'В Era CRM данные есть с мая 2026: у кого первая продажа пришлась на 1–7 мая, тот работал и раньше — его стаж считаем «' + ts + '+ мес». '
        + 'Конверсию оцениваем от ' + num(S.min_leads) + ' первичных лидов за месяц: красным — ниже минимума ' + minTxt + ', жёлтым — ниже средней ' + avgTxt + '. '
        + 'Продажи, первичные лиды и первичные продажи считаются так же, как в таблице «Выполнение плана по группам». '
        + 'Таблица только формирует список — решение принимают директор по продажам и РОП.';
      var extra = goneN
        ? (st.showGone ? 'Показаны и те, кого уже нет (' + goneN + '), — они помечены. <a data-act="gone" role="button" tabindex="0">Скрыть</a>'
          : 'Не показаны ' + goneN + ': ' + (d.is_last ? 'уволены или почти не работают последние ' + esc(d.idle_days) + ' дней' : 'к концу того месяца уже не работали')
            + '. <a data-act="gone" role="button" tabindex="0">Показать</a>')
        : '';
      var plain = plainN && st.status === 'all'
        ? 'В списке ' + plainN + ' без статуса: осталась отметка РОПа, а оснований быть в списке за этот месяц уже нет.' : '';
      var staleAt = st.stale && st.fetchedAt ? almaty(st.fetchedAt) : null;
      var stale = st.stale ? 'Не удалось обновить список' + (staleAt ? ' — показан загруженный в ' + staleAt.time : '')
        + '. <a data-act="retry" role="button" tabindex="0">Повторить</a>' : '';
      var selMonth = st.busy && st.month ? st.month : d.month;

      paint('<div class="ds-root' + (st.busy ? ' busy' : '') + '">'
        + '<div class="ds-hdr"><div><h2 class="ds-title">На увольнение</h2><p class="ds-subt">' + sub + '</p></div>'
        + '<div class="ds-filters"><label>Месяц<select class="ds-sel" data-ds="month">'
        + months.map(function (m) { return '<option value="' + esc(m) + '"' + (m === selMonth ? ' selected' : '') + '>' + esc(monthName(m)) + '</option>'; }).join('')
        + '</select></label>'
        + (d.all_groups ? '<label>РОП<select class="ds-sel" data-ds="rop"><option value="all">Все РОПы</option>'
          + groups.map(function (g) { return '<option value="' + esc(g) + '"' + (g === rop ? ' selected' : '') + '>' + esc(g) + '</option>'; }).join('')
          + '</select></label>' : '')
        + (d.can_settings ? '<button type="button" class="ds-btn" data-act="settings" aria-expanded="' + !!st.settings + '">Настройки</button>' : '')
        + '</div></div>'
        + (stale ? '<p class="ds-note warn" role="status">' + stale + '</p>' : '')
        + setHTML
        + '<div class="ds-tabs" role="group" aria-label="Статус">'
        + tabs.map(function (t) {
          var on = st.status === t[0];
          return '<button type="button" class="ds-tab ' + t[2] + (on ? ' active' : '') + '" aria-pressed="' + on + '" data-tab="' + t[0] + '"><span class="k">'
            + t[1] + '</span><span class="n">' + t[3] + '</span></button>';
        }).join('') + '</div>'
        + '<div class="ds-card"><div class="ds-scroll"><div class="ds-grid' + (d.forecast ? '' : ' nofc') + '" role="table" aria-label="Менеджеры на увольнение">'
        + head + (body || '<div class="ds-empty">' + emptyTxt + '</div>') + '</div></div></div>'
        + (extra ? '<p class="ds-note">' + extra + '</p>' : '')
        + (plain ? '<p class="ds-note">' + plain + '</p>' : '')
        + '<p class="ds-note">' + esc(note) + '</p></div>');
    }

    // ── действия ───────────────────────────────────────────────────────────
    function errText(r) {
      return r.json().then(function (j) { return (j && (j.message || j.error_description || j.hint)) || ('Ошибка ' + r.status); },
        function () { return 'Ошибка ' + r.status; });
    }
    function rowById(id) {
      var rows = el.querySelectorAll('.ds-row[data-id]');
      for (var i = 0; i < rows.length; i++) if (rows[i].getAttribute('data-id') === id) return rows[i];
      return null;
    }
    function readDraft() {       // что сейчас набрано в открытой форме отметки
      if (!st.edit) return;
      var row = rowById(st.edit);
      var ta = row && row.querySelector('[data-ds="cmt"]'), cb = row && row.querySelector('[data-ds="exc"]');
      if (ta && cb) st.draft = { comment: ta.value, exception: cb.checked };
    }
    function readSetDraft() {
      if (!st.settings) return;
      var inputs = el.querySelectorAll('[data-set]');
      if (!inputs.length) return;
      st.setDraft = {};
      for (var i = 0; i < inputs.length; i++) st.setDraft[inputs[i].getAttribute('data-set')] = inputs[i].value;
    }
    // сохранение закончилось, а список за это время уже другой: перечитать, чтобы показать сохранённое
    function after() {
      if (st.state === 'ok' && st.data) load(true); else render();
    }
    function saveNote(id) {
      if (!st.data || st.saving || st.edit !== id) return;
      readDraft();
      var n = st.draft || { comment: '', exception: false };
      if (n.exception && !n.comment.trim()) { st.editErr = 'Чтобы поставить «Исключение», напишите комментарий: почему.'; render(); return; }
      var tok = token();
      if (!tok) { forget(); st.state = 'auth'; render(); return; }
      var data = st.data;
      st.saving = true; st.editErr = ''; render();
      rpc('set_dashboard_dismissal_note', { p_month: data.month, p_manager: id, p_exception: n.exception, p_comment: n.comment }, tok).then(function (r) {
        if (r.status === 401) { var e = new Error('auth'); e.auth = true; throw e; }
        if (!r.ok) return errText(r).then(function (t) { throw new Error(t); });
        return r.json();
      }).then(function (j) {
        st.saving = false;
        if (st.data !== data) { after(); return; }       // пока сохраняли, список перечитали или стёрли
        var x = (data.rows || []).filter(function (q) { return q.id === id; })[0];
        if (x) x.note = (j && j.note) || null;
        st.editErr = '';
        if (st.edit === id) { st.edit = null; st.draft = null; }
        render();
      }).catch(function (e) {
        st.saving = false;
        if (e && e.auth) { denied(tok); forget(); st.state = 'auth'; st.token = null; render(); return; }
        if (st.data !== data) { after(); return; }
        st.editErr = (e && e.message) || 'Не сохранилось';
        render();
      });
    }
    function saveSettings() {
      if (!st.data || st.saving || !st.settings) return;
      readSetDraft();
      var body = {}, bad = false, dr = st.setDraft || {};
      SET_KEYS.forEach(function (k) {
        var v = String(dr[k] == null ? '' : dr[k]).replace(/[\s  ]/g, '').replace(',', '.');
        if (v === '' || !isFinite(Number(v))) bad = true;
        body[k] = Number(v);
      });
      if (bad) { st.setErr = 'Во всех полях должны быть числа.'; render(); return; }
      var tok = token();
      if (!tok) { forget(); st.state = 'auth'; render(); return; }
      var data = st.data;
      st.saving = true; st.setErr = ''; render();
      rpc('set_dashboard_dismissal_settings', { p: body }, tok).then(function (r) {
        if (r.status === 401) { var e = new Error('auth'); e.auth = true; throw e; }
        if (!r.ok) return errText(r).then(function (t) { throw new Error(t); });
        return r.json();
      }).then(function () {
        st.saving = false;
        if (st.data !== data) { after(); return; }
        st.settings = false; st.setDraft = null; st.setErr = '';
        load(true);
      }).catch(function (e) {
        st.saving = false;
        if (e && e.auth) { denied(tok); forget(); st.state = 'auth'; st.token = null; render(); return; }
        if (st.data !== data) { after(); return; }
        st.setErr = (e && e.message) || 'Не сохранилось';
        render();
      });
    }
    function doLogin(form) {
      var email = form.querySelector('[data-ds="email"]').value.trim();
      var pass = form.querySelector('[data-ds="pass"]').value;
      if (!email || !pass || st.loginBusy) return;
      var seq = ++st.seq;
      st.loginBusy = true; st.loginErr = ''; st.loginEmail = email; render();
      Promise.resolve().then(function () { return opts.login(email, pass); }).then(function () {
        if (seq !== st.seq) return;                      // пока входили, блок стёрли (выход)
        st.loginBusy = false; st.denied = null; st.loginEmail = '';
        load(true);
      }).catch(function (e) {
        if (seq !== st.seq) return;
        st.loginBusy = false;
        st.loginErr = 'Не вошло: ' + ((e && e.message) || 'проверьте почту и пароль');
        render();
      });
    }
    function keep() { readDraft(); readSetDraft(); }    // набранное в формах — в память перед перерисовкой
    function act(a) {
      var name = a.getAttribute('data-act');
      var row = a.closest('.ds-row');
      var id = row ? row.getAttribute('data-id') : null;
      keep();
      if (name === 'retry') load(true);
      else if (name === 'relogin') { st.denied = st.token; forget(); st.state = 'auth'; st.token = null; st.loginErr = ''; render(); }
      else if (name === 'gone') { st.showGone = !st.showGone; render(); }
      else if (name === 'settings') { st.settings = !st.settings; st.setDraft = null; st.setErr = ''; render(); }
      else if (name === 'set-cancel') { st.settings = false; st.setDraft = null; st.setErr = ''; render(); }
      else if (name === 'set-save') saveSettings();
      else if (name === 'edit' && id) {
        st.edit = id; st.draft = null; st.editErr = ''; render();
        var r2 = rowById(id), ta = r2 && r2.querySelector('textarea');
        if (ta) ta.focus();
      }
      else if (name === 'cancel') { st.edit = null; st.draft = null; st.editErr = ''; render(); }
      else if (name === 'save' && id) saveNote(id);
    }

    el.addEventListener('click', function (ev) {
      var t = ev.target;
      if (!t || !t.closest) return;
      var tab = t.closest('[data-tab]');
      if (tab) { keep(); st.status = tab.getAttribute('data-tab'); st.edit = null; st.draft = null; st.editErr = ''; render(); return; }
      var a = t.closest('[data-act]');
      if (a) act(a);
    });
    el.addEventListener('keydown', function (ev) {      // ссылки-действия — с клавиатуры тоже
      if (ev.key !== 'Enter' && ev.key !== ' ') return;
      var t = ev.target;
      if (t && t.tagName === 'A' && t.getAttribute('data-act')) { ev.preventDefault(); act(t); }
    });
    el.addEventListener('change', function (ev) {
      var k = ev.target.getAttribute && ev.target.getAttribute('data-ds');
      if (k === 'month') { keep(); st.month = ev.target.value; st.status = 'all'; st.edit = null; st.draft = null; load(true); }
      else if (k === 'rop') { keep(); st.rop = ev.target.value; st.edit = null; st.draft = null; render(); }
    });
    el.addEventListener('submit', function (ev) {
      var f = ev.target;
      if (f.getAttribute && f.getAttribute('data-ds') === 'login') { ev.preventDefault(); if (opts.login) doLogin(f); }
    });

    return {
      // показать / обновить; пока открыта форма отметки или настроек — в сеть не ходим и не перерисовываем (текст не теряем)
      refresh: function (force) {
        if (!force && (st.edit || st.settings || st.saving) && st.state === 'ok' && token() === st.token) return;
        load(!!force);
      },
      clear: function () {
        st.seq++;
        forget();
        st.state = 'idle'; st.token = null; st.denied = null; st.month = null; st.fetchedAt = 0; st.fetchedMonth = undefined;
        st.status = 'all'; st.rop = opts.group || 'all'; st.showGone = false;
        st.loginErr = ''; st.loginBusy = false; st.loginEmail = '';
        st.html = null;
        el.innerHTML = '';
      },
      // отдел, выбранный по умолчанию (кабинет РОПа сменился)
      setGroup: function (g) {
        if ((opts.group || '') === (g || '')) return;
        opts.group = g || '';
        st.rop = opts.group || 'all';
      },
      state: function () { return st.state; }
    };
  }

  window.WGDismiss = { create: create };
})();
