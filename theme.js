/* ═══════════════════════════════════════════════════════════════════════
   theme.js — переключатель тем дашборда WEGLOW.
   Темы описаны в theme.css (data-theme на <html>), выбор хранится в
   localStorage (wg_theme). Кнопка вставляется в сайдбар после .sb-logo, а на
   страницах без сайдбара — плавающей в правом нижнем углу.
   При смене темы шлём событие window 'wg:theme' — страница перерисовывает
   графики/карту, если ей это нужно.
   Ранний скрипт в <head> выставляет атрибут до первой отрисовки; здесь —
   только UI и сохранение выбора.
   ═══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
  var KEY = 'wg_theme';
  var DEFAULT_THEME = 'glow';
  var THEMES = [
    { id: 'glow',    name: 'Midnight Glow',   sw: ['#060c0a', '#0f1f19', '#7affb4'] },
    { id: 'paper',   name: 'Paper Editorial', sw: ['#000000', '#fdfcf5', '#10756a'] },
    { id: 'forest',  name: 'Forest Poster',   sw: ['#032e22', '#043f2e', '#c8f169'] },
    { id: 'bento',   name: 'Studio Bento',    sw: ['#ffffff', '#f7f7f8', '#615fff'] },
    { id: 'classic', name: 'Классика',        sw: ['#0f1623', '#f7f8fc', '#6366f1'] }
  ];

  function byId(id) { for (var i = 0; i < THEMES.length; i++) if (THEMES[i].id === id) return THEMES[i]; return null; }
  function current() {
    var t = null;
    try { t = localStorage.getItem(KEY); } catch (e) {}
    if (t === null || t === undefined || t === '') t = DEFAULT_THEME;
    return byId(t) ? t : DEFAULT_THEME;
  }
  function apply(id, opts) {
    var t = byId(id) ? id : DEFAULT_THEME;
    if (t === 'classic') document.documentElement.removeAttribute('data-theme');
    else document.documentElement.setAttribute('data-theme', t);
    try { localStorage.setItem(KEY, t); } catch (e) {}
    var widgets = document.querySelectorAll('.wg-theme');
    for (var i = 0; i < widgets.length; i++) updateWidget(widgets[i], t);
    if (!opts || !opts.silent) {
      try { window.dispatchEvent(new CustomEvent('wg:theme', { detail: { theme: t } })); } catch (e) {}
    }
    return t;
  }

  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }
  function swatch(sw) {
    var s = el('span', 'wg-swatch');
    for (var i = 0; i < sw.length; i++) { var b = el('i'); b.style.background = sw[i]; s.appendChild(b); }
    return s;
  }
  function buildWidget(floating) {
    var wrap = el('div', 'wg-theme' + (floating ? ' wg-theme-float' : ''));
    var btn = el('button', 'wg-theme-btn');
    btn.type = 'button';
    btn.setAttribute('aria-haspopup', 'listbox');
    btn.title = 'Сменить тему оформления';
    btn.appendChild(el('span', 'wg-theme-dot'));
    btn.appendChild(el('span', 'wg-theme-name', ''));
    btn.appendChild(el('span', 'wg-theme-caret', '▾'));
    var menu = el('div', 'wg-theme-menu');
    menu.setAttribute('role', 'listbox');
    for (var i = 0; i < THEMES.length; i++) {
      (function (th) {
        var item = el('div', 'wg-theme-item');
        item.setAttribute('role', 'option');
        item.dataset.theme = th.id;
        item.appendChild(swatch(th.sw));
        item.appendChild(el('span', null, th.name));
        item.addEventListener('click', function (e) { e.stopPropagation(); apply(th.id); wrap.classList.remove('open'); });
        menu.appendChild(item);
      })(THEMES[i]);
    }
    btn.addEventListener('click', function (e) { e.stopPropagation(); wrap.classList.toggle('open'); });
    wrap.appendChild(btn);
    wrap.appendChild(menu);
    return wrap;
  }
  function updateWidget(wrap, t) {
    var th = byId(t) || byId(DEFAULT_THEME);
    var name = wrap.querySelector('.wg-theme-name');
    if (name) name.textContent = th.name;
    var items = wrap.querySelectorAll('.wg-theme-item');
    for (var i = 0; i < items.length; i++) items[i].classList.toggle('active', items[i].dataset.theme === t);
  }

  function mount() {
    if (document.querySelector('.wg-theme')) return;
    var logo = document.querySelector('.sidebar .sb-logo');
    var widget;
    if (logo && logo.parentNode) {
      widget = buildWidget(false);
      logo.parentNode.insertBefore(widget, logo.nextSibling);
    } else {
      widget = buildWidget(true);
      document.body.appendChild(widget);
    }
    updateWidget(widget, current());
    document.addEventListener('click', function () { widget.classList.remove('open'); });
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape') widget.classList.remove('open'); });
  }

  // Синхронизируем атрибут с сохранённым выбором (ранний скрипт мог отсутствовать на странице).
  apply(current(), { silent: true });
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
  else mount();

  window.WGTheme = { THEMES: THEMES, current: current, apply: apply, KEY: KEY, DEFAULT: DEFAULT_THEME };
})();
