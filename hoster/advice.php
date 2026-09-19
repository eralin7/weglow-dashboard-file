<?php
/**
 * WeGlow — серверная часть ИИ-советника дашборда (officeweglow.kz).
 *
 *   POST advice.php   { "metrics": "...", "period": "..." }
 *                 →   { "ts", "sections": {KPI,PLAN,DYNAMICS,LEADS,SUMMARY}, "text", "model", "cached" }
 *   GET  advice.php?selftest=1   — проверка установки (ключ не показывается)
 *
 * Ключ DeepSeek берётся по порядку из: переменной окружения DEEPSEEK_API_KEY,
 * файла ../advice-config.php (на уровень выше папки сайта), файла ./advice-config.php.
 * Одинаковые метрики отвечаются из кэша 30 минут; есть лимит по IP и общие
 * лимиты в час/сутки, чтобы чужие запросы не тратили баланс.
 * Требования: PHP 7.4+, расширение curl (обычно есть на любом хостинге).
 */

declare(strict_types=1);

$CONFIG = [
  'deepseek_key'    => getenv('DEEPSEEK_API_KEY') ?: '',
  'model'           => 'deepseek-chat',
  'allowed_origins' => [
    'https://officeweglow.kz', 'https://www.officeweglow.kz',
    'http://localhost:8765', 'http://127.0.0.1:8765',        // локальный предпросмотр
  ],
  'cache_ttl'   => 1800,   // сек: кэш ответа для одинакового набора метрик
  'limit_ip'    => 12,     // запросов с одного IP за 10 минут
  'limit_hour'  => 60,     // всего запросов к DeepSeek за час
  'limit_day'   => 400,    // всего за сутки
  'timeout'     => 55,     // сек ожидания ответа DeepSeek
  'cache_dir'   => __DIR__ . '/advice-cache',
];
foreach ([dirname(__DIR__) . '/advice-config.php', __DIR__ . '/advice-config.php'] as $cfgFile) {
  if (is_file($cfgFile)) {
    $user = include $cfgFile;
    if (is_array($user)) { $CONFIG = array_merge($CONFIG, $user); }
    break;
  }
}

// ── Вспомогательные функции ──────────────────────────────────────────────
function respond(int $status, array $data): void {
  http_response_code($status);
  header('Content-Type: application/json; charset=utf-8');
  header('Cache-Control: no-store');
  echo json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
  exit;
}

function origin_allowed(array $cfg, string $origin): bool {
  return $origin !== '' && in_array($origin, $cfg['allowed_origins'], true);
}

function ensure_cache_dir(string $dir): bool {
  if (!is_dir($dir)) { @mkdir($dir, 0700, true); }
  if (is_dir($dir) && !is_file($dir . '/.htaccess')) { @file_put_contents($dir . '/.htaccess', "Require all denied\n"); }
  return is_dir($dir) && is_writable($dir);
}

// Счётчик событий в скользящем окне: в файле по одной метке времени на строку.
function bump_counter(string $file, int $window): int {
  $now = time();
  $kept = [];
  if (is_file($file)) {
    foreach (explode("\n", (string)file_get_contents($file)) as $line) {
      $t = (int)$line;
      if ($t > $now - $window) { $kept[] = $t; }
    }
  }
  $kept[] = $now;
  @file_put_contents($file, implode("\n", $kept), LOCK_EX);
  return count($kept);
}

function parse_sections(string $raw): array {
  $parts = preg_split('/\[([A-Z]+)\]/', $raw, -1, PREG_SPLIT_DELIM_CAPTURE);
  $out = [];
  for ($i = 1; $i < count($parts); $i += 2) {
    $key  = trim($parts[$i]);
    $text = trim($parts[$i + 1] ?? '');
    if ($key !== '' && $text !== '') { $out[$key] = $text; }
  }
  return $out;
}

// [status, body, error]
function http_post_json(string $url, string $payload, array $headers, int $timeout): array {
  $headers[] = 'Content-Type: application/json';
  if (function_exists('curl_init')) {
    $ch = curl_init($url);
    curl_setopt_array($ch, [
      CURLOPT_POST           => true,
      CURLOPT_POSTFIELDS     => $payload,
      CURLOPT_HTTPHEADER     => $headers,
      CURLOPT_RETURNTRANSFER => true,
      CURLOPT_TIMEOUT        => $timeout,
      CURLOPT_CONNECTTIMEOUT => 15,
    ]);
    $body   = curl_exec($ch);
    $err    = $body === false ? curl_error($ch) : '';
    $status = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
    curl_close($ch);
    return [$status, (string)$body, $err];
  }
  $ctx  = stream_context_create(['http' => [
    'method' => 'POST', 'header' => implode("\r\n", $headers), 'content' => $payload,
    'timeout' => $timeout, 'ignore_errors' => true,
  ]]);
  $body = @file_get_contents($url, false, $ctx);
  if ($body === false) { return [0, '', 'исходящее соединение запрещено (нет curl и allow_url_fopen)']; }
  $status = 0;
  foreach ($http_response_header ?? [] as $h) {
    if (preg_match('#^HTTP/\S+\s+(\d{3})#', $h, $m)) { $status = (int)$m[1]; }
  }
  return [$status, (string)$body, ''];
}

// ── CORS ─────────────────────────────────────────────────────────────────
$origin = $_SERVER['HTTP_ORIGIN'] ?? '';
if (origin_allowed($CONFIG, $origin)) {
  header('Access-Control-Allow-Origin: ' . $origin);
  header('Vary: Origin');
  header('Access-Control-Allow-Methods: POST, GET, OPTIONS');
  header('Access-Control-Allow-Headers: Content-Type');
  header('Access-Control-Max-Age: 86400');
}
$method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
if ($method === 'OPTIONS') { http_response_code(204); exit; }

// ── Самопроверка установки ───────────────────────────────────────────────
if ($method === 'GET' && isset($_GET['selftest'])) {
  $cacheOk = ensure_cache_dir($CONFIG['cache_dir']);
  respond(200, [
    'ok'                 => $CONFIG['deepseek_key'] !== '' && $cacheOk && (function_exists('curl_init') || ini_get('allow_url_fopen')),
    'php'                => PHP_VERSION,
    'curl'               => function_exists('curl_init'),
    'key_configured'     => $CONFIG['deepseek_key'] !== '',
    'model'              => $CONFIG['model'],
    'cache_dir_writable' => $cacheOk,
    'allowed_origins'    => $CONFIG['allowed_origins'],
    'limits'             => ['per_ip_10min' => $CONFIG['limit_ip'], 'per_hour' => $CONFIG['limit_hour'], 'per_day' => $CONFIG['limit_day']],
  ]);
}
if ($method !== 'POST') { respond(405, ['error' => 'нужен POST с JSON {metrics}']); }

// ── Доступ: только с дашборда (Origin или Referer из списка) ─────────────
$refOrigin = '';
$referer = $_SERVER['HTTP_REFERER'] ?? '';
if ($referer !== '') {
  $p = parse_url($referer);
  if (is_array($p) && isset($p['scheme'], $p['host'])) {
    $refOrigin = $p['scheme'] . '://' . $p['host'] . (isset($p['port']) ? ':' . $p['port'] : '');
  }
}
if (!origin_allowed($CONFIG, $origin) && !origin_allowed($CONFIG, $refOrigin)) {
  respond(403, ['error' => 'запросы принимаются только с дашборда']);
}
if ($CONFIG['deepseek_key'] === '') {
  respond(503, ['error' => 'на сервере не задан ключ DeepSeek (advice-config.php)']);
}

// ── Входные данные ───────────────────────────────────────────────────────
$raw = (string)file_get_contents('php://input');
if (strlen($raw) > 200000) { respond(413, ['error' => 'слишком большой запрос']); }
$in = json_decode($raw, true);
$metrics = (is_array($in) && isset($in['metrics']) && is_string($in['metrics'])) ? trim($in['metrics']) : '';
if ($metrics === '') { respond(400, ['error' => 'нужно поле metrics']); }
$metrics = function_exists('mb_substr') ? mb_substr($metrics, 0, 12000) : substr($metrics, 0, 12000);

// ── Кэш ──────────────────────────────────────────────────────────────────
$cacheDir = $CONFIG['cache_dir'];
if (!ensure_cache_dir($cacheDir)) {
  $cacheDir = rtrim(sys_get_temp_dir(), '/') . '/weglow-advice';
  ensure_cache_dir($cacheDir);
}
$hash      = sha1($metrics . '|' . $CONFIG['model']);
$cacheFile = $cacheDir . '/' . $hash . '.json';
if (is_file($cacheFile) && time() - filemtime($cacheFile) < (int)$CONFIG['cache_ttl']) {
  $cached = json_decode((string)file_get_contents($cacheFile), true);
  if (is_array($cached) && isset($cached['sections'])) {
    $cached['cached'] = true;
    respond(200, $cached);
  }
}

// ── Лимиты (кэш-попадания выше не считаются) ─────────────────────────────
$ip = $_SERVER['REMOTE_ADDR'] ?? '0.0.0.0';
if (bump_counter($cacheDir . '/rl-ip-' . sha1($ip) . '.txt', 600) > (int)$CONFIG['limit_ip']) {
  respond(429, ['error' => 'слишком много запросов, попробуйте через несколько минут']);
}
if (bump_counter($cacheDir . '/rl-hour.txt', 3600) > (int)$CONFIG['limit_hour']) {
  respond(429, ['error' => 'часовой лимит советника исчерпан']);
}
if (bump_counter($cacheDir . '/rl-day.txt', 86400) > (int)$CONFIG['limit_day']) {
  respond(429, ['error' => 'суточный лимит советника исчерпан']);
}

// ── Запрос к DeepSeek ────────────────────────────────────────────────────
$system = 'Ты — ИИ-советник отдела продаж WeGlow (Казахстан, косметика и БАД). '
  . 'Отвечай по-русски, коротко, без вступлений. Каждый пункт — конкретное действие с опорой на цифры из данных.';
$userMsg = "ДАННЫЕ ДАШБОРДА:\n" . $metrics . "\n\nДай рекомендации по 5 блокам. Формат строго такой, без другого текста:\n\n"
  . "[KPI]\n- 2-3 рекомендации по ключевым показателям (продажи, конверсия, средний чек, ДРР)\n\n"
  . "[PLAN]\n- 2-3 рекомендации по выполнению плана группами (кто отстаёт, что делать)\n\n"
  . "[DYNAMICS]\n- 2-3 рекомендации по динамике продаж и товарному миксу\n\n"
  . "[LEADS]\n- 2-3 рекомендации по лидам и конверсии (качество лидов, воронка, дожим)\n\n"
  . "[SUMMARY]\n- 2-3 рекомендации по итогам периода (прогноз, риски, приоритеты)";
$payload = json_encode([
  'model'       => $CONFIG['model'],
  'temperature' => 0.4,
  'max_tokens'  => 1200,
  'messages'    => [
    ['role' => 'system', 'content' => $system],
    ['role' => 'user',   'content' => $userMsg],
  ],
], JSON_UNESCAPED_UNICODE);

[$status, $body, $err] = http_post_json(
  'https://api.deepseek.com/chat/completions', (string)$payload,
  ['Authorization: Bearer ' . $CONFIG['deepseek_key']], (int)$CONFIG['timeout']
);
if ($err !== '') { respond(502, ['error' => 'нет связи с DeepSeek: ' . $err]); }

$j = json_decode($body, true);
if ($status !== 200) {
  $detail = (is_array($j) && isset($j['error']['message'])) ? (string)$j['error']['message'] : '';
  if ($status === 401)      { $human = 'ключ DeepSeek не принят'; }
  elseif ($status === 402)  { $human = 'на балансе DeepSeek нет средств'; }
  elseif ($status === 429)  { $human = 'DeepSeek ограничил частоту запросов'; }
  else                      { $human = 'DeepSeek ответил ошибкой ' . $status; }
  respond(502, ['error' => $human . ($detail !== '' ? ': ' . $detail : '')]);
}

$text     = trim((string)($j['choices'][0]['message']['content'] ?? ''));
$sections = parse_sections($text);
if (!$sections) { respond(502, ['error' => 'DeepSeek вернул ответ без разделов']); }

$out = [
  'ts'       => (int)round(microtime(true) * 1000),
  'sections' => $sections,
  'text'     => $text,
  'model'    => $CONFIG['model'],
  'cached'   => false,
];
@file_put_contents($cacheFile, json_encode($out, JSON_UNESCAPED_UNICODE), LOCK_EX);
respond(200, $out);
