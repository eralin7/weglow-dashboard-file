<?php
// Скопируйте в advice-config.php, впишите ключ и загрузите на хостинг
// НА УРОВЕНЬ ВЫШЕ папки сайта (рядом с public_html), либо рядом с advice.php
// (тогда доступ к файлу закрывает .htaccess). В git этот файл не попадает.
return [
  'deepseek_key' => 'sk-ВАШ_КЛЮЧ_DEEPSEEK',

  // Необязательно — переопределение настроек по умолчанию:
  // 'model'           => 'deepseek-chat',
  // 'allowed_origins' => ['https://officeweglow.kz', 'https://www.officeweglow.kz'],
  // 'limit_day'       => 400,
];
