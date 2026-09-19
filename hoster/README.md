# Советник на вашем Windows Server (hoster.kz)

Дашборд на officeweglow.kz — статика на GitHub Pages, он не умеет хранить
секреты. Поэтому ключ DeepSeek живёт на вашем сервере в маленьком сервисе
`advice-server.js` (Node, портативный `node.exe`), а по HTTPS его публикует
Caddy — один exe, который сам получает и продлевает сертификат Let's Encrypt.
Дашборд шлёт сервису уже посчитанные метрики, сервис спрашивает DeepSeek и
отдаёт рекомендации всем пользователям.

Дашборд уже настроен на адрес `https://api.officeweglow.kz/advice`.

## Установка — три шага

1. **DNS.** В панели hoster.kz для домена `officeweglow.kz` добавьте
   A-запись `api` → внешний IP сервера. Если у сервера есть свой брандмауэр
   в панели хостинга, откройте там входящие TCP 80 и 443.

2. **На сервере** откройте PowerShell от имени администратора и выполните:

   ```powershell
   Set-ExecutionPolicy -Scope Process Bypass -Force
   Invoke-WebRequest -UseBasicParsing https://raw.githubusercontent.com/eralin7/weglow-dashboard-file/main/hoster/install-windows.ps1 -OutFile install-windows.ps1
   .\install-windows.ps1
   ```

   Скрипт спросит ключ DeepSeek (вставьте и нажмите Enter), остальное сделает
   сам: скачает `node.exe`, создаст `C:\weglow-advice`, откроет порты,
   зарегистрирует задачу с автозапуском и покажет результат самопроверки.
   Если на сервере уже работает Caddy (например для era-crm), второй экземпляр
   не поднимается: скрипт допишет адрес в его конфиг, проверит и перезагрузит
   конфигурацию, а прежний файл сохранит рядом с расширением `.bak`.
   Иначе он скачает `caddy.exe` и заведёт для него свою задачу. Если домен другой:
   `.\install-windows.ps1 -Domain advice.ваш-домен.kz` — тогда тот же адрес
   нужно вписать в `ADVICE_URL` в `dashboard.html`.

3. **Проверка.** Откройте `https://api.officeweglow.kz/advice?selftest=1`.
   Первый запрос может занять до минуты — Caddy получает сертификат. Должно
   быть `"ok": true` и `"key_configured": true`. После этого советник
   работает у всех, кто открывает дашборд.

## Как это защищено

- Ключ хранится в `C:\weglow-advice\advice-config.json`; доступ к файлу
  только у администраторов и SYSTEM, в git он не попадает.
- Сервис слушает только `127.0.0.1:8787`, снаружи виден только Caddy по HTTPS.
- Запросы принимаются только с `officeweglow.kz` (Origin/Referer).
- Одинаковые метрики 30 минут отвечаются из кэша без обращения к DeepSeek.
- Лимиты: 12 запросов с одного IP за 10 минут, 60 в час, 400 в сутки
  (правятся в `advice-config.json`, затем перезапуск задачи).

## Обслуживание

- Логи: `C:\weglow-advice\advice.log` и `C:\weglow-advice\caddy.log`.
- Сменить ключ: запустить `install-windows.ps1` ещё раз и ввести новый ключ,
  либо отредактировать `advice-config.json` и перезапустить задачу:
  `Stop-ScheduledTask 'WeGlow Advice'; Start-ScheduledTask 'WeGlow Advice'`.
- Обновить сервис после изменений в репозитории: снова запустить
  `install-windows.ps1` (Enter на вопросе о ключе оставит текущий).
- Удалить: `Unregister-ScheduledTask 'WeGlow Advice' -Confirm:$false`,
  то же для `'WeGlow Caddy'`, затем удалить папку `C:\weglow-advice`.

## Если что-то не так

- `selftest` по HTTPS не открывается, а `http://127.0.0.1:8787/advice?selftest=1`
  на самом сервере работает — проблема в DNS, портах 80/443 или сертификате:
  смотрите `caddy.log`.
- В панели советника «сервер советника не отвечает» — сервис не запущен:
  `Get-ScheduledTask 'WeGlow *' | Get-ScheduledTaskInfo`.
- Порты 80/443 занял другой веб-сервер, а сертификата нет: посмотрите, кто их
  держит — `Get-CimInstance Win32_Process -Filter "name='caddy.exe'" |
  Select ProcessId, CommandLine`. Если это чужой Caddy, допишите в его конфиг
  блок `api.officeweglow.kz { reverse_proxy 127.0.0.1:8787 }` и выполните
  `caddy reload --config <его конфиг>`. Если это IIS, остановите его
  (`Stop-Service W3SVC`, `Set-Service W3SVC -StartupType Disabled`).

## Альтернатива: обычный PHP-хостинг

Если сервис нужно поднять на shared-хостинге без Windows, в этой папке есть
`advice.php` с тем же протоколом: положите его вместе с `.htaccess` в папку
сайта, ключ — в `advice-config.php` (пример рядом), а в `ADVICE_URL` укажите
`https://ваш-домен/advice.php`. Проверка: `advice.php?selftest=1`.
