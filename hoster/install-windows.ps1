<#
  Установка советника WeGlow на Windows Server.
  Файл сохранён в UTF-8 с BOM: так Windows PowerShell 5.1 правильно читает
  русские буквы. Не пересохраняйте его без BOM.

  Запуск в PowerShell от имени администратора:
    Set-ExecutionPolicy -Scope Process Bypass -Force
    .\install-windows.ps1

  Что делает:
   - кладёт всё в C:\weglow-advice (папку можно сменить: -Root D:\...),
   - скачивает портативный node.exe и caddy.exe (установщики не нужны),
   - спрашивает ключ DeepSeek и сохраняет его в advice-config.json
     (доступ к файлу только у администраторов и SYSTEM),
   - открывает порты 80/443 в брандмауэре Windows,
   - создаёт две задачи планировщика с автозапуском при старте сервера
     («WeGlow Advice» — сервис, «WeGlow Caddy» — HTTPS) и запускает их,
   - делает самопроверку.
  Повторный запуск безопасен: обновляет файлы и перезапускает задачи.
#>
param(
  [string]$Root        = 'C:\weglow-advice',
  [string]$Domain      = 'api.officeweglow.kz',
  [string]$NodeVersion = 'v22.23.2',
  [string]$RepoRaw     = 'https://raw.githubusercontent.com/eralin7/weglow-dashboard-file/main/hoster'
)

$ErrorActionPreference = 'Stop'
$ProgressPreference    = 'SilentlyContinue'   # без индикатора прогресса скачивание в PS 5.1 в разы быстрее
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function Step([string]$msg) { Write-Host "`n== $msg" -ForegroundColor Cyan }

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) { throw 'Запустите PowerShell от имени администратора.' }

Step "Папка $Root"
New-Item -ItemType Directory -Force -Path $Root | Out-Null

Step 'Файлы сервиса'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
foreach ($name in @('advice-server.js', 'Caddyfile')) {
  $local = Join-Path $here $name
  $dest  = Join-Path $Root $name
  if (Test-Path $local) { Copy-Item $local $dest -Force }
  else { Invoke-WebRequest -UseBasicParsing "$RepoRaw/$name" -OutFile $dest }
}
$caddyfile = Join-Path $Root 'Caddyfile'
$text = Get-Content $caddyfile -Raw -Encoding UTF8
$text = $text -replace 'api\.officeweglow\.kz', $Domain
$text = $text -replace 'C:/weglow-advice', ($Root -replace '\\', '/')
[IO.File]::WriteAllText($caddyfile, $text, $utf8NoBom)

Step "Node $NodeVersion (портативный)"
$nodeExe = Join-Path $Root 'node.exe'
if (-not (Test-Path $nodeExe)) {
  $zipName = "node-$NodeVersion-win-x64"
  $zip     = Join-Path $env:TEMP "$zipName.zip"
  Invoke-WebRequest -UseBasicParsing "https://nodejs.org/dist/$NodeVersion/$zipName.zip" -OutFile $zip
  # Из архива нужен только node.exe — достаём его напрямую, без распаковки тысяч файлов
  Add-Type -AssemblyName System.IO.Compression.FileSystem
  $archive = [IO.Compression.ZipFile]::OpenRead($zip)
  try {
    $entry = $archive.Entries | Where-Object { $_.Name -eq 'node.exe' } | Select-Object -First 1
    if (-not $entry) { throw 'В архиве Node не найден node.exe' }
    [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $nodeExe, $true)
  } finally { $archive.Dispose() }
  Remove-Item $zip -Force
}
Write-Host ("node " + (& $nodeExe --version))

Step 'Caddy (HTTPS с автоматическим сертификатом)'
# Если на сервере уже работает Caddy (например для era-crm), второй экземпляр
# не поднимаем: порты 80/443 заняты, он просто не стартует. Вместо этого
# допишем наш адрес в его конфиг и перезагрузим — так делает Add-SiteToCaddy.
$existingCaddy = Get-CimInstance Win32_Process -Filter "name='caddy.exe'" -ErrorAction SilentlyContinue |
                 Where-Object { $_.CommandLine -and $_.CommandLine -notlike "*$Root*" } | Select-Object -First 1
$caddyExe = $null; $caddyCfg = $null
if ($existingCaddy) {
  if ($existingCaddy.CommandLine -match '^"?(?<exe>[^"]*caddy\.exe)"?')      { $caddyExe = $Matches['exe'] }
  if ($existingCaddy.CommandLine -match '--config\s+"?(?<cfg>[^"]+?)"?(\s|$)') { $caddyCfg = $Matches['cfg'] }
}
if ($caddyExe -and $caddyCfg -and (Test-Path $caddyCfg)) {
  Write-Host "  найден работающий Caddy: $caddyExe, конфиг $caddyCfg — добавим адрес в него"
} else {
  $existingCaddy = $null
  $caddyExe = Join-Path $Root 'caddy.exe'
  if (-not (Test-Path $caddyExe)) {
    Invoke-WebRequest -UseBasicParsing 'https://caddyserver.com/api/download?os=windows&arch=amd64' -OutFile $caddyExe
  }
}
Write-Host (& $caddyExe version)

Step 'Порт сервиса'
# На сервере может уже работать чужое приложение (у нас порт 8787 занимал
# era-crm), поэтому берём первый свободный порт из диапазона.
$port = $null
foreach ($p in 8787..8807) {
  $busy = Get-NetTCPConnection -State Listen -LocalPort $p -ErrorAction SilentlyContinue
  if (-not $busy) { $port = $p; break }
}
if (-not $port) { throw 'Не нашёл свободный порт в диапазоне 8787-8807.' }
Write-Host "  сервис советника займёт порт $port"
# свой Caddyfile (нужен, только если на сервере ещё нет работающего Caddy)
$cfText = (Get-Content $caddyfile -Raw -Encoding UTF8) -replace 'reverse_proxy 127\.0\.0\.1:\d+', "reverse_proxy 127.0.0.1:$port"
[IO.File]::WriteAllText($caddyfile, $cfText, $utf8NoBom)

Step 'Ключ DeepSeek'
$cfgPath = Join-Path $Root 'advice-config.json'
$existingKey = ''
if (Test-Path $cfgPath) {
  try { $existingKey = [string](Get-Content $cfgPath -Raw | ConvertFrom-Json).deepseekKey } catch { $existingKey = '' }
}
if ($existingKey) { $key = Read-Host 'Ключ DeepSeek (Enter — оставить текущий)' } else { $key = Read-Host 'Ключ DeepSeek (sk-...)' }
$key = $key.Trim()
if (-not $key) { $key = $existingKey }
if (-not $key) { throw 'Без ключа DeepSeek советник не заработает.' }
$config = [ordered]@{
  deepseekKey    = $key
  model          = 'deepseek-chat'
  host           = '127.0.0.1'
  port           = $port
  allowedOrigins = @('https://officeweglow.kz', 'https://www.officeweglow.kz')
}
[IO.File]::WriteAllText($cfgPath, ($config | ConvertTo-Json), $utf8NoBom)
# Файл с ключом — только для SYSTEM (S-1-5-18) и администраторов (S-1-5-32-544)
& icacls $cfgPath /inheritance:r /grant:r '*S-1-5-18:F' '*S-1-5-32-544:F' | Out-Null

Step 'Брандмауэр: порты 80 и 443 для Caddy'
if (-not (Get-NetFirewallRule -DisplayName 'WeGlow Caddy 80/443' -ErrorAction SilentlyContinue)) {
  New-NetFirewallRule -DisplayName 'WeGlow Caddy 80/443' -Direction Inbound -Protocol TCP -LocalPort 80,443 -Action Allow | Out-Null
}
$listeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $_.LocalPort -eq 80 -or $_.LocalPort -eq 443 }
$names = @()
foreach ($l in $listeners) {
  $p = Get-Process -Id $l.OwningProcess -ErrorAction SilentlyContinue
  if ($p -and $p.ProcessName -ne 'caddy') { $names += $p.ProcessName }
}
$names = $names | Sort-Object -Unique
if ($names.Count -gt 0) {
  Write-Warning ("Порты 80/443 уже заняты: " + ($names -join ', ') + ". Caddy не сможет их занять — остановите IIS или другой веб-сервер и запустите установку снова.")
}

Step 'Задачи планировщика (автозапуск при старте сервера)'
function Install-Task([string]$name, [string]$exe, [string]$arguments) {
  if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $name -Confirm:$false
  }
  $action    = New-ScheduledTaskAction -Execute $exe -Argument $arguments -WorkingDirectory $Root
  $trigger   = New-ScheduledTaskTrigger -AtStartup
  $principal = New-ScheduledTaskPrincipal -UserId 'NT AUTHORITY\SYSTEM' -LogonType ServiceAccount -RunLevel Highest
  $settings  = New-ScheduledTaskSettingsSet -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -StartWhenAvailable
  $settings.ExecutionTimeLimit = 'PT0S'   # не останавливать задачу через 3 дня (работает на всех версиях Windows Server)
  Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
  Start-ScheduledTask -TaskName $name
  Write-Host "  задача «$name» создана и запущена"
}
Install-Task 'WeGlow Advice' $nodeExe 'advice-server.js'
if ($existingCaddy) {
  # Свой Caddy не запускаем — дописываем сайт в конфиг работающего и перезагружаем его
  if (Get-ScheduledTask -TaskName 'WeGlow Caddy' -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName 'WeGlow Caddy' -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName 'WeGlow Caddy' -Confirm:$false
  }
  if ((Get-Content $caddyCfg -Raw) -match [regex]::Escape($Domain)) {
    # адрес уже добавлен — поправим только порт в нашем блоке
    Copy-Item $caddyCfg "$caddyCfg.bak" -Force
    $cfgText = [regex]::Replace((Get-Content $caddyCfg -Raw),
      ('(' + [regex]::Escape($Domain) + '\s*\{[^}]*?reverse_proxy\s+127\.0\.0\.1:)\d+'), ('${1}' + $port))
    [IO.File]::WriteAllText($caddyCfg, $cfgText, $utf8NoBom)
    Write-Host "  адрес $Domain уже есть в $caddyCfg, порт обновлён на $port"
  } else {
    Copy-Item $caddyCfg "$caddyCfg.bak" -Force
    Add-Content $caddyCfg "`r`n$Domain {`r`n    reverse_proxy 127.0.0.1:$port`r`n}"
    & $caddyExe validate --config $caddyCfg | Out-Null
    if ($LASTEXITCODE -ne 0) {
      Copy-Item "$caddyCfg.bak" $caddyCfg -Force
      throw "Конфиг $caddyCfg не прошёл проверку — изменения откатаны."
    }
    Write-Host "  адрес $Domain добавлен в $caddyCfg (копия: $caddyCfg.bak)"
  }
  & $caddyExe reload --config $caddyCfg
  Write-Host '  конфигурация работающего Caddy перезагружена'
} else {
  Install-Task 'WeGlow Caddy' $caddyExe "run --config `"$caddyfile`""
}

Step 'Самопроверка'
Start-Sleep -Seconds 4
try {
  $t = Invoke-RestMethod -UseBasicParsing "http://127.0.0.1:$port/advice?selftest=1"
  Write-Host ($t | ConvertTo-Json -Compress)
  if ($t.ok) { Write-Host 'Сервис советника работает.' -ForegroundColor Green }
} catch {
  Write-Warning ("Сервис не ответил: " + $_.Exception.Message + ". Смотрите " + (Join-Path $Root 'advice.log'))
}

Write-Host ''
Write-Host "Готово. Проверьте, что DNS-запись $Domain указывает на внешний IP этого сервера," -ForegroundColor Green
Write-Host "затем откройте в браузере: https://$Domain/advice?selftest=1  (первый запрос может занять до минуты — Caddy получает сертификат)." -ForegroundColor Green
Write-Host ("Логи: " + (Join-Path $Root 'advice.log') + " и " + (Join-Path $Root 'caddy.log'))
