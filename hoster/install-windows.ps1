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
  $tmpDir  = Join-Path $env:TEMP $zipName
  Invoke-WebRequest -UseBasicParsing "https://nodejs.org/dist/$NodeVersion/$zipName.zip" -OutFile $zip
  if (Test-Path $tmpDir) { Remove-Item $tmpDir -Recurse -Force }
  Expand-Archive -Path $zip -DestinationPath $env:TEMP -Force
  Copy-Item (Join-Path $tmpDir 'node.exe') $nodeExe -Force
  Remove-Item $tmpDir -Recurse -Force
  Remove-Item $zip -Force
}
Write-Host ("node " + (& $nodeExe --version))

Step 'Caddy (HTTPS с автоматическим сертификатом)'
$caddyExe = Join-Path $Root 'caddy.exe'
if (-not (Test-Path $caddyExe)) {
  Invoke-WebRequest -UseBasicParsing 'https://caddyserver.com/api/download?os=windows&arch=amd64' -OutFile $caddyExe
}
Write-Host (& $caddyExe version)

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
  port           = 8787
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
Install-Task 'WeGlow Caddy'  $caddyExe "run --config `"$caddyfile`""

Step 'Самопроверка'
Start-Sleep -Seconds 4
try {
  $t = Invoke-RestMethod -UseBasicParsing 'http://127.0.0.1:8787/advice?selftest=1'
  Write-Host ($t | ConvertTo-Json -Compress)
  if ($t.ok) { Write-Host 'Сервис советника работает.' -ForegroundColor Green }
} catch {
  Write-Warning ("Сервис не ответил: " + $_.Exception.Message + ". Смотрите " + (Join-Path $Root 'advice.log'))
}

Write-Host ''
Write-Host "Готово. Проверьте, что DNS-запись $Domain указывает на внешний IP этого сервера," -ForegroundColor Green
Write-Host "затем откройте в браузере: https://$Domain/advice?selftest=1  (первый запрос может занять до минуты — Caddy получает сертификат)." -ForegroundColor Green
Write-Host ("Логи: " + (Join-Path $Root 'advice.log') + " и " + (Join-Path $Root 'caddy.log'))
