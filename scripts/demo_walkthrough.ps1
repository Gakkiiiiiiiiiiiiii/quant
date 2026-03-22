param(
    [ValidateSet('backtest', 'paper', 'live')]
    [string]$ViewProfile = 'backtest',
    [int]$UiPort = 8501,
    [int]$ApiPort = 8011,
    [switch]$PrepareDemoData,
    [switch]$SkipQmtLaunch,
    [switch]$SkipUi,
    [switch]$SkipApi
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$Root = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $Root 'runtime\demo_logs'
New-Item -ItemType Directory -Force $LogDir | Out-Null
Set-Location $Root

function Resolve-ProfileConfig {
    param([string]$Profile)

    switch ($Profile) {
        'backtest' { return Join-Path $Root 'configs\app.yaml' }
        'paper' { return Join-Path $Root 'configs\paper.yaml' }
        'live' { return Join-Path $Root 'configs\live.yaml' }
    }
}

function Ensure-File {
    param([string]$PathValue, [string]$Label)

    if (-not (Test-Path $PathValue)) {
        throw "$Label 不存在: $PathValue"
    }
}

function Start-BackgroundCommand {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory
    )

    $stdoutPath = Join-Path $LogDir "$Name.stdout.log"
    $stderrPath = Join-Path $LogDir "$Name.stderr.log"
    Remove-Item $stdoutPath, $stderrPath -ErrorAction SilentlyContinue

    $process = Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $WorkingDirectory `
        -PassThru `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath

    Start-Sleep -Seconds 3
    $alive = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if (-not $alive) {
        $stdout = if (Test-Path $stdoutPath) { Get-Content -Raw $stdoutPath } else { '' }
        $stderr = if (Test-Path $stderrPath) { Get-Content -Raw $stderrPath } else { '' }
        throw "$Name 启动失败。stdout: $stdout stderr: $stderr"
    }

    return [PSCustomObject]@{
        Process = $process
        Stdout = $stdoutPath
        Stderr = $stderrPath
    }
}

$Python = Join-Path $Root '.venv\Scripts\python.exe'
$QmtExe = Join-Path $Root 'runtime\qmt_client\installed\bin.x64\XtItClient.exe'
$LiveConfig = Join-Path $Root 'configs\live.yaml'
$ViewConfig = Resolve-ProfileConfig -Profile $ViewProfile

Ensure-File -PathValue $Python -Label '主虚拟环境 Python'
Ensure-File -PathValue $LiveConfig -Label '实盘配置'
Ensure-File -PathValue $ViewConfig -Label '展示配置'

Write-Host '=== 第 1 步：检查并启动 QMT 客户端 ==='
$qmtProcess = Get-Process | Where-Object { $_.ProcessName -like 'XtItClient*' -or $_.ProcessName -like 'XtMiniQmt*' } | Select-Object -First 1
if (-not $qmtProcess) {
    if ($SkipQmtLaunch) {
        throw '当前未检测到 QMT 进程，且指定了 -SkipQmtLaunch。'
    }
    Ensure-File -PathValue $QmtExe -Label 'QMT 客户端'
    $started = Start-Process -FilePath $QmtExe -ArgumentList @() -WorkingDirectory (Split-Path -Parent $QmtExe) -PassThru
    Write-Host "已尝试启动 QMT，进程 ID: $($started.Id)"
    Start-Sleep -Seconds 8
} else {
    Write-Host "检测到 QMT 已在运行，进程 ID: $($qmtProcess.Id)"
}

Write-Host ''
Write-Host '=== 第 2 步：执行只读联调探测 ==='
& $Python 'scripts\run_live.py' '--config' $LiveConfig '--mode' 'probe'
if ($LASTEXITCODE -ne 0) {
    throw 'QMT 联调探测失败，请先确认客户端已登录。'
}

if ($PrepareDemoData) {
    Write-Host ''
    Write-Host '=== 第 3 步：准备展示数据 ==='
    switch ($ViewProfile) {
        'backtest' {
            & $Python 'scripts\run_backtest.py'
        }
        'paper' {
            & $Python 'scripts\run_paper.py'
        }
        'live' {
            Write-Host 'live 视图模式不会自动下单，跳过数据准备。'
        }
    }
    if ($LASTEXITCODE -ne 0) {
        throw '展示数据准备失败。'
    }
}

$uiRuntime = $null
$apiRuntime = $null

if (-not $SkipUi) {
    Write-Host ''
    Write-Host '=== 第 4 步：启动 Streamlit UI ==='
    $uiArgs = @(
        '-m',
        'streamlit',
        'run',
        'scripts\run_dashboard.py',
        '--server.port',
        "$UiPort",
        '--server.headless',
        'true',
        '--',
        '--config',
        $ViewConfig
    )
    $uiRuntime = Start-BackgroundCommand -Name 'ui' -FilePath $Python -ArgumentList $uiArgs -WorkingDirectory $Root
    Write-Host "UI 已启动，进程 ID: $($uiRuntime.Process.Id)"
    Write-Host "UI 日志: $($uiRuntime.Stdout)"
}

if (-not $SkipApi) {
    Write-Host ''
    Write-Host '=== 第 5 步：启动本地 API ==='
    $apiArgs = @('scripts\run_api.py', '--config', $ViewConfig, '--port', "$ApiPort")
    $apiRuntime = Start-BackgroundCommand -Name 'api' -FilePath $Python -ArgumentList $apiArgs -WorkingDirectory $Root
    Write-Host "API 已启动，进程 ID: $($apiRuntime.Process.Id)"
    Write-Host "API 日志: $($apiRuntime.Stdout)"
}

Write-Host ''
Write-Host '=== 演示入口 ==='
Write-Host "QMT 探测命令: .\.venv\Scripts\python.exe scripts\run_live.py --config configs\live.yaml --mode probe"
if ($uiRuntime) {
    Write-Host "UI 地址: http://127.0.0.1:$UiPort"
}
if ($apiRuntime) {
    Write-Host "API 地址: http://127.0.0.1:$ApiPort"
    Write-Host "API 健康检查: http://127.0.0.1:$ApiPort/health"
    Write-Host "API 汇总: http://127.0.0.1:$ApiPort/summary"
    Write-Host "API 订单: http://127.0.0.1:$ApiPort/orders"
    Write-Host "API 成交: http://127.0.0.1:$ApiPort/trades"
}
Write-Host ''
Write-Host '建议操作顺序：'
Write-Host '1. 先观察 probe 输出里的账户、资产和行情。'
Write-Host '2. 打开 UI 看资产曲线、订单和成交。'
Write-Host '3. 用浏览器访问 API 的 /summary、/orders、/trades。'
Write-Host '4. 如需关闭 UI/API，结束上面打印的进程 ID 即可。'
