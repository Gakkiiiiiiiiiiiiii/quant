[CmdletBinding()]
param(
    [string]$BundleRoot = "",
    [string]$TargetRoot = "D:\project\quant",
    [string]$PgHost = "127.0.0.1",
    [int]$PgPort = 5432,
    [string]$PgUser = "quant",
    [switch]$SkipDbRestore,
    [switch]$SkipFrontendBuild,
    [switch]$KeepLiveTradeEnabled
)

$ErrorActionPreference = "Stop"

if (-not $BundleRoot) {
    throw "Missing bundle directory, for example: -BundleRoot D:\backup\migration_bundle_20260505_120000"
}
if (-not (Test-Path -LiteralPath $BundleRoot)) {
    throw "Bundle directory does not exist: $BundleRoot"
}

$projectSource = Join-Path $BundleRoot "project"
$metaSource = Join-Path $BundleRoot "meta"
$dbSource = Join-Path $BundleRoot "db"
$manifestPath = Join-Path $metaSource "bundle-manifest.json"

if (-not (Test-Path -LiteralPath $projectSource)) {
    throw "Bundle missing project directory: $projectSource"
}
if (-not (Test-Path -LiteralPath $manifestPath)) {
    throw "Bundle missing bundle-manifest.json: $manifestPath"
}

$manifest = [System.IO.File]::ReadAllText($manifestPath, [System.Text.Encoding]::UTF8) | ConvertFrom-Json

function Assert-CommandExists {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

Assert-CommandExists -Name "python"
Assert-CommandExists -Name "npm"
if (-not $SkipDbRestore) {
    Assert-CommandExists -Name "createdb"
    Assert-CommandExists -Name "pg_restore"
}

$targetParent = Split-Path -Parent $TargetRoot
if ($targetParent) {
    New-Item -ItemType Directory -Path $targetParent -Force | Out-Null
}
New-Item -ItemType Directory -Path $TargetRoot -Force | Out-Null

Write-Host "Copying project files to: $TargetRoot"
Copy-Item -LiteralPath (Join-Path $projectSource "*") -Destination $TargetRoot -Recurse -Force

if (-not $SkipDbRestore -and (Test-Path -LiteralPath $dbSource)) {
    foreach ($dbItem in $manifest.databases) {
        $dbName = [string]$dbItem.name
        $dumpPath = Join-Path $dbSource "$dbName.dump"
        if (-not (Test-Path -LiteralPath $dumpPath)) {
            Write-Warning "缺少数据库导出文件，跳过: $dumpPath"
            continue
        }
        Write-Host "Restoring database: $dbName"
        & createdb -h $PgHost -p $PgPort -U $PgUser $dbName 2>$null
        & pg_restore -h $PgHost -p $PgPort -U $PgUser -d $dbName $dumpPath
        if ($LASTEXITCODE -ne 0) {
            throw "Database restore failed: $dbName"
        }
    }
}

Push-Location $TargetRoot
try {
    Write-Host "Rebuilding main Python environment"
    & python -m venv .venv
    & .\.venv\Scripts\python.exe -m pip install --upgrade pip
    & .\.venv\Scripts\python.exe -m pip install -e .[api,test,ui]
    if ($LASTEXITCODE -ne 0) {
        throw "Main environment dependency installation failed"
    }

    Write-Host "Rebuilding QMT bridge Python 3.6 environment"
    & py -3.6 -m venv .venv-qmt36
    & .\.venv-qmt36\Scripts\python.exe -m pip install --upgrade pip
    $qmtRequirements = Join-Path $TargetRoot "requirements-qmt36.txt"
    if (-not (Test-Path -LiteralPath $qmtRequirements)) {
        throw "Missing requirements-qmt36.txt: $qmtRequirements"
    }
    & .\.venv-qmt36\Scripts\python.exe -m pip install -r $qmtRequirements
    if ($LASTEXITCODE -ne 0) {
        throw "QMT bridge environment dependency installation failed"
    }

    if (-not $SkipFrontendBuild) {
        Write-Host "Installing frontend dependencies and building"
        Push-Location (Join-Path $TargetRoot "frontend\joinquant-vue")
        try {
            & npm install
            if ($LASTEXITCODE -ne 0) {
                throw "Frontend npm install failed"
            }
            & npm run build
            if ($LASTEXITCODE -ne 0) {
                throw "Frontend npm run build failed"
            }
        }
        finally {
            Pop-Location
        }
    }

    if (-not $KeepLiveTradeEnabled) {
        $liveConfig = Join-Path $TargetRoot "configs\live.yaml"
        if (Test-Path -LiteralPath $liveConfig) {
            $content = Get-Content -LiteralPath $liveConfig -Raw
            $updated = [regex]::Replace($content, '(?m)^qmt_trade_enabled:\s*true\s*$', 'qmt_trade_enabled: false')
            if ($updated -ne $content) {
                Set-Content -LiteralPath $liveConfig -Value $updated -Encoding utf8
                Write-Host "Set live auto-trade switch to false; turn it on manually after validation on the new machine"
            }
        }
    }

    Write-Host "Running migration self-check"
    & .\.venv\Scripts\python.exe scripts\verify_migration.py
    if ($LASTEXITCODE -ne 0) {
        throw "Migration self-check failed"
    }
}
finally {
    Pop-Location
}

Write-Host "Migration install completed"
Write-Host "Project directory: $TargetRoot"
Write-Host "Suggested next steps:"
Write-Host "1. .\\.venv\\Scripts\\python.exe -m pytest"
Write-Host "2. .\\.venv\\Scripts\\python.exe scripts\\run_live.py --config configs\\live.yaml --mode probe"
Write-Host "3. .\\.venv\\Scripts\\python.exe scripts\\run_live.py --config configs\\live.yaml --mode preview --capital 0 --strategy configs\\strategy\\joinquant_microcap_alpha_calendar_crash.yaml"
