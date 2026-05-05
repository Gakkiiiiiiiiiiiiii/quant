[CmdletBinding()]
param(
    [string]$OutputRoot = "",
    [string]$PgHost = "127.0.0.1",
    [int]$PgPort = 5432,
    [string]$PgUser = "quant",
    [string]$PgPassword = "quantpass",
    [string]$PgContainer = "quant-postgres",
    [switch]$SkipDbDump,
    [switch]$NoZip
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
if (-not $OutputRoot) {
    $OutputRoot = Join-Path $projectRoot "migration_bundle_$timestamp"
}

$outputParent = Split-Path -Parent $OutputRoot
if ($outputParent) {
    New-Item -ItemType Directory -Path $outputParent -Force | Out-Null
}
New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null

$projectOut = Join-Path $OutputRoot "project"
$dbOut = Join-Path $OutputRoot "db"
$metaOut = Join-Path $OutputRoot "meta"
New-Item -ItemType Directory -Path $projectOut, $dbOut, $metaOut -Force | Out-Null

$manifestPath = Join-Path $projectRoot "docs\migration\migration-manifest.json"
if (-not (Test-Path -LiteralPath $manifestPath)) {
    throw "Migration manifest not found: $manifestPath"
}

$manifest = [System.IO.File]::ReadAllText($manifestPath, [System.Text.Encoding]::UTF8) | ConvertFrom-Json
$excludedDirNames = @(
    ".venv",
    ".venv-qmt36",
    ".pytest_cache",
    ".pytest_tmp",
    "pytest_tmp",
    "pytest_tmp_run",
    "node_modules",
    "__pycache__"
)

function Resolve-PgDumpTool {
    param(
        [string]$ContainerName
    )

    $localPgDump = Get-Command pg_dump -ErrorAction SilentlyContinue
    if ($localPgDump) {
        return @{
            Kind = "local"
            Command = $localPgDump.Source
        }
    }

    $dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
    if ($dockerCmd) {
        $containerId = (& $dockerCmd.Source ps --filter "name=$ContainerName" --filter "status=running" --format "{{.ID}}" 2>$null | Select-Object -First 1)
        if ($containerId) {
            return @{
                Kind = "docker"
                Command = $dockerCmd.Source
                Container = $containerId.Trim()
            }
        }
    }

    return $null
}

function Export-DatabaseDump {
    param(
        [hashtable]$DumpTool,
        [string]$DbName,
        [string]$DumpPath,
        [string]$DbHost,
        [int]$Port,
        [string]$User,
        [string]$Password
    )

    if ($DumpTool.Kind -eq "local") {
        $env:PGPASSWORD = $Password
        & $DumpTool.Command -Fc -h $DbHost -p $Port -U $User $DbName -f $DumpPath
        $exitCode = $LASTEXITCODE
        Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
        if ($exitCode -ne 0) {
            throw "Database export failed: $DbName"
        }
        return
    }

    if ($DumpTool.Kind -eq "docker") {
        $tmpPath = "/tmp/$DbName.dump"
        & $DumpTool.Command exec -e "PGPASSWORD=$Password" $DumpTool.Container pg_dump -Fc -h host.docker.internal -p $Port -U $User $DbName -f $tmpPath
        if ($LASTEXITCODE -ne 0) {
            throw "Database export failed inside container: $DbName"
        }

        & $DumpTool.Command cp "$($DumpTool.Container):$tmpPath" $DumpPath
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to copy dump from container: $DbName"
        }

        & $DumpTool.Command exec $DumpTool.Container rm -f $tmpPath | Out-Null
        return
    }

    throw "No usable pg_dump export method found"
}

function Copy-BundleDirectory {
    param(
        [string]$SourcePath,
        [string]$DestPath
    )

    New-Item -ItemType Directory -Path $DestPath -Force | Out-Null
    $robocopyArgs = @(
        $SourcePath,
        $DestPath,
        "/E",
        "/R:1",
        "/W:1",
        "/NFL",
        "/NDL",
        "/NJH",
        "/NJS",
        "/NP"
    )
    foreach ($name in $excludedDirNames) {
        $robocopyArgs += "/XD"
        $robocopyArgs += (Join-Path $SourcePath $name)
    }
    & robocopy @robocopyArgs | Out-Null
    if ($LASTEXITCODE -gt 7) {
        throw "Directory copy failed: $SourcePath"
    }
}

function Copy-BundleItem {
    param(
        [string]$RelativePath
    )

    $sourcePath = Join-Path $projectRoot $RelativePath
    $destPath = Join-Path $projectOut $RelativePath
    if (-not (Test-Path -LiteralPath $sourcePath)) {
        Write-Warning "Skipping missing path: $RelativePath"
        return
    }

    $destParent = Split-Path -Parent $destPath
    if ($destParent) {
        New-Item -ItemType Directory -Path $destParent -Force | Out-Null
    }

    $item = Get-Item -LiteralPath $sourcePath
    if ($item.PSIsContainer) {
        Copy-BundleDirectory -SourcePath $sourcePath -DestPath $destPath
        return
    }

    Copy-Item -LiteralPath $sourcePath -Destination $destPath -Force
}

Write-Host "Preparing migration bundle at: $OutputRoot"
foreach ($item in $manifest.copy_required) {
    Copy-BundleItem -RelativePath ([string]$item)
}

$mainRequirements = Join-Path $metaOut "requirements-main-frozen.txt"
$mainPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (Test-Path -LiteralPath $mainPython) {
    Write-Host "Exporting main environment requirements to: $mainRequirements"
    & $mainPython -m pip freeze | Out-File -LiteralPath $mainRequirements -Encoding utf8
}
else {
    Write-Warning "Main Python environment not found, skipping frozen requirements export"
}

$qmtRequirements = Join-Path $metaOut "requirements-qmt36-frozen.txt"
$qmtPython = Join-Path $projectRoot ".venv-qmt36\Scripts\python.exe"
if (Test-Path -LiteralPath $qmtPython) {
    Write-Host "Exporting QMT bridge requirements to: $qmtRequirements"
    & $qmtPython -m pip freeze | Out-File -LiteralPath $qmtRequirements -Encoding utf8
}
else {
    Write-Warning "QMT bridge Python environment not found, skipping frozen requirements export"
}

if (-not $SkipDbDump) {
    $dumpTool = Resolve-PgDumpTool -ContainerName $PgContainer
    if (-not $dumpTool) {
        throw "Could not find pg_dump or a running PostgreSQL container"
    }

    foreach ($dbItem in $manifest.databases) {
        $dbName = [string]$dbItem.name
        $dumpPath = Join-Path $dbOut ("{0}.dump" -f $dbName)
        Write-Host ("Exporting database: {0} -> {1}" -f $dbName, $dumpPath)
        Export-DatabaseDump -DumpTool $dumpTool -DbName $dbName -DumpPath $dumpPath -DbHost $PgHost -Port $PgPort -User $PgUser -Password $PgPassword
    }
}
else {
    Write-Host "Skipping database dump by parameter"
}

$bundleManifest = [ordered]@{
    prepared_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss")
    prepared_by = "Codex"
    source_project_root = $projectRoot
    output_root = $OutputRoot
    includes_db_dump = (-not $SkipDbDump)
    required_paths = @($manifest.copy_required)
    databases = @($manifest.databases)
}
$bundleManifestPath = Join-Path $metaOut "bundle-manifest.json"
$bundleManifest | ConvertTo-Json -Depth 6 | Out-File -LiteralPath $bundleManifestPath -Encoding utf8

$bundleEntryPath = Join-Path $OutputRoot "install_on_new_machine.ps1"
$bundleEntryContent = @'
[CmdletBinding()]
param(
    [string]$TargetRoot = "D:\project\quant",
    [string]$PgHost = "127.0.0.1",
    [int]$PgPort = 5432,
    [string]$PgUser = "quant",
    [switch]$SkipDbRestore,
    [switch]$SkipFrontendBuild,
    [switch]$KeepLiveTradeEnabled
)

$ErrorActionPreference = "Stop"
$bundleRoot = $PSScriptRoot
$installer = Join-Path $bundleRoot "project\scripts\install_migration_bundle.ps1"
if (-not (Test-Path -LiteralPath $installer)) {
    throw "Installer script not found: $installer"
}

& powershell -ExecutionPolicy Bypass -File $installer `
    -BundleRoot $bundleRoot `
    -TargetRoot $TargetRoot `
    -PgHost $PgHost `
    -PgPort $PgPort `
    -PgUser $PgUser `
    @(
        if ($SkipDbRestore) { '-SkipDbRestore' }
        if ($SkipFrontendBuild) { '-SkipFrontendBuild' }
        if ($KeepLiveTradeEnabled) { '-KeepLiveTradeEnabled' }
    )
'@
Set-Content -LiteralPath $bundleEntryPath -Value $bundleEntryContent -Encoding utf8

if (-not $NoZip) {
    $zipPath = "{0}.zip" -f $OutputRoot
    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }
    Write-Host "Compressing migration bundle to: $zipPath"
    Compress-Archive -Path (Join-Path $OutputRoot "*") -DestinationPath $zipPath -Force
}

Write-Host "Migration bundle completed"
Write-Host "Output directory: $OutputRoot"
