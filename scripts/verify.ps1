param(
    [string[]]$Python = @('3.12', '3.13', '3.14'),
    [string]$PostgresDsn = $env:ALETORA_TEST_POSTGRES_DSN,
    [string]$QdrantUrl = $env:ALETORA_TEST_QDRANT_URL,
    [string]$EnvironmentRoot = (Join-Path $env:TEMP 'aletora-verify'),
    [switch]$Cleanup
)

$ErrorActionPreference = 'Stop'
if (-not $PostgresDsn) {
    throw 'Set ALETORA_TEST_POSTGRES_DSN to an isolated disposable test database. Full verification cannot skip PostgreSQL.'
}
if ($Python.Count -eq 0) {
    throw 'At least one Python interpreter must be supplied.'
}

function Invoke-Checked {
    param([string]$Executable, [string[]]$Arguments)
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE"
    }
}

$oldEnvironment = $env:UV_PROJECT_ENVIRONMENT
$oldDsn = $env:ALETORA_TEST_POSTGRES_DSN
$oldQdrantUrl = $env:ALETORA_TEST_QDRANT_URL
$runId = [Guid]::NewGuid().ToString('N')
$EnvironmentRoot = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($EnvironmentRoot)
$artifactRoot = Join-Path $EnvironmentRoot "aletora-artifacts-$runId"
$createdPaths = @($artifactRoot)
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    $env:ALETORA_TEST_POSTGRES_DSN = $PostgresDsn
    if ($QdrantUrl) {
        $env:ALETORA_TEST_QDRANT_URL = $QdrantUrl
    }
    foreach ($interpreter in $Python) {
        $interpreterId = [Guid]::NewGuid().ToString('N')
        $environment = Join-Path $EnvironmentRoot "aletora-verify-$runId-$interpreterId"
        $createdPaths += $environment
        $env:UV_PROJECT_ENVIRONMENT = $environment
        Invoke-Checked 'uv' @('sync', '--frozen', '--all-extras', '--group', 'dev', '--python', $interpreter)
        $checkedPython = Join-Path $environment 'Scripts\python.exe'
        $ruff = Join-Path $environment 'Scripts\ruff.exe'
        Invoke-Checked $checkedPython @('--version')
        Invoke-Checked $ruff @('check', '.')
        Invoke-Checked $ruff @('format', '--check', 'src', 'tests')
        Invoke-Checked $checkedPython @('-m', 'mypy')
        Invoke-Checked $checkedPython @('-m', 'pytest', '-ra')

        $version = & $checkedPython -c 'import platform; print(platform.python_version())'
        $artifactDirectory = Join-Path $artifactRoot "$version-$interpreterId"
        Invoke-Checked $checkedPython @('-m', 'build', '--no-isolation', '--outdir', $artifactDirectory)
        $wheel = Get-ChildItem -LiteralPath $artifactDirectory -Filter 'aletora-*.whl' | Select-Object -First 1
        if (-not $wheel) {
            throw "Built wheel was not found under $artifactDirectory"
        }
        $smoke = Join-Path $EnvironmentRoot "aletora-core-smoke-$runId-$interpreterId"
        $createdPaths += $smoke
        Invoke-Checked 'uv' @('venv', '--python', $checkedPython, $smoke)
        $smokePython = Join-Path $smoke 'Scripts\python.exe'
        Invoke-Checked 'uv' @('pip', 'install', '--python', $smokePython, $wheel.FullName)
        Invoke-Checked $smokePython @('-c', "import aletora, importlib.util; from aletora import Memory, Scope; assert all(importlib.util.find_spec(name) is None for name in ('sqlalchemy', 'tortoise', 'qdrant_client'))")
        Write-Host "Python $version core-only wheel smoke passed. Artifacts in $artifactDirectory"
    }
} finally {
    $env:UV_PROJECT_ENVIRONMENT = $oldEnvironment
    $env:ALETORA_TEST_POSTGRES_DSN = $oldDsn
    $env:ALETORA_TEST_QDRANT_URL = $oldQdrantUrl
    if ($Cleanup) {
        foreach ($path in $createdPaths) {
            if (Test-Path -LiteralPath $path) {
                $resolvedPath = (Resolve-Path -LiteralPath $path).ProviderPath
                $root = (Resolve-Path -LiteralPath $EnvironmentRoot).ProviderPath
                if ((Split-Path -LiteralPath $resolvedPath) -eq $root) {
                    Remove-Item -LiteralPath $resolvedPath -Recurse -Force -ErrorAction SilentlyContinue
                }
            }
        }
    }
    Pop-Location
}
