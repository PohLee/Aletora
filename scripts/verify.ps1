param(
    [string[]]$Python = @('3.12', '3.14'),
    [string]$PostgresDsn = $env:ALETORA_TEST_POSTGRES_DSN,
    [string]$EnvironmentRoot = "$env:LOCALAPPDATA\Temp\opencode"
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
$runId = [Guid]::NewGuid().ToString('N')
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    $env:ALETORA_TEST_POSTGRES_DSN = $PostgresDsn
    foreach ($interpreter in $Python) {
        $environment = Join-Path $EnvironmentRoot "aletora-verify-$runId-$([Guid]::NewGuid().ToString('N'))"
        $env:UV_PROJECT_ENVIRONMENT = $environment
        Invoke-Checked 'uv' @('sync', '--frozen', '--all-extras', '--group', 'dev', '--python', $interpreter)
        $checkedPython = Join-Path $environment 'Scripts\python.exe'
        $ruff = Join-Path $environment 'Scripts\ruff.exe'
        Invoke-Checked $ruff @('check', '.')
        Invoke-Checked $ruff @('format', '--check', 'src', 'tests')
        Invoke-Checked $checkedPython @('-m', 'mypy')
        Invoke-Checked $checkedPython @('-m', 'pytest', '-ra')
    }

    Invoke-Checked $checkedPython @('-m', 'build', '--no-isolation')
    $smoke = Join-Path $EnvironmentRoot "aletora-core-smoke-$runId"
    Invoke-Checked 'uv' @('venv', '--python', $Python[-1], $smoke)
    $smokePython = Join-Path $smoke 'Scripts\python.exe'
    Invoke-Checked 'uv' @('pip', 'install', '--python', $smokePython, '--no-deps', 'dist/aletora-0.1.0-py3-none-any.whl')
    Invoke-Checked $smokePython @('-c', "import aletora, importlib.util; from aletora import Memory, Scope; assert all(importlib.util.find_spec(name) is None for name in ('sqlalchemy', 'tortoise', 'qdrant_client'))")
} finally {
    $env:UV_PROJECT_ENVIRONMENT = $oldEnvironment
    $env:ALETORA_TEST_POSTGRES_DSN = $oldDsn
    Pop-Location
}
