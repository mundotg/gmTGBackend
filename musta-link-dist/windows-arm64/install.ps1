<#
.SYNOPSIS
    Instalador e Emparelhador do MustaLink Data Connector Agent para Windows.
    Nao contem tokens embutidos: utiliza o codigo temporario (XXXX-XXXX) gerado no MustaInf.
#>
param(
    [string]$ServerUrl = "wss://api.mustainfo.cloud",
    [string]$DbHost = "127.0.0.1:5432",
    [string]$AgentId = "luanda-pg-01",
    [string]$PairCode = ""
)

$ErrorActionPreference = "Stop"
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  MustaLink Data Connector Agent - Instalador Windows " -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan

$ExePath = Join-Path $PSScriptRoot "musta-link-agent.exe"
if (!(Test-Path $ExePath)) {
    Write-Error "Executavel 'musta-link-agent.exe' nao encontrado na pasta atual."
    exit 1
}

if ([string]::IsNullOrWhiteSpace($PairCode)) {
    $PairCode = Read-Host "Introduza o Codigo de Emparelhamento gerado no MustaInf (ex: ABCD-1234)"
}

$InputServer = Read-Host "URL do Servidor MustaInf [$ServerUrl]"
if (![string]::IsNullOrWhiteSpace($InputServer)) {
    $ServerUrl = $InputServer.Trim()
}

$InputDb = Read-Host "Endereco da base de dados local [$DbHost]"
if (![string]::IsNullOrWhiteSpace($InputDb)) {
    $DbHost = $InputDb.Trim()
}

$InstallDir = Join-Path $env:LOCALAPPDATA "MustaLink\bin"
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$InstalledExe = Join-Path $InstallDir "musta-link-agent.exe"
Copy-Item -Path $ExePath -Destination $InstalledExe -Force

Write-Host "`n[+] Binario instalado em: $InstalledExe" -ForegroundColor Green
Write-Host "[+] A iniciar associacao segura e tunel reverso..." -ForegroundColor Cyan

& $InstalledExe -server $ServerUrl -db $DbHost -agent-id $AgentId -pair $PairCode
