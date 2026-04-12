#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Build, push, and deploy the Family Tree app to Azure Container Apps.

.DESCRIPTION
  1. Builds the Docker image via ACR Build (no local Docker needed)
  2. Restarts the Container App to pick up the new image

.PARAMETER AppName
  Base name for resources (default: familytreeapp)
.PARAMETER ResourceGroup
  Resource group name (default: <AppName>-rg)
.PARAMETER SkipBuild
  Skip the build step and just restart with the existing image
#>

param(
    [string]$AppName = "familytreeapp",
    [string]$ResourceGroup = "",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$rgName = if ($ResourceGroup) { $ResourceGroup } else { "$AppName-rg" }
$acrName = $AppName -replace '[^a-zA-Z0-9]', ''
$appNameAca = "$AppName-app"

# ── Environment variable validation ──────────────────────────────────────
function Check-EnvVars {
    param(
        [string[]]$Required,
        [string[]]$Optional,
        [string]$Context
    )
    $missing = @()
    $warnings = @()
    foreach ($v in $Required) {
        if (-not [Environment]::GetEnvironmentVariable($v)) { $missing += $v }
    }
    foreach ($v in $Optional) {
        if (-not [Environment]::GetEnvironmentVariable($v)) { $warnings += $v }
    }
    if ($missing.Count -gt 0) {
        Write-Host ""
        Write-Host "ERROR: Missing required environment variables for $Context`:" -ForegroundColor Red
        $missing | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
        Write-Host ""
        Write-Host "Set them with:" -ForegroundColor Yellow
        $missing | ForEach-Object { Write-Host "  `$env:$_ = `"<value>`"" }
        Write-Host ""
        return $false
    }
    if ($warnings.Count -gt 0) {
        Write-Host ""
        Write-Host "Warning: Optional environment variables not set for $Context`:" -ForegroundColor Yellow
        $warnings | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
        Write-Host ""
    }
    return $true
}

# Build requires auth vars baked into frontend
$buildVars = @("NEXT_PUBLIC_AZURE_AD_CLIENT_ID", "NEXT_PUBLIC_AZURE_AD_TENANT_ID")

if (-not $SkipBuild) {
    $ok = Check-EnvVars -Required $buildVars -Optional @() -Context "frontend build"
    if (-not $ok) {
        $proceed = Read-Host "Continue without auth? The deployed app won't require login. [y/N]"
        if ($proceed -ne "y") { exit 1 }
    }
}

$clientId = $env:NEXT_PUBLIC_AZURE_AD_CLIENT_ID
$tenantId = $env:NEXT_PUBLIC_AZURE_AD_TENANT_ID

Write-Host "=== Family Tree Deploy ===" -ForegroundColor Cyan
Write-Host "  Registry:   $acrName"
Write-Host "  App:        $appNameAca"
Write-Host "  RG:         $rgName"
if ($clientId) { Write-Host "  Auth:       enabled (client=$clientId)" -ForegroundColor DarkGray }
else { Write-Host "  Auth:       disabled (no NEXT_PUBLIC_AZURE_AD_CLIENT_ID)" -ForegroundColor Yellow }
Write-Host ""

if (-not $SkipBuild) {
    # Step 1: Build and push
    Write-Host "[1/2] Building and pushing Docker image..." -ForegroundColor Yellow
    Write-Host "  (this takes 2-3 minutes)" -ForegroundColor DarkGray

    $buildArgs = @(
        "acr", "build",
        "--registry", $acrName,
        "--image", "${appNameAca}:latest",
        "--file", "Dockerfile.prod",
        ".",
        "--timeout", "1200"
    )
    if ($clientId) { $buildArgs += "--build-arg"; $buildArgs += "NEXT_PUBLIC_AZURE_AD_CLIENT_ID=$clientId" }
    if ($tenantId) { $buildArgs += "--build-arg"; $buildArgs += "NEXT_PUBLIC_AZURE_AD_TENANT_ID=$tenantId" }

    # Run build (suppress output to avoid Unicode crash on Windows)
    $result = & az @buildArgs 2>&1 | Out-String

    # Wait for completion
    do {
        Start-Sleep 10
        $status = az acr task list-runs -r $acrName --top 1 --query "[0].status" -o tsv 2>&1
    } while ($status -eq "Running")

    if ($status -ne "Succeeded") {
        Write-Host "Build failed! Status: $status" -ForegroundColor Red
        Write-Host "Check logs: az acr task list-runs -r $acrName --top 1" -ForegroundColor Yellow
        exit 1
    }
    Write-Host "  Build succeeded." -ForegroundColor Green
} else {
    Write-Host "[1/2] Skipping build (--SkipBuild)" -ForegroundColor DarkGray
}

# Step 2: Restart the app
Write-Host "[2/2] Restarting Container App..." -ForegroundColor Yellow

$revision = az containerapp revision list `
    --name $appNameAca `
    --resource-group $rgName `
    --query "[?properties.active] | [0].name" -o tsv 2>&1

if ($revision) {
    az containerapp revision restart `
        --name $appNameAca `
        --resource-group $rgName `
        --revision $revision `
        -o none 2>&1
    Write-Host "  Restarted revision: $revision" -ForegroundColor Green
} else {
    Write-Host "  No active revision found. Updating image reference..." -ForegroundColor Yellow
    $acrServer = az acr show --name $acrName --query loginServer -o tsv
    az containerapp update `
        --name $appNameAca `
        --resource-group $rgName `
        --image "${acrServer}/${appNameAca}:latest" `
        -o none 2>&1
    Write-Host "  Updated." -ForegroundColor Green
}

# Step 3: Wait for healthy state
Write-Host ""
Write-Host "Waiting for app to be ready..." -ForegroundColor DarkGray
$attempts = 0
$healthy = $false
while ($attempts -lt 12 -and -not $healthy) {
    Start-Sleep 10
    $attempts++
    $state = az containerapp revision list `
        --name $appNameAca `
        --resource-group $rgName `
        --query "[?properties.active] | [0].properties.runningState" -o tsv 2>&1
    Write-Host "  Status: $state" -ForegroundColor DarkGray
    # Both "Running" and "ScaledToZero" mean the revision is healthy
    if ($state -eq "Running" -or $state -eq "ScaledToZero") { $healthy = $true }
}

if ($healthy) {
    $fqdn = az containerapp show --name $appNameAca --resource-group $rgName --query "properties.configuration.ingress.fqdn" -o tsv
    $domains = az containerapp show --name $appNameAca --resource-group $rgName --query "properties.configuration.ingress.customDomains[].name" -o tsv
    Write-Host ""
    Write-Host "=== Deploy Complete ===" -ForegroundColor Green
    Write-Host "  Default:  https://$fqdn"
    if ($domains) {
        $domains -split "`n" | ForEach-Object { Write-Host "  Custom:   https://$_" }
    }
} else {
    Write-Host ""
    Write-Host "App did not reach Running state within 2 minutes." -ForegroundColor Yellow
    Write-Host "Check logs: az containerapp logs show --name $appNameAca --resource-group $rgName --type system" -ForegroundColor Yellow
}
