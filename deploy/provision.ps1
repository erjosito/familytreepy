#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Provision Azure infrastructure for the Family Tree app.
  Creates: Resource Group, Container Registry, Container Apps Environment, Container App.
  Run once, then use deploy.ps1 to push updates.

.PARAMETER AppName
  Base name for all resources (default: familytreeapp)
.PARAMETER Location
  Azure region (default: spaincentral)
.PARAMETER SubscriptionId
  Azure subscription ID (uses current if not set)
#>

param(
    [string]$AppName = "familytreeapp",
    [string]$Location = "spaincentral",
    [string]$SubscriptionId = ""
)

$ErrorActionPreference = "Stop"

# Resource names
$rgName = "$AppName-rg"
$acrName = $AppName -replace '[^a-zA-Z0-9]', ''  # ACR only allows alphanumeric
$envName = "$AppName-env"
$appName = "$AppName-app"

Write-Host "=== Family Tree Azure Provisioning ===" -ForegroundColor Cyan
Write-Host "  Resource Group:   $rgName"
Write-Host "  Container Registry: $acrName"
Write-Host "  ACA Environment: $envName"
Write-Host "  Container App:   $appName"
Write-Host "  Region:          $Location"
Write-Host ""

# Set subscription if provided
if ($SubscriptionId) {
    Write-Host "Setting subscription to $SubscriptionId..."
    az account set --subscription $SubscriptionId
}

# 1. Resource Group
Write-Host "`n[1/5] Creating Resource Group..." -ForegroundColor Yellow
az group create --name $rgName --location $Location --output none

# 2. Container Registry (Basic SKU — ~$5/month)
Write-Host "[2/5] Creating Container Registry..." -ForegroundColor Yellow
az acr create --name $acrName --resource-group $rgName --sku Basic --admin-enabled true --output none

# Get ACR credentials
$acrServer = az acr show --name $acrName --query loginServer -o tsv
$acrUser = az acr credential show --name $acrName --query username -o tsv
$acrPass = az acr credential show --name $acrName --query "passwords[0].value" -o tsv

# 3. Container Apps Environment (reuse existing Log Analytics workspace if present)
Write-Host "[3/5] Creating Container Apps Environment..." -ForegroundColor Yellow

$lawId = ""
$existingLaw = az monitor log-analytics workspace list --resource-group $rgName --query "[0].id" -o tsv 2>$null
if ($existingLaw) {
    Write-Host "  Reusing existing Log Analytics workspace" -ForegroundColor DarkGray
    $lawId = $existingLaw
    $lawKey = az monitor log-analytics workspace get-shared-keys --resource-group $rgName --workspace-name (az monitor log-analytics workspace list --resource-group $rgName --query "[0].name" -o tsv) --query primarySharedKey -o tsv
    az containerapp env create `
        --name $envName `
        --resource-group $rgName `
        --location $Location `
        --logs-workspace-id (az monitor log-analytics workspace list --resource-group $rgName --query "[0].customerId" -o tsv) `
        --logs-workspace-key $lawKey `
        --output none
} else {
    az containerapp env create `
        --name $envName `
        --resource-group $rgName `
        --location $Location `
        --output none
}

# 4. Build and push initial image
Write-Host "[4/5] Building and pushing Docker image..." -ForegroundColor Yellow
$imageName = "$acrServer/${appName}:latest"
az acr build --registry $acrName --image "${appName}:latest" --file Dockerfile.prod . --output none

# 5. Create Container App
Write-Host "[5/6] Creating Container App..." -ForegroundColor Yellow

# Read from environment variables, fall back to interactive prompt
function Get-ConfigValue([string]$EnvVar, [string]$Prompt) {
    $val = [Environment]::GetEnvironmentVariable($EnvVar)
    if ($val) {
        Write-Host "  $EnvVar = (from env)" -ForegroundColor DarkGray
        return $val
    }
    return Read-Host "  $Prompt"
}

# Validate required env vars and show summary
$requiredVars = @(
    @{ Name = "AZURE_STORAGE_ACCOUNT"; Desc = "Storage account for tree data" },
    @{ Name = "AZURE_STORAGE_KEY";     Desc = "Storage account key" },
    @{ Name = "AZURE_AD_TENANT_ID";    Desc = "Entra tenant ID" },
    @{ Name = "AZURE_AD_CLIENT_ID";    Desc = "Entra app client ID" },
    @{ Name = "AZURE_AD_CLIENT_SECRET"; Desc = "Entra app client secret" },
    @{ Name = "AZURE_AD_TENANT_NAME";  Desc = "Entra CIAM tenant subdomain" },
    @{ Name = "GOOGLE_CLIENT_ID";      Desc = "Google OAuth client ID" },
    @{ Name = "GOOGLE_CLIENT_SECRET";  Desc = "Google OAuth client secret" }
)

Write-Host ""
Write-Host "Checking required configuration:" -ForegroundColor Cyan
$allSet = $true
foreach ($v in $requiredVars) {
    $val = [Environment]::GetEnvironmentVariable($v.Name)
    if ($val) {
        $masked = if ($v.Name -match "KEY|SECRET") { $val.Substring(0, [Math]::Min(4, $val.Length)) + "***" } else { $val }
        Write-Host "  [OK] $($v.Name) = $masked" -ForegroundColor Green
    } else {
        Write-Host "  [!!] $($v.Name) - $($v.Desc)" -ForegroundColor Red
        $allSet = $false
    }
}

if (-not $allSet) {
    Write-Host ""
    Write-Host "Some variables are not set. You will be prompted for them." -ForegroundColor Yellow
    Write-Host "To avoid prompts, set them before running:" -ForegroundColor DarkGray
    foreach ($v in $requiredVars) {
        if (-not [Environment]::GetEnvironmentVariable($v.Name)) {
            Write-Host "  `$env:$($v.Name) = `"<$($v.Desc)>`"" -ForegroundColor DarkGray
        }
    }
}

Write-Host ""
Write-Host "Reading config:" -ForegroundColor Cyan

$storageAccount = Get-ConfigValue "AZURE_STORAGE_ACCOUNT" "AZURE_STORAGE_ACCOUNT"
$storageKey = Get-ConfigValue "AZURE_STORAGE_KEY" "AZURE_STORAGE_KEY"
$adTenantId = Get-ConfigValue "AZURE_AD_TENANT_ID" "AZURE_AD_TENANT_ID"
$adClientId = Get-ConfigValue "AZURE_AD_CLIENT_ID" "AZURE_AD_CLIENT_ID"
$adClientSecret = Get-ConfigValue "AZURE_AD_CLIENT_SECRET" "AZURE_AD_CLIENT_SECRET"
$adTenantName = Get-ConfigValue "AZURE_AD_TENANT_NAME" "AZURE_AD_TENANT_NAME"
$googleClientId = Get-ConfigValue "GOOGLE_CLIENT_ID" "GOOGLE_CLIENT_ID"
$googleClientSecret = Get-ConfigValue "GOOGLE_CLIENT_SECRET" "GOOGLE_CLIENT_SECRET"

# 6. Configure backup on storage account (blob versioning + soft delete)
if ($storageAccount) {
    Write-Host "`n[6/6] Configuring storage account backup (versioning + soft delete)..." -ForegroundColor Yellow
    # Enable blob versioning
    az storage account blob-service-properties update `
        --account-name $storageAccount `
        --enable-versioning true `
        --output none 2>$null
    # Enable blob soft delete (30-day retention)
    az storage blob service-properties delete-policy update `
        --account-name $storageAccount `
        --account-key $storageKey `
        --enable true `
        --days-retained 30 `
        --output none 2>$null
    # Enable container soft delete (7-day retention)
    az storage account blob-service-properties update `
        --account-name $storageAccount `
        --enable-container-delete-retention true `
        --container-delete-retention-days 7 `
        --output none 2>$null
    Write-Host "  Blob versioning:  enabled" -ForegroundColor DarkGray
    Write-Host "  Blob soft delete: 30 days" -ForegroundColor DarkGray
    Write-Host "  Container soft delete: 7 days" -ForegroundColor DarkGray
}

# Generate a session secret
$sessionSecret = -join ((48..57) + (65..90) + (97..122) | Get-Random -Count 64 | ForEach-Object { [char]$_ })

# Build the app URL (will be available after creation)
az containerapp create `
    --name $appName `
    --resource-group $rgName `
    --environment $envName `
    --image $imageName `
    --registry-server $acrServer `
    --registry-username $acrUser `
    --registry-password $acrPass `
    --target-port 8000 `
    --ingress external `
    --min-replicas 0 `
    --max-replicas 3 `
    --cpu 0.5 `
    --memory 1Gi `
    --secrets `
        "storage-key=$storageKey" `
        "ad-client-secret=$adClientSecret" `
        "session-secret=$sessionSecret" `
        "google-client-secret=$googleClientSecret" `
    --env-vars `
        "TREE_BACKEND=azstorage" `
        "AZURE_STORAGE_ACCOUNT=$storageAccount" `
        "AZURE_STORAGE_KEY=secretref:storage-key" `
        "AZURE_STORAGE_CONTAINER=familytreejson" `
        "AZURE_STORAGE_BLOB=familytree.gml" `
        "AZURE_STORAGE_PICS_CONTAINER=familytreepics" `
        "AZURE_AD_TENANT_ID=$adTenantId" `
        "AZURE_AD_CLIENT_ID=$adClientId" `
        "AZURE_AD_CLIENT_SECRET=secretref:ad-client-secret" `
        "AZURE_AD_TENANT_NAME=$adTenantName" `
        "GOOGLE_CLIENT_ID=$googleClientId" `
        "GOOGLE_CLIENT_SECRET=secretref:google-client-secret" `
        "SESSION_SECRET=secretref:session-secret" `
    --output none

# Get the app URL
$appUrl = az containerapp show --name $appName --resource-group $rgName --query "properties.configuration.ingress.fqdn" -o tsv

Write-Host ""
Write-Host "=== Provisioning Complete ===" -ForegroundColor Green
Write-Host "  App URL:  https://$appUrl"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Set CORS and OAuth redirect:"
Write-Host "     az containerapp update --name $appName --resource-group $rgName \"
Write-Host "       --set-env-vars CORS_ORIGINS=https://$appUrl OAUTH_REDIRECT_URI=https://$appUrl/api/auth/callback"
Write-Host ""
Write-Host "  2. Add redirect URI to Entra app registration:"
Write-Host "     https://$appUrl/api/auth/callback  (Web type)"
Write-Host ""
Write-Host "  3. Deploy updates:  .\deploy\deploy.ps1"
Write-Host "  4. Start/Stop:     .\deploy\start.ps1 / .\deploy\stop.ps1"
