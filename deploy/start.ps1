#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Start the Family Tree Container App by activating the latest revision.
#>
param(
    [string]$AppName = "familytreeapp",
    [string]$ResourceGroup = ""
)
$rgName = if ($ResourceGroup) { $ResourceGroup } else { "$AppName-rg" }
$appNameAca = "$AppName-app"

Write-Host "Starting $appNameAca..."

# Find the latest revision (active or not)
$revision = az containerapp revision list --name $appNameAca --resource-group $rgName --query "sort_by([], &properties.createdTime) | [-1].name" -o tsv
if ($revision) {
    az containerapp revision activate --name $appNameAca --resource-group $rgName --revision $revision -o none 2>&1
    az containerapp revision restart --name $appNameAca --resource-group $rgName --revision $revision -o none 2>&1
    Write-Host "Activated and restarted: $revision" -ForegroundColor Green
} else {
    Write-Host "No revisions found. Run deploy.ps1 first." -ForegroundColor Yellow
}
