#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Stop the Family Tree Container App by deactivating all revisions.
#>
param(
    [string]$AppName = "familytreeapp",
    [string]$ResourceGroup = ""
)
$rgName = if ($ResourceGroup) { $ResourceGroup } else { "$AppName-rg" }
$appNameAca = "$AppName-app"

Write-Host "Stopping $appNameAca..."
$revisions = az containerapp revision list --name $appNameAca --resource-group $rgName --query "[?properties.active].name" -o tsv
if ($revisions) {
    $revisions -split "`n" | ForEach-Object {
        $rev = $_.Trim()
        if ($rev) {
            az containerapp revision deactivate --name $appNameAca --resource-group $rgName --revision $rev -o none
            Write-Host "  Deactivated: $rev" -ForegroundColor DarkGray
        }
    }
    Write-Host "Stopped. All revisions deactivated." -ForegroundColor Green
} else {
    Write-Host "No active revisions found." -ForegroundColor Yellow
}
Write-Host "To restart: .\deploy\start.ps1"
