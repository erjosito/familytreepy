#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Backup and restore the Family Tree GML data from Azure Blob Storage.

.DESCRIPTION
  - backup:   Downloads the current GML file to a local timestamped backup
  - restore:  Uploads a local GML file to Azure Blob Storage
  - versions: Lists all blob versions (requires versioning enabled)
  - restore-version: Restores a specific blob version as the current version

.EXAMPLE
  .\backup.ps1 backup
  .\backup.ps1 versions
  .\backup.ps1 restore -File backups\familytree_2026-04-10_120000.gml
  .\backup.ps1 restore-version -VersionId "2026-04-10T12:00:00.0000000Z"
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet("backup", "restore", "versions", "restore-version")]
    [string]$Action = "backup",

    [string]$File = "",
    [string]$VersionId = "",
    [string]$BackupDir = "backups"
)

$ErrorActionPreference = "Stop"

# Read config from environment
$account = $env:AZURE_STORAGE_ACCOUNT
$key = $env:AZURE_STORAGE_KEY
$container = if ($env:AZURE_STORAGE_CONTAINER) { $env:AZURE_STORAGE_CONTAINER } else { "familytreejson" }
$blob = if ($env:AZURE_STORAGE_BLOB) { $env:AZURE_STORAGE_BLOB } else { "familytree.gml" }

if (-not $account -or -not $key) {
    Write-Host "Error: Missing required environment variables:" -ForegroundColor Red
    if (-not $account) { Write-Host "  - AZURE_STORAGE_ACCOUNT" -ForegroundColor Red }
    if (-not $key)     { Write-Host "  - AZURE_STORAGE_KEY" -ForegroundColor Red }
    Write-Host ""
    Write-Host "Set them with:" -ForegroundColor Yellow
    if (-not $account) { Write-Host '  $env:AZURE_STORAGE_ACCOUNT = "<storage account name>"' }
    if (-not $key)     { Write-Host '  $env:AZURE_STORAGE_KEY = "<storage account key>"' }
    exit 1
}

switch ($Action) {
    "backup" {
        # Download current GML to local timestamped file
        if (!(Test-Path $BackupDir)) { New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null }
        $timestamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
        $outFile = Join-Path $BackupDir "familytree_$timestamp.gml"

        Write-Host "Downloading ${container}/${blob} to $outFile..."
        az storage blob download `
            --account-name $account `
            --account-key $key `
            --container-name $container `
            --name $blob `
            --file $outFile `
            --output none

        $size = (Get-Item $outFile).Length
        Write-Host "Backup saved: $outFile ($size bytes)" -ForegroundColor Green

        # Clean up old backups (keep last 20)
        $backups = Get-ChildItem $BackupDir -Filter "familytree_*.gml" | Sort-Object Name -Descending
        if ($backups.Count -gt 20) {
            $toDelete = $backups | Select-Object -Skip 20
            $toDelete | ForEach-Object {
                Write-Host "  Removing old backup: $($_.Name)" -ForegroundColor DarkGray
                Remove-Item $_.FullName
            }
        }
    }

    "restore" {
        if (-not $File) {
            # Show available backups
            if (Test-Path $BackupDir) {
                $backups = Get-ChildItem $BackupDir -Filter "familytree_*.gml" | Sort-Object Name -Descending
                if ($backups.Count -gt 0) {
                    Write-Host "Available backups:"
                    $backups | ForEach-Object { Write-Host "  $($_.Name)  ($($_.Length) bytes)" }
                    Write-Host "`nUsage: .\backup.ps1 restore -File backups\<filename>"
                } else {
                    Write-Host "No backups found in $BackupDir"
                }
            } else {
                Write-Host "No backups directory found. Run 'backup' first."
            }
            exit 0
        }

        if (!(Test-Path $File)) {
            Write-Host "Error: File '$File' not found." -ForegroundColor Red
            exit 1
        }

        $confirm = Read-Host "Restore '$File' to ${container}/${blob}? This will overwrite the current tree. [y/N]"
        if ($confirm -ne "y") { Write-Host "Cancelled."; exit 0 }

        Write-Host "Uploading $File to ${container}/${blob}..."
        az storage blob upload `
            --account-name $account `
            --account-key $key `
            --container-name $container `
            --name $blob `
            --file $File `
            --overwrite `
            --output none

        Write-Host "Restored successfully." -ForegroundColor Green
        Write-Host "Restart the app to load the new data."
    }

    "versions" {
        Write-Host "Blob versions for ${container}/${blob}:"
        az storage blob list `
            --account-name $account `
            --account-key $key `
            --container-name $container `
            --prefix $blob `
            --include v `
            --query "[].{name:name, version:versionId, modified:properties.lastModified, size:properties.contentLength}" `
            --output table
    }

    "restore-version" {
        if (-not $VersionId) {
            Write-Host "Error: Specify -VersionId. Use 'versions' to list available versions." -ForegroundColor Red
            exit 1
        }

        $confirm = Read-Host "Promote version '$VersionId' as the current blob? [y/N]"
        if ($confirm -ne "y") { Write-Host "Cancelled."; exit 0 }

        # Copy the versioned blob over the current one
        $sourceUri = "https://$account.blob.core.windows.net/$container/$($blob)?versionid=$VersionId"
        az storage blob copy start `
            --account-name $account `
            --account-key $key `
            --destination-container $container `
            --destination-blob $blob `
            --source-uri $sourceUri `
            --output none

        Write-Host "Version '$VersionId' restored as current." -ForegroundColor Green
        Write-Host "Restart the app to load the restored data."
    }
}
