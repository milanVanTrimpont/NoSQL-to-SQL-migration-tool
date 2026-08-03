<#
.SYNOPSIS
Starts the interactive menu of the NoSQL to SQL Migration Tool.

.DESCRIPTION
A launcher: it loads the module and opens the menu. The menu itself lives in the
module, so there is only one version of it. This file used to contain a copy of
every menu function, and because those copies shadowed the module, a change in
the module never reached the screen.

For automated runs without a menu, use Start-Migration.ps1 instead.

.EXAMPLE
pwsh -File .\InteractiveMenu.ps1
#>

[CmdletBinding()]
param ()

Write-Host "`n"
Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║          NoSQL to SQL Migration Tool v1.0                  ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

$modulePath = Join-Path $PSScriptRoot "NoSqlToSqlMigration\NoSqlToSqlMigration.psd1"

if (-not (Test-Path $modulePath)) {
    Write-Host " Module not found at: $modulePath" -ForegroundColor Red
    Write-Host " Run this script from the folder that contains NoSqlToSqlMigration." -ForegroundColor Red
    exit 2
}

try {
    Import-Module $modulePath -Force -ErrorAction Stop
    Write-Host " Module loaded" -ForegroundColor Green
}
catch {
    Write-Host " Failed to load the module: $($_.Exception.Message)" -ForegroundColor Red
    exit 2
}

if (-not (Test-Path (Join-Path $PSScriptRoot "config.json"))) {
    Write-Host " No config.json found next to this script." -ForegroundColor Red
    Write-Host " Copy config.example.json to config.json and fill in your own servers." -ForegroundColor Yellow
    exit 2
}

Start-Sleep -Seconds 1
Start-MigrationToolMenu
