

Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "    NoSQL to SQL Migration Tool v1.0" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan

# Check if Mdbc module is available
if (-not (Get-Module -ListAvailable -Name Mdbc)) {
    Write-Host "Mdbc module not found!" -ForegroundColor Red
    Write-Host "Install with: Install-Module -Name Mdbc -Scope CurrentUser" -ForegroundColor Yellow
    exit 1
}

# Import modules (in the correct order!)
Write-Host "Loading modules..." -ForegroundColor Gray
# Note: All functions are loaded by the module, no need to dot-source here
Write-Host "Modules successfully loaded`n" -ForegroundColor Green

# Test database connections
$connectionsOk = Initialize-DatabaseConnections -EnvFilePath ".\.env"

if (-not $connectionsOk) {
    Write-Host "`n Please resolve the above connection issues before proceeding." -ForegroundColor Yellow
    exit 1
}
