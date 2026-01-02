function Reload-Tool {
    Write-Host "`n Reloading Development Environment..." -ForegroundColor Magenta
    
    # De lijst met al jouw bestanden in de juiste volgorde
    $modules = @(
        ".\private\Connection_DB.ps1",
        ".\private\Analyze_scheme.ps1",
        ".\private\Sql_Schema_Generator.ps1",
        ".\private\Data_Migration.ps1",
        ".\private\Sync.ps1",
        ".\private\Migration_Validation.ps1",
        ".\private\Config.ps1",
        ".\public\MasterWorkflow.ps1"
    )

    $errors = $false

    foreach ($module in $modules) {
        if (Test-Path $module) {
            try {
                # De 'dot-sourcing' uitvoeren
                . $module
                Write-Host "Loaded $module" -ForegroundColor DarkGray
            }
            catch {
                Write-Host "Error loading $module : $($_.Exception.Message)" -ForegroundColor Red
                $errors = $true
            }
        }
        else {
            Write-Host "File not found: $module" -ForegroundColor Red
            $errors = $true
        }
    }

    if (-not $errors) {
        Write-Host "Ready! Starting Menu..." -ForegroundColor Green
        Start-Sleep -Milliseconds 500
        # Start direct het menu
        Start_MigrationToolMenu
    }
    else {
        Write-Host "`n Errors detected during reload. Menu not started." -ForegroundColor Yellow
    }
}