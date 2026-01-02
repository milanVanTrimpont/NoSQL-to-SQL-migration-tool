function Start-MigrationToolMenu {
    <#
    .SYNOPSIS
    Interactive menu for NoSQL to SQL Migration Tool
    
    .DESCRIPTION
    Provides a user-friendly interactive interface for all migration operations
    #>
    
    # Load configuration
    try {
        $script:AppConfig = Get-AppConfig
    }
    catch {
        Write-Host "`n Configuration error" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Yellow
        return
    }

    $continue = $true
    
    while ($continue) {
        Show-MainMenu
        
        $choice = Read-Host "`n Enter your choice"
        
        switch ($choice) {
            "1" { Menu-TestConnections }
            "2" { Menu-DiscoverCollections }
            "3" { Menu-MigrateSingle }
            "4" { Menu-MigrateMultiple }
            "5" { Menu-MigrateAll }
            "6" { Menu-SyncSingle }
            "7" { Menu-SyncAll }
            "8" { Menu-ValidateSingle }
            "9" { Menu-SchemaOnly }
            "0" { 
                Write-Host "`n Thank you for using NoSQL to SQL Migration Tool!" -ForegroundColor Cyan
                $continue = $false 
            }
            default { 
                Write-Host "`n Invalid choice. Please try again." -ForegroundColor Red
                Start-Sleep -Seconds 1
            }
        }
        
        if ($continue -and $choice -ne "0") {
            Write-Host "`nPress any key to continue..." -ForegroundColor Gray
            $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
        }
    }
}

function Show-MainMenu {
    Clear-Host
    
    Write-Host "`n"
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "║                                                            ║" -ForegroundColor Cyan
    Write-Host "║       NoSQL to SQL Migration Tool - Main Menu             ║" -ForegroundColor Cyan
    Write-Host "║                                                            ║" -ForegroundColor Cyan
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Database: " -NoNewline -ForegroundColor Gray
    Write-Host "$($AppConfig.MongoDB.Database) → $($AppConfig.MySQL.Database)" -ForegroundColor White
    Write-Host ""
    Write-Host "┌────────────────────────────────────────────────────────────┐" -ForegroundColor DarkGray
    Write-Host "│  SETUP & DISCOVERY                                         │" -ForegroundColor Yellow
    Write-Host "├────────────────────────────────────────────────────────────┤" -ForegroundColor DarkGray
    Write-Host "│  [1] Test Database Connections                             │" -ForegroundColor White
    Write-Host "│  [2] Discover MongoDB Collections                          │" -ForegroundColor White
    Write-Host "│                                                            │" -ForegroundColor DarkGray
    Write-Host "│  MIGRATION                                                 │" -ForegroundColor Yellow
    Write-Host "├────────────────────────────────────────────────────────────┤" -ForegroundColor DarkGray
    Write-Host "│  [3] Migrate Single Collection (Full)                      │" -ForegroundColor White
    Write-Host "│  [4] Migrate Multiple Collections                          │" -ForegroundColor White
    Write-Host "│  [5] Migrate ALL Collections                               │" -ForegroundColor White
    Write-Host "│                                                            │" -ForegroundColor DarkGray
    Write-Host "│  SYNCHRONIZATION                                           │" -ForegroundColor Yellow
    Write-Host "├────────────────────────────────────────────────────────────┤" -ForegroundColor DarkGray
    Write-Host "│  [6] Sync Single Collection (Incremental)                  │" -ForegroundColor White
    Write-Host "│  [7] Sync ALL Collections                                  │" -ForegroundColor White
    Write-Host "│                                                            │" -ForegroundColor DarkGray
    Write-Host "│  VALIDATION & ANALYSIS                                     │" -ForegroundColor Yellow
    Write-Host "├────────────────────────────────────────────────────────────┤" -ForegroundColor DarkGray
    Write-Host "│  [8] Validate Single Collection                            │" -ForegroundColor White
    Write-Host "│  [9] Analyze Schema Only                                   │" -ForegroundColor White
    Write-Host "│                                                            │" -ForegroundColor DarkGray
    Write-Host "│  [0] Exit                                                  │" -ForegroundColor Red
    Write-Host "└────────────────────────────────────────────────────────────┘" -ForegroundColor DarkGray
}

function Menu-TestConnections {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Testing Database Connections" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $result = Initialize-DatabaseConnections -DatabaseType "MySQL"
    
    if ($result) {
        Write-Host "`n All connections successful!" -ForegroundColor Green
    }
    else {
        Write-Host "`n Connection test failed. Please check your configuration." -ForegroundColor Red
    }
}

function Menu-DiscoverCollections {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Discovering MongoDB Collections" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    Write-Host "`nScanning database..." -ForegroundColor Yellow
    
    $collections = Get-MongoDBCollections
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`n Found $($collections.Count) collection(s):" -ForegroundColor Green
    Write-Host ""
    
    foreach ($collection in $collections) {
        # Get document count
        try {
            Connect-Mdbc -ConnectionString $AppConfig.MongoDB.ConnectionString `
                         -DatabaseName $AppConfig.MongoDB.Database `
                         -CollectionName $collection
            
            $count = Get-MdbcData -Count
            Write-Host "  • " -NoNewline -ForegroundColor Cyan
            Write-Host "$collection " -NoNewline -ForegroundColor White
            Write-Host "($count documents)" -ForegroundColor Gray
        }
        catch {
            Write-Host "  • $collection (error reading count)" -ForegroundColor Yellow
        }
    }
}

function Menu-MigrateSingle {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Migrate Single Collection" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $collections = Get-MongoDBCollections
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`nAvailable collections:" -ForegroundColor Yellow
    for ($i = 0; $i -lt $collections.Count; $i++) {
        Write-Host "  [$($i+1)] $($collections[$i])" -ForegroundColor White
    }
    
    $choice = Read-Host "`nEnter collection number"
    $index = [int]$choice - 1
    
    if ($index -ge 0 -and $index -lt $collections.Count) {
        $collectionName = $collections[$index]
        
        Write-Host "`nYou selected: " -NoNewline -ForegroundColor Gray
        Write-Host $collectionName -ForegroundColor White
        
        $confirm = Read-Host "`nThis will perform a FULL MIGRATION (Schema + Data). Continue? (Y/N)"
        
        if ($confirm -eq 'Y' -or $confirm -eq 'y') {
            Migrate-Collection -CollectionName $collectionName -FullMigration
        }
        else {
            Write-Host "`nOperation cancelled." -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "`n Invalid selection." -ForegroundColor Red
    }
}

function Menu-MigrateMultiple {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Migrate Multiple Collections" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $collections = Get-MongoDBCollections
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`n Available collections:" -ForegroundColor Yellow
    for ($i = 0; $i -lt $collections.Count; $i++) {
        Write-Host "  [$($i+1)] $($collections[$i])" -ForegroundColor White
    }
    
    Write-Host "`n Enter collection numbers separated by commas (e.g., 1,3,4)" -ForegroundColor Gray
    $input = Read-Host "Selection"
    
    $indices = $input -split ',' | ForEach-Object { [int]$_.Trim() - 1 }
    $selectedCollections = @()
    
    foreach ($index in $indices) {
        if ($index -ge 0 -and $index -lt $collections.Count) {
            $selectedCollections += $collections[$index]
        }
    }
    
    if ($selectedCollections.Count -eq 0) {
        Write-Host "`n No valid collections selected." -ForegroundColor Red
        return
    }
    
    Write-Host "`nSelected collections:" -ForegroundColor Yellow
    foreach ($col in $selectedCollections) {
        Write-Host "  • $col" -ForegroundColor White
    }
    
    $confirm = Read-Host "`nMigrate these collections? (Y/N)"
    
    if ($confirm -eq 'Y' -or $confirm -eq 'y') {
        Invoke-MigrationWorkflow -Collections $selectedCollections -Operation FullMigration
    }
    else {
        Write-Host "`nOperation cancelled." -ForegroundColor Yellow
    }
}

function Menu-MigrateAll {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Migrate ALL Collections" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $collections = Get-MongoDBCollections
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`nThis will migrate ALL $($collections.Count) collection(s):" -ForegroundColor Yellow
    foreach ($col in $collections) {
        Write-Host "  • $col" -ForegroundColor White
    }
    
    Write-Host "`n WARNING: This is a FULL MIGRATION (may take time)" -ForegroundColor Red
    $confirm = Read-Host "`nAre you sure? (Y/N)"
    
    if ($confirm -eq 'Y' -or $confirm -eq 'y') {
        Invoke-MigrationWorkflow -Collections $collections -Operation FullMigration
    }
    else {
        Write-Host "`nOperation cancelled." -ForegroundColor Yellow
    }
}

function Menu-SyncSingle {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Sync Single Collection (Incremental)" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $collections = Get-MongoDBCollections
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`nAvailable collections:" -ForegroundColor Yellow
    for ($i = 0; $i -lt $collections.Count; $i++) {
        Write-Host "  [$($i+1)] $($collections[$i])" -ForegroundColor White
    }
    
    $choice = Read-Host "`nEnter collection number"
    $index = [int]$choice - 1
    
    if ($index -ge 0 -and $index -lt $collections.Count) {
        $collectionName = $collections[$index]
        
        Write-Host "`nSyncing: " -NoNewline -ForegroundColor Gray
        Write-Host $collectionName -ForegroundColor White
        Write-Host "This will sync only NEW/UPDATED/DELETED records (fast!)" -ForegroundColor Gray
        
        Migrate-Collection -CollectionName $collectionName
    }
    else {
        Write-Host "`n Invalid selection." -ForegroundColor Red
    }
}

function Menu-SyncAll {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Sync ALL Collections" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $collections = Get-MongoDBCollections
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`nThis will sync ALL $($collections.Count) collection(s):" -ForegroundColor Yellow
    foreach ($col in $collections) {
        Write-Host "  • $col" -ForegroundColor White
    }
    
    Write-Host "`nThis is an INCREMENTAL sync (only changes will be processed)" -ForegroundColor Gray
    $confirm = Read-Host "`nContinue? (Y/N)"
    
    if ($confirm -eq 'Y' -or $confirm -eq 'y') {
        Sync-AllCollections
    }
    else {
        Write-Host "`nOperation cancelled." -ForegroundColor Yellow
    }
}

function Menu-ValidateSingle {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Validate Single Collection" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $collections = Get-MongoDBCollections
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`nAvailable collections:" -ForegroundColor Yellow
    for ($i = 0; $i -lt $collections.Count; $i++) {
        Write-Host "  [$($i+1)] $($collections[$i])" -ForegroundColor White
    }
    
    $choice = Read-Host "`nEnter collection number"
    $index = [int]$choice - 1
    
    if ($index -ge 0 -and $index -lt $collections.Count) {
        $collectionName = $collections[$index]
        
        Write-Host "`nValidating: " -NoNewline -ForegroundColor Gray
        Write-Host $collectionName -ForegroundColor White
        
        $sampleSize = Read-Host "Enter sample size (default: 10)"
        if ([string]::IsNullOrWhiteSpace($sampleSize)) {
            $sampleSize = 10
        }
        
        Validate-Collection -CollectionName $collectionName
    }
    else {
        Write-Host "`n Invalid selection." -ForegroundColor Red
    }
}

function Menu-SchemaOnly {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Analyze Schema Only" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $collections = Get-MongoDBCollections
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`nAvailable collections:" -ForegroundColor Yellow
    for ($i = 0; $i -lt $collections.Count; $i++) {
        Write-Host "  [$($i+1)] $($collections[$i])" -ForegroundColor White
    }
    
    $choice = Read-Host "`nEnter collection number"
    $index = [int]$choice - 1
    
    if ($index -ge 0 -and $index -lt $collections.Count) {
        $collectionName = $collections[$index]
        
        Write-Host "`nAnalyzing schema for: " -NoNewline -ForegroundColor Gray
        Write-Host $collectionName -ForegroundColor White
        
        Invoke-MigrationWorkflow -Collections @($collectionName) -Operation SchemaOnly
    }
    else {
        Write-Host "`n Invalid selection." -ForegroundColor Red
    }
}

# Quick start function
function Start-MigrationTool {
    <#
    .SYNOPSIS
    Quick start function with automatic setup
    #>
    
    Write-Host "`n"
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "║                                                            ║" -ForegroundColor Cyan
    Write-Host "║          NoSQL to SQL Migration Tool v1.0                 ║" -ForegroundColor Cyan
    Write-Host "║                                                            ║" -ForegroundColor Cyan
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Loading module..." -ForegroundColor Yellow
    
    # Import the module
    $modulePath = Join-Path $PSScriptRoot "NoSqlToSqlMigration\NoSqlToSqlMigration.psd1"
    
    if (-not (Test-Path $modulePath)) {
        Write-Host " Module not found at: $modulePath" -ForegroundColor Red
        Write-Host " Please ensure the NoSqlToSqlMigration module is present." -ForegroundColor Red
        return
    }
    
    try {
        Import-Module $modulePath -Force -ErrorAction Stop
        Write-Host " Module loaded successfully!" -ForegroundColor Green
        Start-Sleep -Seconds 1
    }
    catch {
        Write-Host " Failed to load module: $($_.Exception.Message)" -ForegroundColor Red
        return
    }
    
    # Start interactive menu
    Start-MigrationToolMenu
}

# Run the tool when script is executed
Start-MigrationTool