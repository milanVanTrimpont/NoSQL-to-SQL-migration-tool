function Invoke-MigrationWorkflow {
    <#
    .SYNOPSIS
    Complete migration workflow with support for multiple collections
    
    .DESCRIPTION
    Performs complete migration/sync for one or all MongoDB collections:
    - Schema analysis
    - SQL table generation
    - Data migration
    - Validation
    - Incremental sync
    
    .PARAMETER Collections
    Array of collection names to process. If empty, processes all collections
    
    .PARAMETER Operation
    Type of operation: FullMigration, IncrementalSync, ValidationOnly
    
    .PARAMETER DatabaseType
    Type of SQL database (MySQL or SQLServer)
    
    .PARAMETER SampleSize
    Number of documents to sample for schema analysis
    
    .EXAMPLE
    # Migrate specific collections
    Invoke-MigrationWorkflow -Collections @("klanten", "producten") -Operation FullMigration
    
    .EXAMPLE
    # Sync all collections
    Invoke-MigrationWorkflow -Operation IncrementalSync
    
    .EXAMPLE
    # Validate specific collection
    Invoke-MigrationWorkflow -Collections @("klanten") -Operation ValidationOnly
    #>
    
    param (
        [Parameter(Mandatory=$false)]
        [string[]]$Collections = @(),
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("FullMigration", "IncrementalSync", "ValidationOnly", "SchemaOnly")]
        [string]$Operation = "IncrementalSync",
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL",
        
        [Parameter(Mandatory=$false)]
        [int]$SampleSize = 100
    )
    
    Write-Host "`n" + ("="*70) -ForegroundColor Cyan
    Write-Host "  NoSQL to SQL Migration Tool - Multi-Collection Workflow" -ForegroundColor Cyan
    Write-Host ("="*70) + "`n" -ForegroundColor Cyan
    
    # Load configuration
    $script:AppConfig = Get-AppConfig
    
    # Get collections to process
    if ($Collections.Count -eq 0) {
        Write-Host "Discovering collections..." -ForegroundColor Yellow
        $discoveredCollections = Get-MongoDBCollections
        
        if ($discoveredCollections.Count -eq 0) {
            Write-Host "No collections found in database." -ForegroundColor Red
            return
        }
        
        Write-Host "Found $($discoveredCollections.Count) collection(s): $($discoveredCollections -join ', ')" -ForegroundColor Green
        
        # Ask for confirmation
        $response = Read-Host "`nProcess ALL collections? (Y/N)"
        if ($response -ne 'Y' -and $response -ne 'y') {
            Write-Host "Operation cancelled." -ForegroundColor Yellow
            return
        }
        
        $Collections = $discoveredCollections
    }
    
    Write-Host "`nOperation: $Operation" -ForegroundColor Cyan
    Write-Host "Collections: $($Collections -join ', ')" -ForegroundColor Cyan
    Write-Host "Database Type: $DatabaseType" -ForegroundColor Cyan
    Write-Host ""
    
    # Overall results
    $overallResults = @{
        Operation = $Operation
        StartTime = Get-Date
        Collections = @()
        TotalSuccess = 0
        TotalFailed = 0
    }
    
    # Process each collection
    foreach ($collectionName in $Collections) {
        Write-Host "`n" + ("─"*70) -ForegroundColor Gray
        Write-Host "Processing Collection: $collectionName" -ForegroundColor Yellow
        Write-Host ("─"*70) -ForegroundColor Gray
        
        $collectionResult = @{
            Name = $collectionName
            Success = $false
            Error = $null
            Details = $null
        }
        
        try {
            switch ($Operation) {
                "FullMigration" {
                    $collectionResult.Details = Invoke-FullMigration -CollectionName $collectionName `
                                                                     -DatabaseType $DatabaseType `
                                                                     -SampleSize $SampleSize
                    $collectionResult.Success = $true
                }
                
                "IncrementalSync" {
                    $collectionResult.Details = Invoke-IncrementalMigration -CollectionName $collectionName `
                                                                            -DatabaseType $DatabaseType `
                                                                            -SampleSize $SampleSize
                    $collectionResult.Success = $true
                }
                
                "ValidationOnly" {
                    $collectionResult.Details = Invoke-ValidationOnly -CollectionName $collectionName `
                                                                      -DatabaseType $DatabaseType `
                                                                      -SampleSize $SampleSize
                    $collectionResult.Success = $true
                }
                
                "SchemaOnly" {
                    $collectionResult.Details = Invoke-SchemaOnly -CollectionName $collectionName `
                                                                  -SampleSize $SampleSize
                    $collectionResult.Success = $true
                }
            }
            
            $overallResults.TotalSuccess++
            Write-Host " $collectionName completed successfully" -ForegroundColor Green
        }
        catch {
            $collectionResult.Error = $_.Exception.Message
            $overallResults.TotalFailed++
            Write-Host " $collectionName failed: $($_.Exception.Message)" -ForegroundColor Red
        }
        
        $overallResults.Collections += $collectionResult
    }
    
    # Display overall summary
    $overallResults.EndTime = Get-Date
    $duration = $overallResults.EndTime - $overallResults.StartTime
    
    Write-Host "`n" + ("="*70) -ForegroundColor Cyan
    Write-Host "Overall Summary" -ForegroundColor Cyan
    Write-Host ("="*70) -ForegroundColor Cyan
    Write-Host "Duration: $($duration.TotalSeconds) seconds" -ForegroundColor Gray
    Write-Host "Collections Processed: $($Collections.Count)" -ForegroundColor Gray
    Write-Host "Successful: $($overallResults.TotalSuccess)" -ForegroundColor Green
    Write-Host "Failed: $($overallResults.TotalFailed)" -ForegroundColor $(if ($overallResults.TotalFailed -gt 0) { 'Red' } else { 'Gray' })
    
    Write-Host "`nCollection Results:" -ForegroundColor Yellow
    foreach ($result in $overallResults.Collections) {
        $status = if ($result.Success) { "" } else { "" }
        $color = if ($result.Success) { "Green" } else { "Red" }
        Write-Host "  $status $($result.Name)" -ForegroundColor $color
        
        if ($result.Error) {
            Write-Host "    Error: $($result.Error)" -ForegroundColor Red
        }
    }
    
    Write-Host ("="*70) + "`n" -ForegroundColor Cyan
    
    # Export overall report
    $reportFile = ".\workflow_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').json"
    $overallResults | ConvertTo-Json -Depth 10 | Out-File -FilePath $reportFile -Encoding UTF8
    Write-Host "Workflow report exported to: $reportFile`n" -ForegroundColor Green
    
    return $overallResults
}

function Get-MongoDBCollections {
    try {
        # Get config if not already loaded
        if (-not $script:AppConfig) {
            $script:AppConfig = Get-AppConfig
        }
        
        Connect-Mdbc -ConnectionString $script:AppConfig.MongoDB.ConnectionString `
                     -DatabaseName $script:AppConfig.MongoDB.Database

        # Gebruik listCollections command (Mdbc-native)
        $result = Invoke-MdbcCommand -Command @{
            listCollections = 1
        }

        $collectionNames = @()

        foreach ($col in $result.cursor.firstBatch) {
            $name = $col.name
            if ($name -and $name -notlike "system.*") {
                Write-Host "  Found collection: $name" -ForegroundColor Gray
                $collectionNames += $name
            }
        }

        if ($collectionNames.Count -eq 0) {
            Write-Host "  Warning: No collections found in database $($script:AppConfig.MongoDB.Database)" -ForegroundColor Yellow
        }

        return $collectionNames
    }
    catch {
        Write-Host "Error retrieving collections: $($_.Exception.Message)" -ForegroundColor Red
        return @()
    }
}




function Invoke-FullMigration {
    <#
    .SYNOPSIS
    Performs full migration for a single collection
    #>
    
    param (
        [string]$CollectionName,
        [string]$DatabaseType,
        [int]$SampleSize
    )
    
    $result = @{
        Schema = $null
        SQLSchema = $null
        Migration = $null
        Validation = $null
    }
    
    try {
        # Step 1: Analyze schema
        Write-Host "\n[1/4] Analyzing MongoDB schema..." -ForegroundColor Cyan
        $result.Schema = Get-MongoDBSchema -ConnectionString $script:AppConfig.MongoDB.ConnectionString `
                                          -DatabaseName $script:AppConfig.MongoDB.Database `
                                          -CollectionName $CollectionName `
                                          -SampleSize $SampleSize
        
        # Step 2: Generate SQL schema
        Write-Host "`n[2/4] Generating SQL schema..." -ForegroundColor Cyan
        $result.SQLSchema = New-SQLSchema -Schema $result.Schema `
                                         -TableName $CollectionName `
                                         -PrimaryKeyField "_id"
        
        Export-SQLSchema -SchemaResult $result.SQLSchema `
                        -OutputPath ".\schema_$CollectionName.sql" | Out-Null
        
        # Step 3: Migrate data
        Write-Host "`n[3/4] Migrating data..." -ForegroundColor Cyan
        $result.Migration = Start-DataMigration -Schema $result.Schema `
                                               -SQLSchema $result.SQLSchema `
                                               -CollectionName $CollectionName `
                                               -BatchSize 100 `
                                               -DatabaseType $DatabaseType
        
        # Step 4: Validate
        Write-Host "`n[4/4] Validating migration..." -ForegroundColor Cyan
        $result.Validation = Test-MigrationValidation -TableName $CollectionName `
                                                      -SampleSize 10 `
                                                      -DatabaseType $DatabaseType
        
        return $result
    }
    catch {
        Write-Host "Error in full migration: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

function Invoke-IncrementalMigration {
    <#
    .SYNOPSIS
    Performs incremental migration for a single collection
    #>
    
    param (
        [string]$CollectionName,
        [string]$DatabaseType,
        [int]$SampleSize
    )
    
    try {
        # Check if table exists, if not do full migration
        $sqlConnection = Get-SQLConnectionObject -DatabaseType $DatabaseType
        $sqlConnection.Open()
        
        try {
            $cmd = $sqlConnection.CreateCommand()
            $cmd.CommandText = "SELECT COUNT(*) FROM ``" + $CollectionName + "``"
            $cmd.ExecuteScalar() | Out-Null
            $tableExists = $true
        }
        catch {
            $tableExists = $false
        }
        finally {
            $sqlConnection.Close()
        }
        
        if (-not $tableExists) {
            Write-Host "Table doesn't exist, performing full migration..." -ForegroundColor Yellow
            return Invoke-FullMigration -CollectionName $CollectionName `
                                       -DatabaseType $DatabaseType `
                                       -SampleSize $SampleSize
        }
        
        # Perform incremental sync
        $syncResult = Start-IncrementalSync -TableName $CollectionName `
                                           -DatabaseType $DatabaseType
        
        return @{
            Sync = $syncResult
        }
    }
    catch {
        Write-Host "Error in incremental migration: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

function Invoke-ValidationOnly {
    <#
    .SYNOPSIS
    Performs validation only for a single collection
    #>
    
    param (
        [string]$CollectionName,
        [string]$DatabaseType,
        [int]$SampleSize
    )
    
    try {
        # Pass the collection name as TableName parameter
        $validation = Test-MigrationValidation -TableName $CollectionName `
                                              -SampleSize $SampleSize `
                                              -DatabaseType $DatabaseType
        
        # Export validation report
        $reportPath = ".\validation_$CollectionName`_$(Get-Date -Format 'yyyyMMdd_HHmmss').html"
        Export-ValidationReport -ValidationResult $validation `
                               -OutputPath $reportPath | Out-Null
        
        return @{
            Validation = $validation
        }
    }
    catch {
        Write-Host "Error in validation: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

function Invoke-SchemaOnly {
    <#
    .SYNOPSIS
    Performs schema analysis only
    #>
    
    param (
        [string]$CollectionName,
        [int]$SampleSize
    )
    
    try {
        $schema = Get-MongoDBSchema -ConnectionString $script:AppConfig.MongoDB.ConnectionString `
                                   -DatabaseName $script:AppConfig.MongoDB.Database `
                                   -CollectionName $CollectionName `
                                   -SampleSize $SampleSize
        
        $sqlSchema = New-SQLSchema -Schema $schema `
                                  -TableName $CollectionName `
                                  -PrimaryKeyField "_id"
        
        Export-SQLSchema -SchemaResult $sqlSchema `
                        -OutputPath ".\schema_$CollectionName.sql" | Out-Null
        
        return @{
            Schema = $schema
            SQLSchema = $sqlSchema
        }
    }
    catch {
        Write-Host "Error in schema analysis: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

# Quick helper functions for common operations

function Sync-AllCollections {
    <#
    .SYNOPSIS
    Quick function to sync all collections
    #>
    
    param (
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL"
    )
    
    Invoke-MigrationWorkflow -Operation IncrementalSync -DatabaseType $DatabaseType
}

function Migrate-Collection {
    <#
    .SYNOPSIS
    Quick function to migrate a specific collection
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [string]$CollectionName,
        
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL",
        
        [switch]$FullMigration
    )
    
    $operation = if ($FullMigration) { "FullMigration" } else { "IncrementalSync" }
    
    Invoke-MigrationWorkflow -Collections @($CollectionName) `
                            -Operation $operation `
                            -DatabaseType $DatabaseType
}

function Validate-Collection {
    <#
    .SYNOPSIS
    Quick function to validate a specific collection
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [string]$CollectionName,
        
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL"
    )
    
    Invoke-MigrationWorkflow -Collections @($CollectionName) `
                            -Operation ValidationOnly `
                            -DatabaseType $DatabaseType
}