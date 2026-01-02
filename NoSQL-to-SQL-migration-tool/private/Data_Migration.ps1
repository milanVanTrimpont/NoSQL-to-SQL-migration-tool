function Start-DataMigration {
    <#
    .SYNOPSIS
    Migrates data from MongoDB to MySQL database
    
    .DESCRIPTION
    This function performs a complete data migration from MongoDB to MySQL:
    - Creates SQL tables based on schema
    - Transforms MongoDB documents to SQL rows
    - Handles nested objects and arrays
    - Provides progress tracking and error handling
    
    .PARAMETER Schema
    The schema hashtable from Get-MongoDBSchema
    
    .PARAMETER SQLSchema
    The SQL schema result from New-SQLSchema
    
    .PARAMETER BatchSize
    Number of documents to process in each batch (default: 100)
    
    .PARAMETER DatabaseType
    Type of SQL database (MySQL or SQLServer)
    
    .EXAMPLE
    Start-DataMigration -Schema $schema -SQLSchema $sqlSchema -BatchSize 50
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [hashtable]$Schema,
        
        [Parameter(Mandatory=$true)]
        $SQLSchema,
        
        [Parameter(Mandatory=$true)]
        [string]$CollectionName,
        
        [Parameter(Mandatory=$false)]
        [int]$BatchSize = 100,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL"
    )
    
    Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "    Data Migration - MongoDB to $DatabaseType" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
    
    # Initialize migration tracking
    $migrationResult = @{
        TotalDocuments = 0
        MigratedDocuments = 0
        FailedDocuments = 0
        Errors = @()
        StartTime = Get-Date
        TablesCreated = @()
        RecordsInserted = @{}
    }
    
    try {
        # Step 1: Connect to databases
        Write-Host "Step 1: Establishing connections..." -ForegroundColor Yellow
        
        # Get configuration
        $config = Get-AppConfig
        
        # Connect to MongoDB
        Connect-Mdbc -ConnectionString $config.MongoDB.ConnectionString `
                     -DatabaseName $config.MongoDB.Database `
                     -CollectionName $CollectionName
        
        $totalDocs = Get-MdbcData -Count
        $migrationResult.TotalDocuments = $totalDocs
        Write-Host " MongoDB connected: $totalDocs documents found" -ForegroundColor Green
        
        # Connect to SQL
        $sqlConnection = Get-SQLConnectionObject -DatabaseType $DatabaseType
        $sqlConnection.Open()
        Write-Host " $DatabaseType connected" -ForegroundColor Green
        
        # Step 2: Create tables
        Write-Host "`nStep 2: Creating tables..." -ForegroundColor Yellow
        foreach ($statement in $SQLSchema.Statements) {
            try {
                $cmd = $sqlConnection.CreateCommand()
                
                # Convert SQL Server syntax to MySQL if needed
                if ($DatabaseType -eq "MySQL") {
                    $statement = Convert-ToMySQLSyntax -SQLStatement $statement
                }
                
                $cmd.CommandText = $statement
                $cmd.ExecuteNonQuery() | Out-Null
                
                # Extract table name from statement
                if ($statement -match "CREATE TABLE [`\[]?(\w+)[`\]]?") {
                    $tableName = $matches[1]
                    $migrationResult.TablesCreated += $tableName
                    $migrationResult.RecordsInserted[$tableName] = 0
                    Write-Host " Created table: $tableName" -ForegroundColor Green
                }
            }
            catch {
                Write-Host "⚠ Table creation warning: $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }
        
        # Step 3: Migrate data
        Write-Host "`nStep 3: Migrating data..." -ForegroundColor Yellow
        Write-Host "Processing $totalDocs documents in batches of $BatchSize..." -ForegroundColor Gray
        
        $processedCount = 0
        $batchNumber = 0
        
        while ($processedCount -lt $totalDocs) {
            $batchNumber++
            $documents = Get-MdbcData -Skip $processedCount -First $BatchSize
            
            foreach ($doc in $documents) {
                $processedCount++
                
                # Update progress
                $percentComplete = [math]::Round(($processedCount / $totalDocs) * 100, 1)
                Write-Progress -Activity "Migrating documents" `
                              -Status "Document $processedCount of $totalDocs ($percentComplete%)" `
                              -PercentComplete $percentComplete
                
                try {
                    # Migrate main document
                    $success = Invoke-DocumentMigration -Document $doc `
                                                        -Connection $sqlConnection `
                                                        -TableName $SQLSchema.MainTable `
                                                        -Schema $Schema `
                                                        -DatabaseType $DatabaseType
                    
                    if ($success) {
                        $migrationResult.MigratedDocuments++
                        $migrationResult.RecordsInserted[$SQLSchema.MainTable]++
                    }
                    else {
                        $migrationResult.FailedDocuments++
                    }
                }
                catch {
                    $migrationResult.FailedDocuments++
                    $migrationResult.Errors += @{
                        Document = $doc._id
                        Error = $_.Exception.Message
                        Timestamp = Get-Date
                    }
                    Write-Host " Failed to migrate document: $($doc._id)" -ForegroundColor Red
                }
            }
            
            Write-Host " Batch $batchNumber complete: $processedCount/$totalDocs documents processed" -ForegroundColor Gray
        }
        
        Write-Progress -Activity "Migrating documents" -Completed
        
        # Step 4: Summary
        $migrationResult.EndTime = Get-Date
        $duration = $migrationResult.EndTime - $migrationResult.StartTime
        
        Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "Migration Complete!" -ForegroundColor Green
        Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "Duration: $($duration.TotalSeconds) seconds" -ForegroundColor Gray
        Write-Host "Total documents: $($migrationResult.TotalDocuments)" -ForegroundColor Gray
        Write-Host "Successfully migrated: $($migrationResult.MigratedDocuments)" -ForegroundColor Green
        Write-Host "Failed: $($migrationResult.FailedDocuments)" -ForegroundColor $(if ($migrationResult.FailedDocuments -gt 0) { "Red" } else { "Gray" })
        
        Write-Host "`nRecords per table:" -ForegroundColor Yellow
        foreach ($table in $migrationResult.RecordsInserted.Keys | Sort-Object) {
            Write-Host "  $table : $($migrationResult.RecordsInserted[$table])" -ForegroundColor Gray
        }
        
        if ($migrationResult.Errors.Count -gt 0) {
            Write-Host "`nErrors encountered:" -ForegroundColor Red
            $migrationResult.Errors | ForEach-Object {
                Write-Host "  Document $($_.Document): $($_.Error)" -ForegroundColor Red
            }
        }
        
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
        
        return $migrationResult
    }
    catch {
        Write-Host "`n Migration failed: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
    finally {
        # Cleanup
        if ($sqlConnection -and $sqlConnection.State -eq 'Open') {
            $sqlConnection.Close()
        }
    }
}

function Invoke-DocumentMigration {
    <#
    .SYNOPSIS
    Migrates a single MongoDB document to SQL
    #>
    
    param (
        $Document,
        $Connection,
        [string]$TableName,
        [hashtable]$Schema,
        [string]$DatabaseType
    )
    
    try {
        # Extract flat fields only (no nested or arrays)
        $flatFields = @{}
        
        if ($Document -is [System.Collections.IDictionary]) {
            foreach ($key in $Document.Keys) {
                $value = $Document[$key]
                
                # Skip nested objects and arrays for main table
                if ($value -isnot [System.Collections.IEnumerable] -or $value -is [string]) {
                    if ($value -isnot [PSCustomObject] -and $value -isnot [System.Collections.Hashtable]) {
                        $flatFields[$key] = $value
                    }
                }
            }
        }
        
        # Build INSERT statement
        $columns = @()
        $values  = @()
        
        foreach ($field in $flatFields.Keys) {
        # Gebruik twee backticks om één backtick in de tekst te krijgen
        # En géén backtick voor de $field, want die variabele moet hij juist wel lezen
        $columns += "``$field``" 
        
        $values += "?"
        }
        
        # Build INSERT SQL with backticked table name
        # Use REPLACE INTO instead of INSERT INTO to handle duplicates
        $insertSQL = "REPLACE INTO " + ('`' + $TableName + '`') + " (" + ($columns -join ', ') + ") VALUES (" + ($values -join ', ') + ")"
        
        # Create command
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = $insertSQL
        
        # Add parameters for MySQL
        foreach ($field in $flatFields.Keys) {
            $value = $flatFields[$field]
            
            # Convert value to appropriate SQL type
            $sqlValue = Convert-ToSQLValue -Value $value -DatabaseType $DatabaseType
            
            # For MySQL, use CreateParameter
            $param = $cmd.CreateParameter()
            $param.Value = $sqlValue
            $cmd.Parameters.Add($param) | Out-Null
        }
        
        # Execute
        $cmd.ExecuteNonQuery() | Out-Null
        
        return $true
    }
    catch {
        Write-Host "Error migrating document: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Convert-ToSQLValue {
    <#
    .SYNOPSIS
    Converts MongoDB values to SQL-compatible values
    #>
    
    param (
        $Value,
        [string]$DatabaseType
    )
    
    if ($null -eq $Value) {
        return [DBNull]::Value
    }
    
    # Handle ObjectId
    if ($Value.GetType().Name -eq "ObjectId") {
        return $Value.ToString()
    }
    
    # Handle MongoDB BsonDocument types
    if ($Value.GetType().FullName -like "*Bson*") {
        return $Value.ToString()
    }
    
    # Handle DateTime
    if ($Value -is [DateTime]) {
        return $Value
    }
    
    # Handle Boolean
    if ($Value -is [bool]) {
        if ($DatabaseType -eq "MySQL") {
            return if ($Value) { 1 } else { 0 }
        }
        return $Value
    }
    
    # Handle numbers
    if ($Value -is [int] -or $Value -is [long] -or $Value -is [double] -or $Value -is [decimal]) {
        return $Value
    }
    
    # Everything else as string
    return $Value.ToString()
}

function Convert-ToMySQLSyntax {
    <#
    .SYNOPSIS
    Converts SQL Server syntax to MySQL syntax
    #>
    
    param (
        [string]$SQLStatement
    )
    
    # Remove SQL Server specific syntax
    $mysqlStatement = $SQLStatement
    
    # Remove IF OBJECT_ID checks
    $mysqlStatement = $mysqlStatement -replace "IF OBJECT_ID\('[^']+',\s*'U'\)\s*IS NOT NULL\s*", ""
    $mysqlStatement = $mysqlStatement -replace "DROP TABLE [^;]+;", ""
    
    # Replace square brackets with backticks
    $mysqlStatement = $mysqlStatement -replace '\[', '`'
    $mysqlStatement = $mysqlStatement -replace '\]', '`'
    
    # Replace IDENTITY with AUTO_INCREMENT
    $mysqlStatement = $mysqlStatement -replace 'INT IDENTITY\(1,1\)', 'INT AUTO_INCREMENT'
    
    # Replace BIT with TINYINT(1) for booleans
    $mysqlStatement = $mysqlStatement -replace '\sBIT\b', ' TINYINT(1)'
    
    # Replace DATETIME2 with DATETIME
    $mysqlStatement = $mysqlStatement -replace 'DATETIME2', 'DATETIME'
    
    # Add DROP TABLE IF EXISTS for MySQL
    if ($mysqlStatement -match "CREATE TABLE ``(\w+)``") {
        $tableName = $matches[1]
        $mysqlStatement = "DROP TABLE IF EXISTS `$tableName`;`n`n$mysqlStatement"
    }
    
    return $mysqlStatement
}

function Get-SQLConnectionObject {
    <#
    .SYNOPSIS
    Creates and returns a SQL connection object
    #>
    
    param (
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL"
    )
    
    # Get configuration
    $config = Get-AppConfig
    
    if ($DatabaseType -eq "MySQL") {
        $server = $config.MySQL.Server
        $database = $config.MySQL.Database
        $username = $config.MySQL.Username
        $password = $config.MySQL.Password
        $port = if ($config.MySQL.Port) { $config.MySQL.Port } else { 3306 }
        
        $connectionString = "Server=$server;Port=$port;Database=$database;"
        if ($username -and $password) {
            $connectionString += "Uid=$username;Pwd=$password;"
        }
        $connectionString += "SslMode=Disabled;AllowPublicKeyRetrieval=True;"
        
        # Load MySQL DLL
        $dllPaths = @(
            "/mnt/c/Program Files (x86)/MySQL/MySQL Connector NET 9.5/MySql.Data.dll",
            "/mnt/c/Program Files/MySQL/MySQL Connector NET 9.5/MySql.Data.dll",
            "C:\Program Files (x86)\MySQL\MySQL Connector NET 9.5\MySql.Data.dll"
        )
        
        foreach ($path in $dllPaths) {
            if (Test-Path $path) {
                Add-Type -Path $path -ErrorAction SilentlyContinue
                break
            }
        }
        
        $connection = New-Object MySql.Data.MySqlClient.MySqlConnection
        $connection.ConnectionString = $connectionString
        
        return $connection
    }
    else {
        # SQL Server configuration
        $server = $config.SQLServer.Server
        $database = $config.SQLServer.Database
        $username = $config.SQLServer.Username
        $password = $config.SQLServer.Password
        
        if ($username -and $password) {
            $connectionString = "Server=$server;Database=$database;User Id=$username;Password=$password;"
        } else {
            $connectionString = "Server=$server;Database=$database;Integrated Security=True;"
        }
        
        $connection = New-Object System.Data.SqlClient.SqlConnection
        $connection.ConnectionString = $connectionString
        
        return $connection
    }
}

function Export-MigrationLog {
    <#
    .SYNOPSIS
    Exports migration results to a log file
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        $MigrationResult,
        
        [Parameter(Mandatory=$false)]
        [string]$OutputPath = ".\migration_log.txt"
    )
    
    try {
        $log = "="*60 + "`n"
        $log += "Migration Log - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`n"
        $log += "="*60 + "`n`n"
        
        $log += "Duration: $($MigrationResult.EndTime - $MigrationResult.StartTime)`n"
        $log += "Total documents: $($MigrationResult.TotalDocuments)`n"
        $log += "Successfully migrated: $($MigrationResult.MigratedDocuments)`n"
        $log += "Failed: $($MigrationResult.FailedDocuments)`n`n"
        
        $log += "Tables created:`n"
        foreach ($table in $MigrationResult.TablesCreated) {
            $log += "  - $table`n"
        }
        
        $log += "`nRecords inserted:`n"
        foreach ($table in $MigrationResult.RecordsInserted.Keys) {
            $log += "  $table : $($MigrationResult.RecordsInserted[$table])`n"
        }
        
        if ($MigrationResult.Errors.Count -gt 0) {
            $log += "`nErrors:`n"
            foreach ($err in $MigrationResult.Errors) {
                $log += "  [$($error.Timestamp)] Document $($error.Document): $($error.Error)`n"
            }
        }
        
        $log | Out-File -FilePath $OutputPath -Encoding UTF8
        Write-Host "Migration log exported to: $OutputPath" -ForegroundColor Green
        
        return $true
    }
    catch {
        Write-Host "Error exporting log: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Invoke-CompleteMigration {
    <#
    .SYNOPSIS
    Performs complete migration workflow: analyze, generate schema, migrate data
    #>
    
    param (
        [Parameter(Mandatory=$false)]
        [int]$SampleSize = 100,
        
        [Parameter(Mandatory=$false)]
        [int]$BatchSize = 100,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL"
    )
    
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "  Complete MongoDB to $DatabaseType Migration Workflow" -ForegroundColor Cyan
    Write-Host ("="*60) + "`n" -ForegroundColor Cyan
    
    # Get configuration
    $config = Get-AppConfig
    $collectionName = $config.MongoDB.Collection
    if (-not $collectionName) {
        throw "Collection name must be specified in config.json under MongoDB.Collection"
    }
    
    # Step 1: Analyze MongoDB schema
    Write-Host "Phase 1: Schema Analysis" -ForegroundColor Yellow
    $schema = Get-MongoDBSchema -ConnectionString $config.MongoDB.ConnectionString `
                                -DatabaseName $config.MongoDB.Database `
                                -CollectionName $collectionName `
                                -SampleSize $SampleSize
    
    # Step 2: Generate SQL schema
    Write-Host "`nPhase 2: SQL Schema Generation" -ForegroundColor Yellow
    $sqlSchema = New-SQLSchema -Schema $schema `
                               -TableName $collectionName `
                               -PrimaryKeyField "_id"
    
    # Export schema
    $schemaFile = ".\schema_$collectionName.sql"
    Export-SQLSchema -SchemaResult $sqlSchema -OutputPath $schemaFile
    
    # Step 3: Migrate data
    Write-Host "`nPhase 3: Data Migration" -ForegroundColor Yellow
    $migrationResult = Start-DataMigration -Schema $schema `
                                           -SQLSchema $sqlSchema `
                                           -CollectionName $collectionName `
                                           -BatchSize $BatchSize `
                                           -DatabaseType $DatabaseType
    
    # Step 4: Export log
    $logFile = ".\migration_log_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"
    Export-MigrationLog -MigrationResult $migrationResult -OutputPath $logFile
    
    Write-Host "`n Complete migration workflow finished!" -ForegroundColor Green
    Write-Host "  Schema file: $schemaFile" -ForegroundColor Gray
    Write-Host "  Log file: $logFile" -ForegroundColor Gray
    
    return $migrationResult
}