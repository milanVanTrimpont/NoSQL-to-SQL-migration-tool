function Start-IncrementalSync {
    <#
    .SYNOPSIS
    Performs incremental synchronization by detecting and syncing only changes
    
    .DESCRIPTION
    This function detects changes since the last sync and only migrates:
    - New documents (inserted)
    - Modified documents (updated)
    - Deleted documents (removed)
    
    Uses a sync state file to track last sync timestamp and document hashes
    
    .PARAMETER TableName
    Name of the SQL table to sync
    
    .PARAMETER DatabaseType
    Type of SQL database (MySQL or SQLServer)
    
    .PARAMETER ForceFullSync
    Forces a full resync instead of incremental
    
    .EXAMPLE
    Start-IncrementalSync -TableName "klanten" -DatabaseType "MySQL"
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [string]$TableName,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL",
        
        [Parameter(Mandatory=$false)]
        [switch]$ForceFullSync
    )
    
    Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "    Incremental Sync - $TableName" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
    
    # Initialize sync result
    $syncResult = @{
        SyncTime = Get-Date
        TableName = $TableName
        NewRecords = 0
        UpdatedRecords = 0
        DeletedRecords = 0
        UnchangedRecords = 0
        TotalProcessed = 0
        Errors = @()
        LastSyncTime = $null
        IsFullSync = $ForceFullSync.IsPresent
    }
    
    try {
        # Step 1: Load or create sync state
        $syncStateFile = ".\sync_state_$TableName.json"
        $syncState = Get-SyncState -FilePath $syncStateFile
        
        if ($ForceFullSync -or $null -eq $syncState) {
            Write-Host "Performing FULL SYNC..." -ForegroundColor Yellow
            $syncResult.IsFullSync = $true
        }
        else {
            Write-Host "Performing INCREMENTAL SYNC since $($syncState.LastSyncTime)" -ForegroundColor Yellow
            $syncResult.LastSyncTime = $syncState.LastSyncTime
        }
        
        # Step 2: Connect to databases
        Write-Host "`nStep 1: Connecting to databases..." -ForegroundColor Yellow
        
        # Get configuration
        $config = Get-AppConfig
        
        # MongoDB
        Connect-Mdbc -ConnectionString $config.MongoDB.ConnectionString `
                     -DatabaseName $config.MongoDB.Database `
                     -CollectionName $TableName
        
        $mongoDocuments = Get-MdbcData
        Write-Host " MongoDB: $($mongoDocuments.Count) documents" -ForegroundColor Green
        
        # SQL
        $sqlConnection = Get-SQLConnectionObject -DatabaseType $DatabaseType
        $sqlConnection.Open()
        Write-Host " SQL connected" -ForegroundColor Green
        
        # Step 2.5: Check and update schema if needed
        Write-Host "`nStep 1.5: Checking for schema changes..." -ForegroundColor Yellow
        $schemaUpdated = Update-SQLSchema -Connection $sqlConnection `
                                         -TableName $TableName `
                                         -MongoDocuments $mongoDocuments `
                                         -DatabaseType $DatabaseType
        
        if ($schemaUpdated) {
            Write-Host " Schema updated with new columns" -ForegroundColor Green
        }
        else {
            Write-Host " Schema is up to date" -ForegroundColor Gray
        }
        
        # Step 3: Get current SQL records
        Write-Host "`nStep 2: Loading existing SQL records..." -ForegroundColor Yellow
        $existingRecords = Get-AllSQLRecords -Connection $sqlConnection `
                                            -TableName $TableName `
                                            -DatabaseType $DatabaseType
        
        Write-Host " Loaded $($existingRecords.Count) existing SQL records" -ForegroundColor Green
        
        # Step 4: Detect changes
        Write-Host "`nStep 3: Detecting changes..." -ForegroundColor Yellow
        
        $mongoIds = @{}
        $newDocs = @()
        $updatedDocs = @()
        
        foreach ($doc in $mongoDocuments) {
            $syncResult.TotalProcessed++
            $docId = $doc._id.ToString()
            $mongoIds[$docId] = $true
            
            # Calculate document hash
            $docHash = Get-DocumentHash -Document $doc
            
            # Check if document exists in SQL
            if ($existingRecords.ContainsKey($docId)) {
                # Document exists - check if modified
                $lastHash = if ($syncState -and $syncState.DocumentHashes.ContainsKey($docId)) {
                    $syncState.DocumentHashes[$docId]
                } else {
                    $null
                }
                
                if ($syncResult.IsFullSync -or $docHash -ne $lastHash) {
                    $updatedDocs += @{
                        Document = $doc
                        Id = $docId
                        Hash = $docHash
                    }
                }
                else {
                    $syncResult.UnchangedRecords++
                }
            }
            else {
                # New document
                $newDocs += @{
                    Document = $doc
                    Id = $docId
                    Hash = $docHash
                }
            }
        }
        
        # Detect deleted documents
        $deletedIds = @()
        foreach ($sqlId in $existingRecords.Keys) {
            if (-not $mongoIds.ContainsKey($sqlId)) {
                $deletedIds += $sqlId
            }
        }
        
        Write-Host "  New documents: $($newDocs.Count)" -ForegroundColor Green
        Write-Host "  Updated documents: $($updatedDocs.Count)" -ForegroundColor Yellow
        Write-Host "  Deleted documents: $($deletedIds.Count)" -ForegroundColor Red
        Write-Host "  Unchanged: $($syncResult.UnchangedRecords)" -ForegroundColor Gray
        
        # Step 5: Sync changes
        Write-Host "`nStep 4: Syncing changes..." -ForegroundColor Yellow
        
        $newSyncState = @{
            LastSyncTime = $syncResult.SyncTime
            DocumentHashes = @{}
        }
        
        # Insert new documents
        if ($newDocs.Count -gt 0) {
            Write-Host "  Inserting $($newDocs.Count) new records..." -ForegroundColor Green
            
            foreach ($item in $newDocs) {
                try {
                    $success = Invoke-InsertDocument -Connection $sqlConnection `
                                                     -TableName $TableName `
                                                     -Document $item.Document `
                                                     -DatabaseType $DatabaseType
                    
                    if ($success) {
                        $syncResult.NewRecords++
                        $newSyncState.DocumentHashes[$item.Id] = $item.Hash
                    }
                }
                catch {
                    $syncResult.Errors += "Failed to insert document $($item.Id): $($_.Exception.Message)"
                }
            }
            
            Write-Host "   Inserted $($syncResult.NewRecords) records" -ForegroundColor Green
        }
        
        # Update modified documents
        if ($updatedDocs.Count -gt 0) {
            Write-Host "  Updating $($updatedDocs.Count) modified records..." -ForegroundColor Yellow
            
            foreach ($item in $updatedDocs) {
                try {
                    $success = Invoke-UpdateDocument -Connection $sqlConnection `
                                                     -TableName $TableName `
                                                     -Document $item.Document `
                                                     -DatabaseType $DatabaseType
                    
                    if ($success) {
                        $syncResult.UpdatedRecords++
                        $newSyncState.DocumentHashes[$item.Id] = $item.Hash
                    }
                }
                catch {
                    $syncResult.Errors += "Failed to update document $($item.Id): $($_.Exception.Message)"
                }
            }
            
            Write-Host "   Updated $($syncResult.UpdatedRecords) records" -ForegroundColor Yellow
        }
        
        # Delete removed documents
        if ($deletedIds.Count -gt 0) {
            Write-Host "  Deleting $($deletedIds.Count) removed records..." -ForegroundColor Red
            
            foreach ($id in $deletedIds) {
                try {
                    $success = Invoke-DeleteDocument -Connection $sqlConnection `
                                                     -TableName $TableName `
                                                     -Id $id `
                                                     -DatabaseType $DatabaseType
                    
                    if ($success) {
                        $syncResult.DeletedRecords++
                    }
                }
                catch {
                    $syncResult.Errors += "Failed to delete document $id : $($_.Exception.Message)"
                }
            }
            
            Write-Host "   Deleted $($syncResult.DeletedRecords) records" -ForegroundColor Red
        }
        
        # Preserve hashes for unchanged documents
        if ($syncState) {
            foreach ($id in $mongoIds.Keys) {
                if (-not $newSyncState.DocumentHashes.ContainsKey($id) -and $syncState.DocumentHashes.ContainsKey($id)) {
                    $newSyncState.DocumentHashes[$id] = $syncState.DocumentHashes[$id]
                }
            }
        }
        
        # Step 6: Save sync state
        Save-SyncState -FilePath $syncStateFile -SyncState $newSyncState
        
        # Display summary
        Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "Sync Complete!" -ForegroundColor Green
        Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "Sync Type: $(if ($syncResult.IsFullSync) { 'FULL' } else { 'INCREMENTAL' })" -ForegroundColor Gray
        Write-Host "Total Processed: $($syncResult.TotalProcessed)" -ForegroundColor Gray
        Write-Host "New Records: $($syncResult.NewRecords)" -ForegroundColor Green
        Write-Host "Updated Records: $($syncResult.UpdatedRecords)" -ForegroundColor Yellow
        Write-Host "Deleted Records: $($syncResult.DeletedRecords)" -ForegroundColor Red
        Write-Host "Unchanged: $($syncResult.UnchangedRecords)" -ForegroundColor Gray
        Write-Host "Errors: $($syncResult.Errors.Count)" -ForegroundColor $(if ($syncResult.Errors.Count -gt 0) { 'Red' } else { 'Gray' })
        
        if ($syncResult.Errors.Count -gt 0) {
            Write-Host "`nErrors:" -ForegroundColor Red
            foreach ($err in $syncResult.Errors) {
                Write-Host "  - $error" -ForegroundColor Red
            }
        }
        
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
        
        return $syncResult
    }
    catch {
        Write-Host "`n Sync failed: $($_.Exception.Message)" -ForegroundColor Red
        $syncResult.Errors += "Sync error: $($_.Exception.Message)"
        return $syncResult
    }
    finally {
        if ($sqlConnection -and $sqlConnection.State -eq 'Open') {
            $sqlConnection.Close()
        }
    }
}

function Get-SyncState {
    <#
    .SYNOPSIS
    Loads the sync state from file
    #>
    
    param (
        [string]$FilePath
    )
    
    if (Test-Path $FilePath) {
        try {
            $json = Get-Content $FilePath -Raw | ConvertFrom-Json
            
            # Convert back to hashtable
            $state = @{
                LastSyncTime = [DateTime]$json.LastSyncTime
                DocumentHashes = @{}
            }
            
            foreach ($property in $json.DocumentHashes.PSObject.Properties) {
                $state.DocumentHashes[$property.Name] = $property.Value
            }
            
            return $state
        }
        catch {
            Write-Host "Warning: Could not load sync state, performing full sync" -ForegroundColor Yellow
            return $null
        }
    }
    
    return $null
}

function Save-SyncState {
    <#
    .SYNOPSIS
    Saves the sync state to file
    #>
    
    param (
        [string]$FilePath,
        [hashtable]$SyncState
    )
    
    try {
        $SyncState | ConvertTo-Json -Depth 10 | Out-File -FilePath $FilePath -Encoding UTF8
        Write-Host "`n Sync state saved to: $FilePath" -ForegroundColor Green
    }
    catch {
        Write-Host "Warning: Could not save sync state: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

function Get-DocumentHash {
    <#
    .SYNOPSIS
    Calculates a hash of a document to detect changes
    #>
    
    param (
        $Document
    )
    
    try {
        # Extract flat fields and convert to sorted JSON
        $flatFields = @{}
        
        if ($Document -is [System.Collections.IDictionary]) {
            foreach ($key in ($Document.Keys | Sort-Object)) {
                $value = $Document[$key]
                
                # Only include flat fields for hash
                if ($value -isnot [System.Collections.IEnumerable] -or $value -is [string]) {
                    if ($value -isnot [PSCustomObject] -and $value -isnot [System.Collections.Hashtable]) {
                        # Convert to string for consistent hashing
                        $flatFields[$key] = if ($null -eq $value) { "" } else { $value.ToString() }
                    }
                }
            }
        }
        
        $json = $flatFields | ConvertTo-Json -Compress
        
        # Calculate MD5 hash
        $md5 = [System.Security.Cryptography.MD5]::Create()
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
        $hashBytes = $md5.ComputeHash($bytes)
        $hash = [System.BitConverter]::ToString($hashBytes).Replace("-", "")
        
        return $hash
    }
    catch {
        Write-Host "Warning: Could not calculate hash for document" -ForegroundColor Yellow
        return [guid]::NewGuid().ToString()
    }
}

function Update-SQLSchema {
    <#
    .SYNOPSIS
    Detects new fields in MongoDB and adds corresponding columns to SQL table
    #>
    
    param (
        $Connection,
        [string]$TableName,
        $MongoDocuments,
        [string]$DatabaseType
    )
    
    try {
        # Get existing SQL columns
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = "SHOW COLUMNS FROM " + $TableName
        $reader = $cmd.ExecuteReader()
        
        $existingColumns = @{}
        while ($reader.Read()) {
            $columnName = $reader.GetString(0)
            $existingColumns[$columnName] = $true
        }
        $reader.Close()
        
        # Collect all fields from MongoDB documents
        $mongoFields = @{}
        foreach ($doc in $MongoDocuments) {
            if ($doc -is [System.Collections.IDictionary]) {
                foreach ($key in $doc.Keys) {
                    $value = $doc[$key]
                    
                    # Only track flat fields
                    if ($value -isnot [System.Collections.IEnumerable] -or $value -is [string]) {
                        if ($value -isnot [PSCustomObject] -and $value -isnot [System.Collections.Hashtable]) {
                            if (-not $mongoFields.ContainsKey($key)) {
                                $mongoFields[$key] = $value
                            }
                        }
                    }
                }
            }
        }
        
        # Find missing columns
        $missingColumns = @()
        foreach ($field in $mongoFields.Keys) {
            if (-not $existingColumns.ContainsKey($field)) {
                $missingColumns += @{
                    Name = $field
                    SampleValue = $mongoFields[$field]
                }
            }
        }
        
        # Add missing columns
        if ($missingColumns.Count -gt 0) {
            Write-Host "  Found $($missingColumns.Count) new field(s): $($missingColumns.Name -join ', ')" -ForegroundColor Yellow
            
            foreach ($column in $missingColumns) {
                $dataType = Get-SQLDataType -Value $column.SampleValue -DatabaseType $DatabaseType
                
                # Add column as NULLABLE to allow missing values in existing/new records
                $alterSQL = "ALTER TABLE " + $TableName + " ADD COLUMN " + $column.Name + " " + $dataType + " NULL"
                
                $cmd = $Connection.CreateCommand()
                $cmd.CommandText = $alterSQL
                $cmd.ExecuteNonQuery() | Out-Null
                
                Write-Host "   Added column: $($column.Name) ($dataType NULL)" -ForegroundColor Green
            }
            
            return $true
        }
        
        return $false
    }
    catch {
        Write-Host "Warning: Could not update schema: $($_.Exception.Message)" -ForegroundColor Yellow
        return $false
    }
}

function Get-SQLDataType {
    <#
    .SYNOPSIS
    Determines appropriate SQL data type based on sample value
    #>
    
    param (
        $Value,
        [string]$DatabaseType
    )
    
    if ($null -eq $Value) {
        return "VARCHAR(255)"
    }
    
    $valueType = $Value.GetType().Name
    
    switch -Wildcard ($valueType) {
        "String" { return "VARCHAR(255)" }
        "Int*" { return "INT" }
        "Double" { return "DECIMAL(18,2)" }
        "Float" { return "DECIMAL(18,2)" }
        "Decimal" { return "DECIMAL(18,2)" }
        "Boolean" { return "TINYINT(1)" }
        "DateTime" { return "DATETIME" }
        "ObjectId" { return "VARCHAR(24)" }
        default { return "VARCHAR(255)" }
    }
}

function Get-AllSQLRecords {
    <#
    .SYNOPSIS
    Retrieves all records from SQL table as a hashtable indexed by ID
    #>
    
    param (
        $Connection,
        [string]$TableName,
        [string]$DatabaseType
    )
    
    $records = @{}
    
    try {
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = "SELECT _id FROM " + $TableName
        
        $reader = $cmd.ExecuteReader()
        
        while ($reader.Read()) {
            $id = $reader.GetString(0)
            $records[$id] = $true
        }
        
        $reader.Close()
    }
    catch {
        Write-Host "Error loading SQL records: $($_.Exception.Message)" -ForegroundColor Red
    }
    
    return $records
}

function Invoke-InsertDocument {
    <#
    .SYNOPSIS
    Inserts a new document into SQL
    #>
    
    param (
        $Connection,
        [string]$TableName,
        $Document,
        [string]$DatabaseType
    )
    
    try {
        # Get all columns from SQL table
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = "SHOW COLUMNS FROM " + $TableName
        $reader = $cmd.ExecuteReader()
        
        $allColumns = @()
        while ($reader.Read()) {
            $columnName = $reader.GetString(0)
            $allColumns += $columnName
        }
        $reader.Close()
        
        # Extract flat fields from document
        $documentFields = @{}
        
        if ($Document -is [System.Collections.IDictionary]) {
            foreach ($key in $Document.Keys) {
                $value = $Document[$key]
                
                if ($value -isnot [System.Collections.IEnumerable] -or $value -is [string]) {
                    if ($value -isnot [PSCustomObject] -and $value -isnot [System.Collections.Hashtable]) {
                        $documentFields[$key] = $value
                    }
                }
            }
        }
        
        # Build INSERT with all columns (use NULL for missing fields)
        $columns = @()
        $values = @()
        $parameters = @()
        
        foreach ($column in $allColumns) {
            $columns += $column
            $values += "?"
            
            if ($documentFields.ContainsKey($column)) {
                $parameters += Convert-ToSQLValue -Value $documentFields[$column] -DatabaseType $DatabaseType
            }
            else {
                $parameters += [DBNull]::Value
            }
        }
        
        $insertSQL = "INSERT INTO " + $TableName + " (" + ($columns -join ', ') + ") VALUES (" + ($values -join ', ') + ")"
        
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = $insertSQL
        
        foreach ($param in $parameters) {
            $p = $cmd.CreateParameter()
            $p.Value = $param
            $cmd.Parameters.Add($p) | Out-Null
        }
        
        $cmd.ExecuteNonQuery() | Out-Null
        return $true
    }
    catch {
        Write-Host "Insert error: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Invoke-UpdateDocument {
    <#
    .SYNOPSIS
    Updates an existing document in SQL
    #>
    
    param (
        $Connection,
        [string]$TableName,
        $Document,
        [string]$DatabaseType
    )
    
    try {
        $docId = $Document._id.ToString()
        
        # Extract flat fields
        $flatFields = @{}
        
        if ($Document -is [System.Collections.IDictionary]) {
            foreach ($key in $Document.Keys) {
                if ($key -eq "_id") { continue }  # Skip ID for UPDATE
                
                $value = $Document[$key]
                
                if ($value -isnot [System.Collections.IEnumerable] -or $value -is [string]) {
                    if ($value -isnot [PSCustomObject] -and $value -isnot [System.Collections.Hashtable]) {
                        $flatFields[$key] = $value
                    }
                }
            }
        }
        
        # Build UPDATE
        $setClauses = @()
        
        foreach ($field in $flatFields.Keys) {
            $setClauses += "$field = ?"
        }
        
        $updateSQL = "UPDATE " + $TableName + " SET " + ($setClauses -join ', ') + " WHERE _id = ?"
        
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = $updateSQL
        
        # Add field parameters
        foreach ($field in $flatFields.Keys) {
            $value = $flatFields[$field]
            $sqlValue = Convert-ToSQLValue -Value $value -DatabaseType $DatabaseType
            
            $param = $cmd.CreateParameter()
            $param.Value = $sqlValue
            $cmd.Parameters.Add($param) | Out-Null
        }
        
        # Add WHERE parameter
        $idParam = $cmd.CreateParameter()
        $idParam.Value = $docId
        $cmd.Parameters.Add($idParam) | Out-Null
        
        $cmd.ExecuteNonQuery() | Out-Null
        return $true
    }
    catch {
        Write-Host "Update error: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Invoke-DeleteDocument {
    <#
    .SYNOPSIS
    Deletes a document from SQL
    #>
    
    param (
        $Connection,
        [string]$TableName,
        [string]$Id,
        [string]$DatabaseType
    )
    
    try {
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = "DELETE FROM " + $TableName + " WHERE _id = ?"
        
        $param = $cmd.CreateParameter()
        $param.Value = $Id
        $cmd.Parameters.Add($param) | Out-Null
        
        $cmd.ExecuteNonQuery() | Out-Null
        return $true
    }
    catch {
        Write-Host "Delete error: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Export-SyncReport {
    <#
    .SYNOPSIS
    Exports sync results to a report file
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        $SyncResult,
        
        [Parameter(Mandatory=$false)]
        [string]$OutputPath = ".\sync_report.txt"
    )
    
    try {
        $report = "="*60 + "`n"
        $report += "Sync Report - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`n"
        $report += "="*60 + "`n`n"
        
        $report += "Table: $($SyncResult.TableName)`n"
        $report += "Sync Type: $(if ($SyncResult.IsFullSync) { 'FULL' } else { 'INCREMENTAL' })`n"
        
        if ($SyncResult.LastSyncTime) {
            $report += "Last Sync: $($SyncResult.LastSyncTime.ToString('yyyy-MM-dd HH:mm:ss'))`n"
        }
        
        $report += "Current Sync: $($SyncResult.SyncTime.ToString('yyyy-MM-dd HH:mm:ss'))`n`n"
        
        $report += "Results:`n"
        $report += "  Total Processed: $($SyncResult.TotalProcessed)`n"
        $report += "  New Records: $($SyncResult.NewRecords)`n"
        $report += "  Updated Records: $($SyncResult.UpdatedRecords)`n"
        $report += "  Deleted Records: $($SyncResult.DeletedRecords)`n"
        $report += "  Unchanged: $($SyncResult.UnchangedRecords)`n"
        $report += "  Errors: $($SyncResult.Errors.Count)`n"
        
        if ($SyncResult.Errors.Count -gt 0) {
            $report += "`nErrors:`n"
            foreach ($err in $SyncResult.Errors) {
                $report += "  - $error`n"
            }
        }
        
        $report | Out-File -FilePath $OutputPath -Encoding UTF8
        Write-Host "Sync report exported to: $OutputPath" -ForegroundColor Green
        
        return $true
    }
    catch {
        Write-Host "Error exporting report: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Invoke-ScheduledSync {
    <#
    .SYNOPSIS
    Performs incremental sync and exports report
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [string]$TableName,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL",
        
        [Parameter(Mandatory=$false)]
        [switch]$ForceFullSync
    )
    
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "  Scheduled Sync - $TableName" -ForegroundColor Cyan
    Write-Host ("="*60) + "`n" -ForegroundColor Cyan
    
    # Run sync
    $syncResult = Start-IncrementalSync -TableName $TableName `
                                        -DatabaseType $DatabaseType `
                                        -ForceFullSync:$ForceFullSync
    
    # Export report
    $reportFile = ".\sync_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"
    Export-SyncReport -SyncResult $syncResult -OutputPath $reportFile
    
    Write-Host "`n Scheduled sync complete!" -ForegroundColor Green
    Write-Host "  Report: $reportFile" -ForegroundColor Gray
    
    return $syncResult
}