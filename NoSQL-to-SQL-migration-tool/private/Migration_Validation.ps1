function Test-MigrationValidation {
    <#
    .SYNOPSIS
    Validates data migration from MongoDB to SQL database
    
    .DESCRIPTION
    Performs comprehensive validation of migrated data:
    - Compares record counts between source and destination
    - Validates sample data integrity
    - Checks for data type consistency
    - Generates detailed validation report
    
    .PARAMETER TableName
    Name of the SQL table to validate
    
    .PARAMETER SampleSize
    Number of random records to validate in detail (default: 10)
    
    .PARAMETER DatabaseType
    Type of SQL database (MySQL or SQLServer)
    
    .EXAMPLE
    Test-MigrationValidation -TableName "klanten" -SampleSize 5 -DatabaseType "MySQL"
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [string]$TableName,
        
        [Parameter(Mandatory=$false)]
        [int]$SampleSize = 10,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL"
    )
    
    Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "    Migration Validation - $TableName" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
    
    # Initialize validation result
    $validationResult = @{
        TableName = $TableName
        ValidationTime = Get-Date
        RecordCountMatch = $false
        MongoCount = 0
        SQLCount = 0
        SamplesValidated = 0
        SamplesPassed = 0
        SamplesFailed = 0
        Issues = @()
        Warnings = @()
        Details = @()
        OverallStatus = "Unknown"
    }
    
    try {
        # Step 1: Connect to databases
        Write-Host "Step 1: Connecting to databases..." -ForegroundColor Yellow
        
        # Get configuration
        $config = Get-AppConfig
        
        # MongoDB connection
        Connect-Mdbc -ConnectionString $config.MongoDB.ConnectionString `
                     -DatabaseName $config.MongoDB.Database `
                     -CollectionName $TableName
        
        $validationResult.MongoCount = Get-MdbcData -Count
        Write-Host " MongoDB: $($validationResult.MongoCount) documents" -ForegroundColor Green
        
        # SQL connection
        $sqlConnection = Get-SQLConnectionObject -DatabaseType $DatabaseType
        $sqlConnection.Open()
        
        $sqlCmd = $sqlConnection.CreateCommand()
        $countQuery = "SELECT COUNT(*) FROM ``" + $TableName + "``"
        $sqlCmd.CommandText = $countQuery
        $validationResult.SQLCount = [int]$sqlCmd.ExecuteScalar()
        Write-Host " $DatabaseType : $($validationResult.SQLCount) records" -ForegroundColor Green
        
        # Step 2: Compare record counts
        Write-Host "`nStep 2: Comparing record counts..." -ForegroundColor Yellow
        
        if ($validationResult.MongoCount -eq $validationResult.SQLCount) {
            Write-Host " Record counts match!" -ForegroundColor Green
            $validationResult.RecordCountMatch = $true
        }
        else {
            $diff = [Math]::Abs($validationResult.MongoCount - $validationResult.SQLCount)
            Write-Host " Record count mismatch! Difference: $diff records" -ForegroundColor Red
            $validationResult.Issues += "Record count mismatch: MongoDB=$($validationResult.MongoCount), SQL=$($validationResult.SQLCount)"
        }
        
        # Step 3: Validate sample data
        Write-Host "`nStep 3: Validating sample data..." -ForegroundColor Yellow
        
        $actualSampleSize = [Math]::Min($SampleSize, $validationResult.MongoCount)
        $validationResult.SamplesValidated = $actualSampleSize
        
        if ($actualSampleSize -gt 0) {
            # Get random sample from MongoDB
            $mongoDocuments = Get-MdbcData -Last $actualSampleSize
            
            foreach ($mongoDoc in $mongoDocuments) {
                $docId = $mongoDoc._id.ToString()
                
                Write-Progress -Activity "Validating samples" `
                              -Status "Checking document $docId" `
                              -PercentComplete (($validationResult.SamplesPassed + $validationResult.SamplesFailed) / $actualSampleSize * 100)
                
                # Get corresponding SQL record
                $sqlRecord = Get-SQLRecord -Connection $sqlConnection `
                                          -TableName $TableName `
                                          -Id $docId `
                                          -DatabaseType $DatabaseType
                
                if ($null -eq $sqlRecord) {
                    $validationResult.SamplesFailed++
                    $validationResult.Issues += "Document $docId not found in SQL database"
                    Write-Host " Document $docId not found in SQL" -ForegroundColor Red
                }
                else {
                    # Compare fields
                    $comparisonResult = Compare-DocumentToRecord -MongoDocument $mongoDoc `
                                                                 -SQLRecord $sqlRecord `
                                                                 -DatabaseType $DatabaseType
                    
                    if ($comparisonResult.Match) {
                        $validationResult.SamplesPassed++
                        Write-Host " Document $docId validated successfully" -ForegroundColor Green
                    }
                    else {
                        $validationResult.SamplesFailed++
                        $validationResult.Issues += "Document $docId has mismatches: $($comparisonResult.Differences -join ', ')"
                        Write-Host " Document $docId has differences: $($comparisonResult.Differences -join ', ')" -ForegroundColor Red
                    }
                    
                    $validationResult.Details += $comparisonResult
                }
            }
            
            Write-Progress -Activity "Validating samples" -Completed
        }
        
        # Step 4: Data integrity checks
        Write-Host "`nStep 4: Checking data integrity..." -ForegroundColor Yellow
        
        $integrityIssues = Test-DataIntegrity -Connection $sqlConnection `
                                              -TableName $TableName `
                                              -DatabaseType $DatabaseType
        
        if ($integrityIssues.Count -eq 0) {
            Write-Host " No integrity issues found" -ForegroundColor Green
        }
        else {
            foreach ($issue in $integrityIssues) {
                Write-Host "⚠ $issue" -ForegroundColor Yellow
                $validationResult.Warnings += $issue
            }
        }
        
        # Determine overall status
        if ($validationResult.Issues.Count -eq 0) {
            $validationResult.OverallStatus = "PASSED"
            $statusColor = "Green"
        }
        elseif ($validationResult.SamplesPassed -gt $validationResult.SamplesFailed) {
            $validationResult.OverallStatus = "PARTIAL"
            $statusColor = "Yellow"
        }
        else {
            $validationResult.OverallStatus = "FAILED"
            $statusColor = "Red"
        }
        
        # Display summary
        Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "Validation Summary" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "Overall Status: $($validationResult.OverallStatus)" -ForegroundColor $statusColor
        Write-Host "Record Count Match: $(if ($validationResult.RecordCountMatch) { 'YES' } else { 'NO' })" -ForegroundColor $(if ($validationResult.RecordCountMatch) { 'Green' } else { 'Red' })
        Write-Host "Samples Validated: $($validationResult.SamplesValidated)" -ForegroundColor Gray
        Write-Host "  - Passed: $($validationResult.SamplesPassed)" -ForegroundColor Green
        Write-Host "  - Failed: $($validationResult.SamplesFailed)" -ForegroundColor $(if ($validationResult.SamplesFailed -gt 0) { 'Red' } else { 'Gray' })
        Write-Host "Issues Found: $($validationResult.Issues.Count)" -ForegroundColor $(if ($validationResult.Issues.Count -gt 0) { 'Red' } else { 'Green' })
        Write-Host "Warnings: $($validationResult.Warnings.Count)" -ForegroundColor $(if ($validationResult.Warnings.Count -gt 0) { 'Yellow' } else { 'Gray' })
        
        if ($validationResult.Issues.Count -gt 0) {
            Write-Host "`nIssues:" -ForegroundColor Red
            foreach ($issue in $validationResult.Issues) {
                Write-Host "  - $issue" -ForegroundColor Red
            }
        }
        
        if ($validationResult.Warnings.Count -gt 0) {
            Write-Host "`nWarnings:" -ForegroundColor Yellow
            foreach ($warning in $validationResult.Warnings) {
                Write-Host "  - $warning" -ForegroundColor Yellow
            }
        }
        
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
        
        return $validationResult
    }
    catch {
        Write-Host "`n Validation failed: $($_.Exception.Message)" -ForegroundColor Red
        $validationResult.OverallStatus = "ERROR"
        $validationResult.Issues += "Validation error: $($_.Exception.Message)"
        return $validationResult
    }
    finally {
        if ($sqlConnection -and $sqlConnection.State -eq 'Open') {
            $sqlConnection.Close()
        }
    }
}

function Get-SQLRecord {
    <#
    .SYNOPSIS
    Retrieves a single record from SQL database by ID
    #>
    
    param (
        $Connection,
        [string]$TableName,
        [string]$Id,
        [string]$DatabaseType
    )
    
    try {
        $cmd = $Connection.CreateCommand()
        # Fix: Build query without template literals
        $query = 'SELECT * FROM `' + $TableName + '` WHERE `_id` = ?'
        $cmd.CommandText = $query
        
        $param = $cmd.CreateParameter()
        $param.Value = $Id
        $cmd.Parameters.Add($param) | Out-Null
        
        $reader = $cmd.ExecuteReader()
        
        if ($reader.Read()) {
            $record = @{}
            for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                $fieldName = $reader.GetName($i)
                $fieldValue = if ($reader.IsDBNull($i)) { $null } else { $reader.GetValue($i) }
                $record[$fieldName] = $fieldValue
            }
            $reader.Close()
            return $record
        }
        
        $reader.Close()
        return $null
    }
    catch {
        Write-Host "Error retrieving SQL record: $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

function Compare-DocumentToRecord {
    <#
    .SYNOPSIS
    Compares a MongoDB document with a SQL record
    #>
    
    param (
        $MongoDocument,
        $SQLRecord,
        [string]$DatabaseType
    )
    
    $result = @{
        DocumentId = $MongoDocument._id.ToString()
        Match = $true
        Differences = @()
        FieldsCompared = 0
    }
    
    # Get flat fields from MongoDB document
    $mongoFields = @{}
    if ($MongoDocument -is [System.Collections.IDictionary]) {
        foreach ($key in $MongoDocument.Keys) {
            $value = $MongoDocument[$key]
            
            # Only compare flat fields
            if ($value -isnot [System.Collections.IEnumerable] -or $value -is [string]) {
                if ($value -isnot [PSCustomObject] -and $value -isnot [System.Collections.Hashtable]) {
                    $mongoFields[$key] = $value
                }
            }
        }
    }
    
    # Compare each field
    foreach ($fieldName in $mongoFields.Keys) {
        if ($SQLRecord.ContainsKey($fieldName)) {
            $result.FieldsCompared++
            
            $mongoValue = $mongoFields[$fieldName]
            $sqlValue = $SQLRecord[$fieldName]
            
            # Normalize values for comparison
            $mongoNormalized = Normalize-ValueForComparison -Value $mongoValue -DatabaseType $DatabaseType
            $sqlNormalized = Normalize-ValueForComparison -Value $sqlValue -DatabaseType $DatabaseType
            
            if ($mongoNormalized -ne $sqlNormalized) {
                $result.Match = $false
                $result.Differences += "$fieldName (Mongo: '$mongoNormalized' vs SQL: '$sqlNormalized')"
            }
        }
        else {
            $result.Match = $false
            $result.Differences += "$fieldName missing in SQL"
        }
    }
    
    return $result
}

function Normalize-ValueForComparison {
    <#
    .SYNOPSIS
    Normalizes values for comparison between MongoDB and SQL
    #>
    
    param (
        $Value,
        [string]$DatabaseType
    )
    
    if ($null -eq $Value) {
        return ""
    }
    
    # Handle ObjectId
    if ($Value.GetType().Name -eq "ObjectId") {
        return $Value.ToString()
    }
    
    # Handle Boolean (MySQL stores as 0/1)
    if ($Value -is [bool]) {
        return if ($Value) { "1" } else { "0" }
    }
    
    # Handle numbers (convert to string for comparison)
    if ($Value -is [int] -or $Value -is [long] -or $Value -is [double] -or $Value -is [decimal]) {
        return $Value.ToString()
    }
    
    # Handle DateTime
    if ($Value -is [DateTime]) {
        return $Value.ToString("yyyy-MM-dd HH:mm:ss")
    }
    
    # Everything else as string
    return $Value.ToString().Trim()
}

function Test-DataIntegrity {
    <#
    .SYNOPSIS
    Checks data integrity in SQL table
    #>
    
    param (
        $Connection,
        [string]$TableName,
        [string]$DatabaseType
    )
    
    $issues = @()
    
    try {
        # Check for NULL values in PRIMARY KEY
        $cmd = $Connection.CreateCommand()
        $query1 = "SELECT COUNT(*) FROM ``" + $TableName + "`` WHERE `_id` IS NULL"
        $cmd.CommandText = $query1
        $nullPKCount = [int]$cmd.ExecuteScalar()
        
        if ($nullPKCount -gt 0) {
            $issues += "Found $nullPKCount records with NULL primary key"
        }
        
        # Check for duplicate IDs
        $query2 = "SELECT ``_id``, COUNT(*) as cnt FROM ``" + $TableName + "`` GROUP BY ``_id`` HAVING cnt > 1"
        $cmd.CommandText = $query2
        $reader = $cmd.ExecuteReader()
        $duplicates = 0
        while ($reader.Read()) {
            $duplicates++
        }
        $reader.Close()
        
        if ($duplicates -gt 0) {
            $issues += "Found $duplicates duplicate _id values"
        }
        
        # Check table statistics
        $query3 = "SELECT COUNT(*) FROM ``" + $TableName + "``"
        $cmd.CommandText = $query3
        $totalRecords = [int]$cmd.ExecuteScalar()
        
        if ($totalRecords -eq 0) {
            $issues += "Table is empty - migration may have failed"
        }
    }
    catch {
        $issues += "Error during integrity check: $($_.Exception.Message)"
    }
    
    return $issues
}

function Export-ValidationReport {
    <#
    .SYNOPSIS
    Exports validation results to a detailed report
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        $ValidationResult,
        
        [Parameter(Mandatory=$false)]
        [string]$OutputPath = ".\validation_report.html"
    )
    
    try {
        $html = @"
<!DOCTYPE html>
<html>
<head>
    <title>Migration Validation Report - $($ValidationResult.TableName)</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
        h2 { color: #34495e; margin-top: 30px; }
        .status { font-size: 24px; font-weight: bold; padding: 15px; border-radius: 5px; margin: 20px 0; }
        .status.passed { background: #d4edda; color: #155724; }
        .status.partial { background: #fff3cd; color: #856404; }
        .status.failed { background: #f8d7da; color: #721c24; }
        .metric { display: inline-block; margin: 15px 30px 15px 0; }
        .metric-label { color: #7f8c8d; font-size: 14px; }
        .metric-value { font-size: 32px; font-weight: bold; color: #2c3e50; }
        .issue { background: #f8d7da; border-left: 4px solid #dc3545; padding: 10px; margin: 10px 0; }
        .warning { background: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; margin: 10px 0; }
        .success { color: #28a745; }
        .error { color: #dc3545; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #3498db; color: white; }
        tr:hover { background: #f5f5f5; }
        .footer { margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; color: #7f8c8d; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Migration Validation Report</h1>
        <p><strong>Table:</strong> $($ValidationResult.TableName)</p>
        <p><strong>Validation Time:</strong> $($ValidationResult.ValidationTime.ToString('yyyy-MM-dd HH:mm:ss'))</p>
        
        <div class="status $($ValidationResult.OverallStatus.ToLower())">
            Overall Status: $($ValidationResult.OverallStatus)
        </div>
        
        <h2>Record Count Comparison</h2>
        <div>
            <div class="metric">
                <div class="metric-label">MongoDB Documents</div>
                <div class="metric-value">$($ValidationResult.MongoCount)</div>
            </div>
            <div class="metric">
                <div class="metric-label">SQL Records</div>
                <div class="metric-value">$($ValidationResult.SQLCount)</div>
            </div>
            <div class="metric">
                <div class="metric-label">Match</div>
                <div class="metric-value $(if ($ValidationResult.RecordCountMatch) { 'success' } else { 'error' })">
                    $(if ($ValidationResult.RecordCountMatch) { '' } else { '' })
                </div>
            </div>
        </div>
        
        <h2>Sample Validation</h2>
        <div>
            <div class="metric">
                <div class="metric-label">Samples Validated</div>
                <div class="metric-value">$($ValidationResult.SamplesValidated)</div>
            </div>
            <div class="metric">
                <div class="metric-label">Passed</div>
                <div class="metric-value success">$($ValidationResult.SamplesPassed)</div>
            </div>
            <div class="metric">
                <div class="metric-label">Failed</div>
                <div class="metric-value error">$($ValidationResult.SamplesFailed)</div>
            </div>
        </div>
"@

        if ($ValidationResult.Issues.Count -gt 0) {
            $html += @"
        
        <h2>Issues Found ($($ValidationResult.Issues.Count))</h2>
"@
            foreach ($issue in $ValidationResult.Issues) {
                $html += "<div class='issue'>$issue</div>`n"
            }
        }
        
        if ($ValidationResult.Warnings.Count -gt 0) {
            $html += @"
        
        <h2>Warnings ($($ValidationResult.Warnings.Count))</h2>
"@
            foreach ($warning in $ValidationResult.Warnings) {
                $html += "<div class='warning'>$warning</div>`n"
            }
        }
        
        if ($ValidationResult.Details.Count -gt 0) {
            $html += @"
        
        <h2>Detailed Comparison Results</h2>
        <table>
            <tr>
                <th>Document ID</th>
                <th>Status</th>
                <th>Fields Compared</th>
                <th>Differences</th>
            </tr>
"@
            foreach ($detail in $ValidationResult.Details) {
                $statusText = if ($detail.Match) { " Pass" } else { " Fail" }
                $statusClass = if ($detail.Match) { "success" } else { "error" }
                $differences = if ($detail.Differences.Count -gt 0) { $detail.Differences -join "<br>" } else { "-" }
                
                $html += @"
            <tr>
                <td>$($detail.DocumentId)</td>
                <td class='$statusClass'>$statusText</td>
                <td>$($detail.FieldsCompared)</td>
                <td>$differences</td>
            </tr>
"@
            }
            $html += "</table>`n"
        }
        
        $html += @"
        
        <div class="footer">
            Generated by NoSQL-to-SQL Migration Tool
        </div>
    </div>
</body>
</html>
"@
        
        $html | Out-File -FilePath $OutputPath -Encoding UTF8
        Write-Host "Validation report exported to: $OutputPath" -ForegroundColor Green
        
        return $true
    }
    catch {
        Write-Host "Error exporting validation report: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Invoke-CompleteValidation {
    <#
    .SYNOPSIS
    Performs complete validation and generates report
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [string]$TableName,
        
        [Parameter(Mandatory=$false)]
        [int]$SampleSize = 10,
        
        [Parameter(Mandatory=$false)]
        [ValidateSet("MySQL", "SQLServer")]
        [string]$DatabaseType = "MySQL"
    )
    
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "  Complete Migration Validation" -ForegroundColor Cyan
    Write-Host ("="*60) + "`n" -ForegroundColor Cyan
    
    # Run validation
    $validationResult = Test-MigrationValidation -TableName $TableName `
                                                  -SampleSize $SampleSize `
                                                  -DatabaseType $DatabaseType
    
    # Export report
    $reportFile = ".\validation_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').html"
    Export-ValidationReport -ValidationResult $validationResult -OutputPath $reportFile
    
    Write-Host "`n Validation complete!" -ForegroundColor Green
    Write-Host "  Report file: $reportFile" -ForegroundColor Gray
    Write-Host "  Open the HTML file in your browser to view the detailed report.`n" -ForegroundColor Gray
    
    return $validationResult
}