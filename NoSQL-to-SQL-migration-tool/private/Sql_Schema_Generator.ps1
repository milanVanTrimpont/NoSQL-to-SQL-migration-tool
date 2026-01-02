function New-SQLSchema {
    <#
    .SYNOPSIS
    Generates SQL CREATE TABLE statements from MongoDB schema analysis
    
    .DESCRIPTION
    This function takes a MongoDB schema analysis and generates normalized SQL table structures.
    It handles:
    - Basic field type mapping (MongoDB types to SQL types)
    - Nested objects (creates separate tables with foreign keys)
    - Arrays (creates junction/child tables)
    - Primary keys and constraints
    
    .PARAMETER Schema
    The schema hashtable returned from Get-MongoDBSchema
    
    .PARAMETER TableName
    Base name for the main table
    
    .PARAMETER PrimaryKeyField
    Field to use as primary key (default: _id)
    
    .PARAMETER IncludeDropStatements
    Whether to include DROP TABLE statements (default: true)
    
    .EXAMPLE
    $schema = Get-MongoDBSchema -ConnectionString $conn -DatabaseName "mydb" -CollectionName "users"
    $sqlStatements = New-SQLSchema -Schema $schema -TableName "users"
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [hashtable]$Schema,
        
        [Parameter(Mandatory=$true)]
        [string]$TableName,
        
        [Parameter(Mandatory=$false)]
        [string]$PrimaryKeyField = "_id",
        
        [Parameter(Mandatory=$false)]
        [bool]$IncludeDropStatements = $true
    )
    
    Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "    SQL Schema Generation - $TableName" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
    
    # Initialize result object
    $result = @{
        MainTable = $TableName
        Tables = @()
        Statements = @()
        Relationships = @()
    }
    
    # Separate fields by type (flat, nested, arrays)
    $flatFields = @{}
    $nestedObjects = @{}
    $arrayFields = @{}
    
    foreach ($fieldPath in $Schema.Keys) {
        $fieldInfo = $Schema[$fieldPath]
        
        # Skip fields that are inside arrays (they have [] in path)
        if ($fieldPath -match '\[\]\.') {
            continue
        }
        
        # Check if this is an array element container
        if ($fieldPath -match '\[\]$') {
            $cleanPath = $fieldPath -replace '\[\]$', ''
            $arrayFields[$cleanPath] = $fieldInfo
        }
        # Check if this is a nested object field
        elseif ($fieldPath -contains '.') {
            $rootPath = $fieldPath.Split('.')[0]
            if (-not $nestedObjects.ContainsKey($rootPath)) {
                $nestedObjects[$rootPath] = @{}
            }
            $nestedObjects[$rootPath][$fieldPath] = $fieldInfo
        }
        # Regular flat field
        else {
            # Only add if not array or nested
            if (-not $fieldInfo.IsArray -and -not $fieldInfo.IsNested) {
                $flatFields[$fieldPath] = $fieldInfo
            }
            elseif ($fieldInfo.IsNested -and -not $fieldInfo.IsArray) {
                # Nested object (not array)
                $nestedObjects[$fieldPath] = @{$fieldPath = $fieldInfo}
            }
        }
    }
    
    Write-Host "Analysis:" -ForegroundColor Yellow
    Write-Host "  Flat fields: $($flatFields.Keys.Count)" -ForegroundColor Gray
    Write-Host "  Nested objects: $($nestedObjects.Keys.Count)" -ForegroundColor Gray
    Write-Host "  Array fields: $($arrayFields.Keys.Count)" -ForegroundColor Gray
    Write-Host ""
    
    # Generate main table
    Write-Host "Generating main table: $TableName" -ForegroundColor Green
    $mainTableSQL = New-TableDefinition -TableName $TableName `
                                       -Fields $flatFields `
                                       -PrimaryKeyField $PrimaryKeyField `
                                       -Schema $Schema `
                                       -IncludeDrop $IncludeDropStatements
    
    $result.Tables += $TableName
    $result.Statements += $mainTableSQL
    
    # Generate tables for nested objects
    foreach ($nestedPath in $nestedObjects.Keys) {
        $nestedTableName = "${TableName}_${nestedPath}"
        Write-Host "Generating nested table: $nestedTableName" -ForegroundColor Green
        
        # Get all fields that belong to this nested object
        $nestedFields = @{}
        foreach ($fieldPath in $Schema.Keys) {
            if ($fieldPath -like "$nestedPath.*" -and $fieldPath -notmatch '\[\]') {
                $shortName = $fieldPath.Replace("$nestedPath.", "")
                $nestedFields[$shortName] = $Schema[$fieldPath]
            }
        }
        
        if ($nestedFields.Keys.Count -gt 0) {
            $nestedTableSQL = New-NestedTableDefinition -TableName $nestedTableName `
                                                        -ParentTable $TableName `
                                                        -ParentKeyField $PrimaryKeyField `
                                                        -Fields $nestedFields `
                                                        -IncludeDrop $IncludeDropStatements
            
            $result.Tables += $nestedTableName
            $result.Statements += $nestedTableSQL
            $result.Relationships += "$nestedTableName -> $TableName (${PrimaryKeyField})"
        }
    }
    
    # Generate tables for arrays
    foreach ($arrayPath in $arrayFields.Keys) {
        $arrayTableName = "${TableName}_${arrayPath}"
        Write-Host "Generating array table: $arrayTableName" -ForegroundColor Green
        
        $arrayInfo = $arrayFields[$arrayPath]
        
        # Determine if array contains objects or primitives
        $hasObjects = $false
        if ($arrayInfo.ArrayElementTypes.ContainsKey('object')) {
            $hasObjects = $true
        }
        
        if ($hasObjects) {
            # Array of objects - get nested fields
            $arrayObjectFields = @{}
            foreach ($fieldPath in $Schema.Keys) {
                if ($fieldPath -like "${arrayPath}[].*") {
                    $shortName = $fieldPath -replace "^${arrayPath}\[\]\.", ""
                    $arrayObjectFields[$shortName] = $Schema[$fieldPath]
                }
            }
            
            $arrayTableSQL = New-ArrayObjectTableDefinition -TableName $arrayTableName `
                                                            -ParentTable $TableName `
                                                            -ParentKeyField $PrimaryKeyField `
                                                            -Fields $arrayObjectFields `
                                                            -IncludeDrop $IncludeDropStatements
        }
        else {
            # Array of primitives
            $arrayTableSQL = New-ArrayPrimitiveTableDefinition -TableName $arrayTableName `
                                                               -ParentTable $TableName `
                                                               -ParentKeyField $PrimaryKeyField `
                                                               -ArrayInfo $arrayInfo `
                                                               -IncludeDrop $IncludeDropStatements
        }
        
        $result.Tables += $arrayTableName
        $result.Statements += $arrayTableSQL
        $result.Relationships += "$arrayTableName -> $TableName (${PrimaryKeyField})"
    }
    
    # Display summary
    Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "Schema Generation Complete!" -ForegroundColor Green
    Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "Tables created: $($result.Tables.Count)" -ForegroundColor Gray
    $result.Tables | ForEach-Object { Write-Host "  - $_" -ForegroundColor Gray }
    
    if ($result.Relationships.Count -gt 0) {
        Write-Host "`nRelationships:" -ForegroundColor Yellow
        $result.Relationships | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
    }
    Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
    
    return $result
}

function New-TableDefinition {
    <#
    .SYNOPSIS
    Creates SQL for a main table with flat fields
    #>
    
    param (
        [string]$TableName,
        [hashtable]$Fields,
        [string]$PrimaryKeyField,
        [hashtable]$Schema,
        [bool]$IncludeDrop
    )
    
    $sql = ""
    
    if ($IncludeDrop) {
        $sql += "-- Drop table if exists`n"
        $sql += "IF OBJECT_ID('$TableName', 'U') IS NOT NULL DROP TABLE $TableName;`n`n"
    }
    
    $sql += "-- Main table: $TableName`n"
    $sql += "CREATE TABLE $TableName (`n"
    
    $columns = @()
    
    foreach ($fieldName in ($Fields.Keys | Sort-Object)) {
        $fieldInfo = $Fields[$fieldName]
        $sqlType = Convert-MongoTypeToSQL -FieldInfo $fieldInfo -FieldName $fieldName
        
        $columnDef = "    [$fieldName] $sqlType"
        
        # Add PRIMARY KEY constraint
        if ($fieldName -eq $PrimaryKeyField) {
            $columnDef += " PRIMARY KEY"
        }
        
        # Add NOT NULL for fields that appear in all documents
        if ($fieldInfo.Count -eq $Schema[$fieldName].Count) {
            $columnDef += " NOT NULL"
        }
        
        $columns += $columnDef
    }
    
    $sql += ($columns -join ",`n")
    $sql += "`n);`n"
    
    return $sql
}

function New-NestedTableDefinition {
    <#
    .SYNOPSIS
    Creates SQL for a nested object table
    #>
    
    param (
        [string]$TableName,
        [string]$ParentTable,
        [string]$ParentKeyField,
        [hashtable]$Fields,
        [bool]$IncludeDrop
    )
    
    $sql = ""
    
    if ($IncludeDrop) {
        $sql += "`n-- Drop table if exists`n"
        $sql += "IF OBJECT_ID('$TableName', 'U') IS NOT NULL DROP TABLE $TableName;`n`n"
    }
    
    $sql += "-- Nested object table: $TableName`n"
    $sql += "CREATE TABLE $TableName (`n"
    
    $columns = @()
    
    # Add ID column
    $columns += "    [id] INT IDENTITY(1,1) PRIMARY KEY"
    
    # Add foreign key to parent
    $columns += "    [${ParentTable}_${ParentKeyField}] VARCHAR(255) NOT NULL"
    
    # Add nested fields
    foreach ($fieldName in ($Fields.Keys | Sort-Object)) {
        $fieldInfo = $Fields[$fieldName]
        $sqlType = Convert-MongoTypeToSQL -FieldInfo $fieldInfo -FieldName $fieldName
        $columns += "    [$fieldName] $sqlType"
    }
    
    $sql += ($columns -join ",`n")
    $sql += ",`n"
    $sql += "    FOREIGN KEY ([${ParentTable}_${ParentKeyField}]) REFERENCES $ParentTable([$ParentKeyField])`n"
    $sql += ");`n"
    
    return $sql
}

function New-ArrayObjectTableDefinition {
    <#
    .SYNOPSIS
    Creates SQL for an array of objects table
    #>
    
    param (
        [string]$TableName,
        [string]$ParentTable,
        [string]$ParentKeyField,
        [hashtable]$Fields,
        [bool]$IncludeDrop
    )
    
    $sql = ""
    
    if ($IncludeDrop) {
        $sql += "`n-- Drop table if exists`n"
        $sql += "IF OBJECT_ID('$TableName', 'U') IS NOT NULL DROP TABLE $TableName;`n`n"
    }
    
    $sql += "-- Array of objects table: $TableName`n"
    $sql += "CREATE TABLE $TableName (`n"
    
    $columns = @()
    
    # Add ID column
    $columns += "    [id] INT IDENTITY(1,1) PRIMARY KEY"
    
    # Add foreign key to parent
    $columns += "    [${ParentTable}_${ParentKeyField}] VARCHAR(255) NOT NULL"
    
    # Add array index
    $columns += "    [array_index] INT NOT NULL"
    
    # Add fields from array objects
    foreach ($fieldName in ($Fields.Keys | Sort-Object)) {
        $fieldInfo = $Fields[$fieldName]
        $sqlType = Convert-MongoTypeToSQL -FieldInfo $fieldInfo -FieldName $fieldName
        $columns += "    [$fieldName] $sqlType"
    }
    
    $sql += ($columns -join ",`n")
    $sql += ",`n"
    $sql += "    FOREIGN KEY ([${ParentTable}_${ParentKeyField}]) REFERENCES $ParentTable([$ParentKeyField])`n"
    $sql += ");`n"
    
    return $sql
}

function New-ArrayPrimitiveTableDefinition {
    <#
    .SYNOPSIS
    Creates SQL for an array of primitive values table
    #>
    
    param (
        [string]$TableName,
        [string]$ParentTable,
        [string]$ParentKeyField,
        [hashtable]$ArrayInfo,
        [bool]$IncludeDrop
    )
    
    $sql = ""
    
    if ($IncludeDrop) {
        $sql += "`n-- Drop table if exists`n"
        $sql += "IF OBJECT_ID('$TableName', 'U') IS NOT NULL DROP TABLE $TableName;`n`n"
    }
    
    $sql += "-- Array of primitives table: $TableName`n"
    $sql += "CREATE TABLE $TableName (`n"
    
    $columns = @()
    
    # Add ID column
    $columns += "    [id] INT IDENTITY(1,1) PRIMARY KEY"
    
    # Add foreign key to parent
    $columns += "    [${ParentTable}_${ParentKeyField}] VARCHAR(255) NOT NULL"
    
    # Add array index
    $columns += "    [array_index] INT NOT NULL"
    
    # Determine value type from array element types
    $valueType = "VARCHAR(MAX)"
    if ($ArrayInfo.ArrayElementTypes.ContainsKey('integer')) {
        $valueType = "INT"
    }
    elseif ($ArrayInfo.ArrayElementTypes.ContainsKey('number')) {
        $valueType = "DECIMAL(18,2)"
    }
    elseif ($ArrayInfo.ArrayElementTypes.ContainsKey('boolean')) {
        $valueType = "BIT"
    }
    
    $columns += "    [value] $valueType"
    
    $sql += ($columns -join ",`n")
    $sql += ",`n"
    $sql += "    FOREIGN KEY ([${ParentTable}_${ParentKeyField}]) REFERENCES $ParentTable([$ParentKeyField])`n"
    $sql += ");`n"
    
    return $sql
}

function Convert-MongoTypeToSQL {
    <#
    .SYNOPSIS
    Converts MongoDB field types to appropriate SQL types
    #>
    
    param (
        [hashtable]$FieldInfo,
        [string]$FieldName
    )
    
    # Get the most common type for this field
    $primaryType = ($FieldInfo.Types.GetEnumerator() | Sort-Object -Property Value -Descending | Select-Object -First 1).Key
    
    # Special handling for _id field
    if ($FieldName -eq "_id") {
        return "VARCHAR(24)"
    }
    
    # Map MongoDB types to SQL types
    switch ($primaryType) {
        "string" {
            # Check sample values to estimate length
            $maxLength = 255
            if ($FieldInfo.SampleValues.Count -gt 0) {
                $maxSampleLength = ($FieldInfo.SampleValues | Measure-Object -Property Length -Maximum).Maximum
                if ($maxSampleLength -gt 255) {
                    $maxLength = "MAX"
                }
            }
            return "VARCHAR($maxLength)"
        }
        "integer" {
            return "INT"
        }
        "number" {
            return "DECIMAL(18,2)"
        }
        "boolean" {
            return "BIT"
        }
        "datetime" {
            return "DATETIME2"
        }
        "ObjectId" {
            return "VARCHAR(24)"
        }
        "null" {
            return "VARCHAR(255)"
        }
        default {
            return "VARCHAR(MAX)"
        }
    }
}

function Export-SQLSchema {
    <#
    .SYNOPSIS
    Exports SQL schema to a file
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        $SchemaResult,
        
        [Parameter(Mandatory=$true)]
        [string]$OutputPath
    )
    
    try {
        $content = "-- SQL Schema Generated by NoSQL-to-SQL Migration Tool`n"
        $content += "-- Generated on: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`n"
        $content += "-- Main Table: $($SchemaResult.MainTable)`n"
        $content += "-- Total Tables: $($SchemaResult.Tables.Count)`n`n"
        $content += "-- ═══════════════════════════════════════════════════════`n`n"
        
        foreach ($statement in $SchemaResult.Statements) {
            $content += $statement + "`n"
        }
        
        $content | Out-File -FilePath $OutputPath -Encoding UTF8
        
        Write-Host "SQL schema exported to: $OutputPath" -ForegroundColor Green
        return $true
    }
    catch {
        Write-Host "Error exporting SQL schema: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Example usage function
function Test-SQLSchemaGeneration {
    <#
    .SYNOPSIS
    Test function to generate SQL schema from MongoDB analysis
    #>
    
    # Load configuration
    $config = Get-AppConfig
    
    # Analyze MongoDB schema
    Write-Host "Step 1: Analyzing MongoDB collection..." -ForegroundColor Yellow
    $schema = Get-MongoDBSchema -ConnectionString $config.MongoDB.ConnectionString `
                                -DatabaseName $config.MongoDB.Database `
                                -CollectionName $config.MongoDB.Collection `
                                -SampleSize 100
    
    # Generate SQL schema
    Write-Host "`nStep 2: Generating SQL schema..." -ForegroundColor Yellow
    $sqlSchema = New-SQLSchema -Schema $schema `
                               -TableName $config.MongoDB.Collection `
                               -PrimaryKeyField "_id"
    
    # Display generated SQL
    Write-Host "`nGenerated SQL Statements:" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    foreach ($statement in $sqlSchema.Statements) {
        Write-Host $statement -ForegroundColor White
    }
    
    # Export to file
    $outputFile = ".\schema_$($config.MongoDB.Collection).sql"
    Export-SQLSchema -SchemaResult $sqlSchema -OutputPath $outputFile
    
    return $sqlSchema
}