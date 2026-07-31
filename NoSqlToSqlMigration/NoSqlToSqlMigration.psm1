function Get-MongoDBSchema {
    <#
    .SYNOPSIS
    Analyzes MongoDB collection structure and generates a schema overview
    
    .DESCRIPTION
    This function examines documents in a MongoDB collection to identify:
    - Field names and their data types
    - Nested structures (objects and arrays)
    - Field occurrence frequency
    - Sample values for each field
    
    .PARAMETER ConnectionString
    MongoDB connection string
    
    .PARAMETER DatabaseName
    Name of the MongoDB database
    
    .PARAMETER CollectionName
    Name of the collection to analyze
    
    .PARAMETER SampleSize
    Number of documents to sample for analysis (default: 100)
    
    .EXAMPLE
    $config = Get-AppConfig
    Get-MongoDBSchema -ConnectionString $config.MongoDB.ConnectionString -DatabaseName $config.MongoDB.Database -CollectionName "users" -SampleSize 50
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [string]$ConnectionString,
        
        [Parameter(Mandatory=$true)]
        [string]$DatabaseName,
        
        [Parameter(Mandatory=$true)]
        [string]$CollectionName,
        
        [Parameter(Mandatory=$false)]
        [int]$SampleSize = 100
    )
    
    try {
        Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "    MongoDB Schema Analysis - $CollectionName" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
        
        # Connect to MongoDB
        Write-Host "Connecting to MongoDB..." -ForegroundColor Yellow
        Connect-Mdbc -ConnectionString $ConnectionString -DatabaseName $DatabaseName -CollectionName $CollectionName
        
        # Get total document count
        $totalDocs = Get-MdbcData -Count
        Write-Host "Total documents in collection: $totalDocs" -ForegroundColor Gray
        
        # Determine actual sample size
        $actualSampleSize = [Math]::Min($SampleSize, $totalDocs)
        Write-Host "Analyzing $actualSampleSize documents...`n" -ForegroundColor Gray
        
        # Get sample documents
        $documents = Get-MdbcData -Last $actualSampleSize
        
        # Initialize schema structure
        $schema = @{}
        
        # Analyze each document
        $docCount = 0
        foreach ($doc in $documents) {
            $docCount++
            Write-Progress -Activity "Analyzing documents" -Status "Document $docCount of $actualSampleSize" -PercentComplete (($docCount / $actualSampleSize) * 100)
            
            # Debug: Check document type
            Write-Host "DEBUG: Document type: $($doc.GetType().FullName)" -ForegroundColor DarkGray
            
            # Convert to proper object if needed
            if ($doc -is [MongoDB.Bson.BsonDocument]) {
                $doc = [MongoDB.Bson.BsonTypeMapper]::MapToDotNetValue($doc)
            }
            
            Analyze-DocumentStructure -Document $doc -Schema $schema -Path "" -TotalDocs $actualSampleSize
        }
        
        Write-Progress -Activity "Analyzing documents" -Completed
        
        # Generate and display results
        Write-Host "Schema Analysis Results:" -ForegroundColor Green
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
        
        Display-SchemaResults -Schema $schema -TotalDocs $actualSampleSize
        
        # Return schema object for further processing
        return $schema
    }
    catch {
        Write-Host "Error during schema analysis: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

function Analyze-DocumentStructure {
    <#
    .SYNOPSIS
    Recursively analyzes document structure and updates schema
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        $Document,
        
        [Parameter(Mandatory=$true)]
        [hashtable]$Schema,
        
        [Parameter(Mandatory=$false)]
        [string]$Path = "",
        
        [Parameter(Mandatory=$true)]
        [int]$TotalDocs
    )
    
    # Safety check
    if ($null -eq $Document) {
        Write-Host "WARNING: Null document encountered at path: $Path" -ForegroundColor Yellow
        return
    }
    
    # Get properties based on document type
    $properties = @()
    
    if ($Document -is [System.Collections.IDictionary]) {
        # This handles Mdbc.Dictionary, Hashtable, and other dictionary types
        $properties = $Document.GetEnumerator() | ForEach-Object {
            [PSCustomObject]@{
                Name = $_.Key
                Value = $_.Value
            }
        }
    }
    elseif ($Document -is [PSCustomObject]) {
        $properties = $Document.PSObject.Properties | ForEach-Object {
            [PSCustomObject]@{
                Name = $_.Name
                Value = $_.Value
            }
        }
    }
    else {
        Write-Host "WARNING: Unexpected document type: $($Document.GetType().FullName)" -ForegroundColor Yellow
        return
    }
    
    foreach ($property in $properties) {
        $fieldName = $property.Name
        $fieldValue = $property.Value
        
        # Skip MongoDB internal fields if desired (optional)
        # if ($fieldName -eq "_id") { continue }
        
        # Create full path for nested fields
        $fullPath = if ($Path) { "$Path.$fieldName" } else { $fieldName }
        
        # Initialize field in schema if not exists
        if (-not $Schema.ContainsKey($fullPath)) {
            $Schema[$fullPath] = @{
                Types = @{}
                Count = 0
                IsNested = $false
                IsArray = $false
                SampleValues = @()
                ArrayElementTypes = @{}
                MaxLength = 0
                MaxElementLength = 0
            }
        }
        
        # Increment occurrence count
        $Schema[$fullPath].Count++
        
        # Determine and record type
        $fieldType = Get-FieldType -Value $fieldValue
        
        if ($Schema[$fullPath].Types.ContainsKey($fieldType)) {
            $Schema[$fullPath].Types[$fieldType]++
        } else {
            $Schema[$fullPath].Types[$fieldType] = 1
        }
        
        # Handle different data types
        # Note: dictionaries must be tested before IEnumerable, because every
        # dictionary is also enumerable and would otherwise look like an array
        if ($null -eq $fieldValue) {
            # Null value - already counted in types
        }
        elseif (Test-IsDocumentObject -Value $fieldValue) { #if it is a sub-document  than mark it as nested and analyze its structure
            # Nested object
            $Schema[$fullPath].IsNested = $true
            Analyze-DocumentStructure -Document $fieldValue -Schema $Schema -Path $fullPath -TotalDocs $TotalDocs
        }
        elseif ($fieldValue -is [System.Collections.IEnumerable] -and $fieldValue -isnot [string]) {
            # Array or collection
            $Schema[$fullPath].IsArray = $true

            foreach ($item in $fieldValue) {
                $itemType = Get-FieldType -Value $item

                if ($Schema[$fullPath].ArrayElementTypes.ContainsKey($itemType)) {
                    $Schema[$fullPath].ArrayElementTypes[$itemType]++
                } else {
                    $Schema[$fullPath].ArrayElementTypes[$itemType] = 1
                }

                # Track the longest element so the value column can be sized
                if ($null -ne $item -and -not (Test-IsDocumentObject -Value $item)) {
                    $itemLength = $item.ToString().Length
                    if ($itemLength -gt $Schema[$fullPath].MaxElementLength) {
                        $Schema[$fullPath].MaxElementLength = $itemLength
                    }
                }

                # Recursively analyze nested objects in arrays
                if (Test-IsDocumentObject -Value $item) {
                    $Schema[$fullPath].IsNested = $true
                    Analyze-DocumentStructure -Document $item -Schema $Schema -Path "$fullPath[]" -TotalDocs $TotalDocs
                }
            }
        }
        else {
            # Track the real (untruncated) length so column sizes fit the data
            $valueStr = $fieldValue.ToString()
            if ($valueStr.Length -gt $Schema[$fullPath].MaxLength) {
                $Schema[$fullPath].MaxLength = $valueStr.Length
            }

            # Store sample values (limit to 3 unique samples)
            if ($Schema[$fullPath].SampleValues.Count -lt 3) {
                if ($valueStr.Length -gt 50) {
                    $valueStr = $valueStr.Substring(0, 47) + "..."
                }
                if ($valueStr -notin $Schema[$fullPath].SampleValues) {
                    $Schema[$fullPath].SampleValues += $valueStr
                }
            }
        }
    }
}

function Test-IsDocumentObject {
    <#
    .SYNOPSIS
    Tells whether a value is a sub-document (object) rather than an array or scalar

    .DESCRIPTION
    MongoDB sub-documents arrive as Mdbc.Dictionary / BsonDocument / hashtable,
    all of which implement IDictionary. Because IDictionary is also IEnumerable,
    this test must be used before any array test, otherwise sub-documents are
    mistaken for arrays.
    #>

    param (
        $Value
    )

    if ($null -eq $Value) {
        return $false
    }

    return ($Value -is [System.Collections.IDictionary] -or $Value -is [PSCustomObject])
}

function Get-FieldType {
    <#
    .SYNOPSIS
    Determines the data type of a field value
    #>

    param (
        $Value
    )

    if ($null -eq $Value) {
        return "null"
    }
    elseif ($Value -is [string]) {
        return "string"
    }
    elseif ($Value -is [int] -or $Value -is [int32] -or $Value -is [int64]) {
        return "integer"
    }
    elseif ($Value -is [double] -or $Value -is [float] -or $Value -is [decimal]) {
        return "number"
    }
    elseif ($Value -is [bool]) {
        return "boolean"
    }
    elseif ($Value -is [datetime]) {
        return "datetime"
    }
    elseif (Test-IsDocumentObject -Value $Value) {
        return "object"
    }
    elseif ($Value -is [System.Collections.IEnumerable]) {
        return "array"
    }
    else {
        return $Value.GetType().Name
    }
}

function Display-SchemaResults {
    <#
    .SYNOPSIS
    Displays the schema analysis results in a readable format
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [hashtable]$Schema,
        
        [Parameter(Mandatory=$true)]
        [int]$TotalDocs
    )
    
    # Check if schema is empty
    if ($Schema.Keys.Count -eq 0) {
        Write-Host "No fields found in the analyzed documents." -ForegroundColor Yellow
        return
    }
    
    # Sort fields by path
    $sortedFields = $Schema.Keys | Sort-Object
    
    foreach ($fieldPath in $sortedFields) {
        $fieldInfo = $Schema[$fieldPath]
        
        # Safety check
        if ($null -eq $fieldInfo -or $null -eq $fieldInfo.Types) {
            Write-Host "WARNING: Invalid field info for $fieldPath" -ForegroundColor Yellow
            continue
        }
        
        $percentage = [math]::Round(($fieldInfo.Count / $TotalDocs) * 100, 1)
        
        # Field name and occurrence
        Write-Host "Field: " -NoNewline -ForegroundColor Cyan
        Write-Host $fieldPath -ForegroundColor White
        Write-Host "  Occurrence: $($fieldInfo.Count)/$TotalDocs ($percentage%)" -ForegroundColor Gray
        
        # Types
        Write-Host "  Types: " -NoNewline -ForegroundColor Gray
        $typeStrings = $fieldInfo.Types.GetEnumerator() | ForEach-Object {
            "$($_.Key) ($($_.Value))"
        }
        Write-Host ($typeStrings -join ", ") -ForegroundColor Yellow
        
        # Array information
        if ($fieldInfo.IsArray) {
            Write-Host "  Array Element Types: " -NoNewline -ForegroundColor Gray
            $arrayTypeStrings = $fieldInfo.ArrayElementTypes.GetEnumerator() | ForEach-Object {
                "$($_.Key) ($($_.Value))"
            }
            Write-Host ($arrayTypeStrings -join ", ") -ForegroundColor Magenta
        }
        
        # Nested indicator
        if ($fieldInfo.IsNested) {
            Write-Host "  [NESTED STRUCTURE]" -ForegroundColor Red
        }
        
        # Sample values
        if ($fieldInfo.SampleValues.Count -gt 0) {
            Write-Host "  Samples: " -NoNewline -ForegroundColor Gray
            Write-Host ($fieldInfo.SampleValues -join " | ") -ForegroundColor DarkGray
        }
        
        Write-Host ""
    }
    
    # Summary statistics
    Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "Summary:" -ForegroundColor Green
    Write-Host "  Total unique fields: $($Schema.Keys.Count)" -ForegroundColor Gray
    
    $nestedFields = ($Schema.Values | Where-Object { $_.IsNested }).Count
    $arrayFields = ($Schema.Values | Where-Object { $_.IsArray }).Count
    
    Write-Host "  Nested structures: $nestedFields" -ForegroundColor Gray
    Write-Host "  Array fields: $arrayFields" -ForegroundColor Gray
    Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
}

# Example usage function
function Test-SchemaAnalysis {
    <#
    .SYNOPSIS
    Test function to run schema analysis with config.json
    #>
    
    # Load configuration
    $config = Get-AppConfig
    
    # Run analysis
    $schema = Get-MongoDBSchema -ConnectionString $config.MongoDB.ConnectionString `
                                -DatabaseName $config.MongoDB.Database `
                                -CollectionName $config.MongoDB.Collection `
                                -SampleSize 100
    
    return $schema
}


<#
.SYNOPSIS
Loads application configuration from a JSON file.

.DESCRIPTION
Reads and parses the configuration file containing database connection settings
for MongoDB, MySQL, and SQL Server. The configuration file should be in JSON format
and contain connection strings, credentials, and other required settings.

.PARAMETER Path
The path to the configuration file. Defaults to config.json in the script's directory.

#>
function Get-AppConfig {
    param(
        [string]$Path = "$PSScriptRoot\..\config.json"    )

    if (-not (Test-Path $Path)) {
        throw "Config file not found: $Path"
    }

    return Get-Content $Path -Raw | ConvertFrom-Json
}

<#
.SYNOPSIS
Database Connection Initialization and Testing
.DESCRIPTION
The script:
- Loads database configuration settings via a configuration loader (Get-AppConfig)
- Tests the connection to MongoDB (optionally including a collection)
- Tests the connection to MySQL or SQL Server
- Provides clear status and error messages in the console
- Initializes and validates all required database connections
- Returns reusable SQL connection objects for subsequent operations

.FUNCTIONALITY
To ensure that all database connections are correctly configured and operational before executing migration or data processing steps.
#>

    function Test-MongoDBConnection {
        param (
            [Parameter(Mandatory)]
            [string]$ConnectionString,

            [Parameter(Mandatory)]
            [string]$DatabaseName,

            [string]$CollectionName
        )

        try {
            Write-Host "Testing MongoDB connection..." -ForegroundColor Cyan

            if ($CollectionName) {
                Connect-Mdbc -ConnectionString $ConnectionString `
                            -DatabaseName $DatabaseName `
                            -CollectionName $CollectionName
            }
            else {
                Connect-Mdbc -ConnectionString $ConnectionString `
                            -DatabaseName $DatabaseName
            }

            if ($CollectionName) {
                $count = Get-MdbcData -Count
            }
            else {
                # Alleen testen of connectie werkt
                $count = 0
            }


            Write-Host "MongoDB connection successful!" -ForegroundColor Green
            Write-Host "Database: $DatabaseName" -ForegroundColor Gray
            if ($CollectionName) {
                Write-Host "Collection: $CollectionName" -ForegroundColor Gray
            }
            Write-Host "Document count: $count" -ForegroundColor Gray

            return $true
        }
        catch {
            Write-Host "MongoDB connection failed!" -ForegroundColor Red
            Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
            return $false
        }
    }

    
    function Initialize-MySQLAssembly {
        <#
        .SYNOPSIS
        Loads the MySQL Connector/NET assembly

        .DESCRIPTION
        Looks for MySql.Data.dll instead of hardcoding one connector version:
        a copy next to the module first, then any installed connector version
        (newest first), and finally the assembly name itself.
        #>

        if ('MySql.Data.MySqlClient.MySqlConnection' -as [type]) {
            return $true
        }

        $candidates = @()
        $candidates += Join-Path $PSScriptRoot "lib\MySql.Data.dll"

        foreach ($programFiles in @($env:ProgramFiles, ${env:ProgramFiles(x86)})) {
            if (-not $programFiles) { continue }

            $mysqlRoot = Join-Path $programFiles "MySQL"
            if (Test-Path $mysqlRoot) {
                $candidates += Get-ChildItem -Path $mysqlRoot -Filter "MySql.Data.dll" -Recurse -ErrorAction SilentlyContinue |
                               Sort-Object FullName -Descending |
                               Select-Object -ExpandProperty FullName
            }
        }

        foreach ($path in $candidates) {
            if ($path -and (Test-Path $path)) {
                try {
                    Add-Type -Path $path -ErrorAction Stop
                    return $true
                }
                catch {
                    # Try the next candidate
                }
            }
        }

        try {
            Add-Type -AssemblyName "MySql.Data" -ErrorAction Stop
            return $true
        }
        catch {
            return $false
        }
    }

    # MySQL Connection Test
    function Test-MySQLConnection {
        param (
            [Parameter(Mandatory)]
            [string]$Server,

            [Parameter(Mandatory)]
            [string]$Database,

            [int]$Port = 3306,

            [string]$Username,

            [string]$Password
        )

        try {
            Write-Host "Testing MySQL connection..." -ForegroundColor Cyan

            $connectionString = "Server=$Server;Port=$Port;Database=$Database;"

            if ($Username -and $Password) {
                $connectionString += "Uid=$Username;Pwd=$Password;"
            }

            $connectionString += "SslMode=Disabled;AllowPublicKeyRetrieval=True;"

            if (-not (Initialize-MySQLAssembly)) {
                throw "MySql.Data connector not found. Please install MySQL Connector/NET."
            }

            $connection = New-Object MySql.Data.MySqlClient.MySqlConnection
            $connection.ConnectionString = $connectionString
            $connection.Open()

            Write-Host "MySQL connection successful!" -ForegroundColor Green
            Write-Host "Server: $Server`:$Port" -ForegroundColor Gray
            Write-Host "Database: $Database" -ForegroundColor Gray
            Write-Host "Version: $($connection.ServerVersion)" -ForegroundColor Gray

            $connection.Close()
            return $true
        }
        catch {
            Write-Host "MySQL connection failed!" -ForegroundColor Red
            Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
            return $false
        }
    }

    
    # SQL Server Connection Test
    function Test-SQLServerConnection {
        param (
            [Parameter(Mandatory)]
            [string]$Server,

            [Parameter(Mandatory)]
            [string]$Database,

            [string]$Username,

            [string]$Password
        )

        try {
            Write-Host "Testing SQL Server connection..." -ForegroundColor Cyan

            if ($Username -and $Password) {
                $connectionString = "Server=$Server;Database=$Database;User Id=$Username;Password=$Password;"
            }
            else {
                $connectionString = "Server=$Server;Database=$Database;Integrated Security=True;"
            }

            $connection = New-Object System.Data.SqlClient.SqlConnection
            $connection.ConnectionString = $connectionString
            $connection.Open()
            $connection.Close()

            Write-Host "SQL Server connection successful!" -ForegroundColor Green
            Write-Host "Server: $Server" -ForegroundColor Gray
            Write-Host "Database: $Database" -ForegroundColor Gray

            return $true
        }
        catch {
            Write-Host "SQL Server connection failed!" -ForegroundColor Red
            Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
            return $false
        }
    }

    # Initialize All Database Connections
    function Initialize-DatabaseConnections {
        param(
            [ValidateSet("MySQL", "SQLServer")]
            [string]$DatabaseType = "MySQL"
        )

        Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "    NoSQL to SQL Migration Tool - Connection Test" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan

        try {
            $config = Get-AppConfig
        }
        catch {
            Write-Host $_.Exception.Message -ForegroundColor Red
            return $false
        }

        # MongoDB
        $mongoOk = Test-MongoDBConnection `
            -ConnectionString $config.MongoDB.ConnectionString `
            -DatabaseName $config.MongoDB.Database `
            -CollectionName $config.MongoDB.Collection

        Write-Host ""

        # SQL / MySQL
        if ($DatabaseType -eq "MySQL") {
            $sqlOk = Test-MySQLConnection `
                -Server $config.MySQL.Server `
                -Database $config.MySQL.Database `
                -Port $config.MySQL.Port `
                -Username $config.MySQL.Username `
                -Password $config.MySQL.Password
        }
        else {
            $sqlOk = Test-SQLServerConnection `
                -Server $config.SQLServer.Server `
                -Database $config.SQLServer.Database `
                -Username $config.SQLServer.Username `
                -Password $config.SQLServer.Password
        }

        Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan

        if ($mongoOk -and $sqlOk) {
            Write-Host "All database connections are successful!" -ForegroundColor Green
            Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
            return $true
        }

        Write-Host "One or more database connections failed!" -ForegroundColor Red
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
        return $false
    }


    # Get SQL Connection Object
    function Get-SQLConnection {
        param(
            [Parameter(Mandatory)]
            $Config,

            [ValidateSet("MySQL", "SQLServer")]
            [string]$DatabaseType = "MySQL"
        )

        if ($DatabaseType -eq "MySQL") {
            $connectionString = "Server=$($Config.MySQL.Server);Port=$($Config.MySQL.Port);Database=$($Config.MySQL.Database);"
            if ($Config.MySQL.Username -and $Config.MySQL.Password) {
                $connectionString += "Uid=$($Config.MySQL.Username);Pwd=$($Config.MySQL.Password);"
            }
            $connectionString += "SslMode=Disabled;AllowPublicKeyRetrieval=True;"

            if (-not (Initialize-MySQLAssembly)) {
                throw "MySql.Data connector not found. Please install MySQL Connector/NET."
            }

            $conn = New-Object MySql.Data.MySqlClient.MySqlConnection
            $conn.ConnectionString = $connectionString
            return $conn
        }
        else {
            if ($Config.SQLServer.Username -and $Config.SQLServer.Password) {
                $connectionString = "Server=$($Config.SQLServer.Server);Database=$($Config.SQLServer.Database);User Id=$($Config.SQLServer.Username);Password=$($Config.SQLServer.Password);"
            }
            else {
                $connectionString = "Server=$($Config.SQLServer.Server);Database=$($Config.SQLServer.Database);Integrated Security=True;"
            }

            $conn = New-Object System.Data.SqlClient.SqlConnection
            $conn.ConnectionString = $connectionString
            return $conn
        }
    }

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

        # Column layout is read back from the database while inserting;
        # start with an empty cache because the tables are recreated below
        $script:N2STableColumns = @{}

        # Child tables reference the main table, so drops must ignore the
        # foreign keys while the tables are being recreated
        if ($DatabaseType -eq "MySQL") {
            Invoke-SQLNonQuery -Connection $sqlConnection -CommandText "SET FOREIGN_KEY_CHECKS = 0" | Out-Null
        }

        foreach ($statement in $SQLSchema.Statements) {
            # Convert SQL Server syntax to MySQL if needed
            $targetStatement = $statement
            if ($DatabaseType -eq "MySQL") {
                $targetStatement = Convert-ToMySQLSyntax -SQLStatement $statement
            }

            # A statement block contains a DROP and a CREATE; send them separately
            # so no batching support is needed from the database driver
            foreach ($singleStatement in (Split-SQLStatement -SQLText $targetStatement)) {
                try {
                    Invoke-SQLNonQuery -Connection $sqlConnection -CommandText $singleStatement | Out-Null

                    if ($singleStatement -match 'CREATE TABLE\s+[`\[]?(\w+)') {
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
        }

        if ($DatabaseType -eq "MySQL") {
            Invoke-SQLNonQuery -Connection $sqlConnection -CommandText "SET FOREIGN_KEY_CHECKS = 1" | Out-Null
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
                                                        -DatabaseType $DatabaseType `
                                                        -SQLSchema $SQLSchema

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

        # Read back the real row counts so child tables are reported too
        foreach ($table in $SQLSchema.Tables) {
            $rowCount = Get-SQLTableRowCount -Connection $sqlConnection -TableName $table
            if ($null -ne $rowCount) {
                $migrationResult.RecordsInserted[$table] = $rowCount
            }
        }

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

function Invoke-SQLNonQuery {
    <#
    .SYNOPSIS
    Executes a single SQL statement without result set
    #>

    param (
        $Connection,
        [string]$CommandText
    )

    $cmd = $Connection.CreateCommand()
    $cmd.CommandText = $CommandText
    return $cmd.ExecuteNonQuery()
}

function Split-SQLStatement {
    <#
    .SYNOPSIS
    Splits a generated SQL block into separate statements

    .DESCRIPTION
    The schema generator emits a DROP and a CREATE in one block. Sending them as
    one command would rely on multi statement support in the database driver, so
    they are split here. Fragments that only contain comments are dropped.
    #>

    param (
        [string]$SQLText
    )

    $statements = @()

    foreach ($part in ($SQLText -split ';')) {
        $codeLines = $part -split "`n" | Where-Object {
            $_.Trim() -ne '' -and -not $_.Trim().StartsWith('--')
        }

        if ($codeLines.Count -gt 0) {
            $statements += $part.Trim()
        }
    }

    return $statements
}

function Get-SQLTableRowCount {
    <#
    .SYNOPSIS
    Returns the number of rows in a table, or $null when it cannot be read
    #>

    param (
        $Connection,
        [string]$TableName
    )

    try {
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = 'SELECT COUNT(*) FROM `' + $TableName + '`'
        return [int]$cmd.ExecuteScalar()
    }
    catch {
        return $null
    }
}

function Get-SQLTableColumns {
    <#
    .SYNOPSIS
    Returns the columns of a table as a lookup hashtable (cached per run)

    .DESCRIPTION
    The SQL schema is built from a sample of the collection, so a document can
    contain a field that has no column. Checking the real table layout keeps
    those documents from failing on an unknown column.
    #>

    param (
        $Connection,
        [string]$TableName
    )

    if ($null -eq $script:N2STableColumns) {
        $script:N2STableColumns = @{}
    }

    if ($script:N2STableColumns.ContainsKey($TableName)) {
        return $script:N2STableColumns[$TableName]
    }

    $columns = @{}

    try {
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = 'SHOW COLUMNS FROM `' + $TableName + '`'
        $reader = $cmd.ExecuteReader()

        while ($reader.Read()) {
            $columns[$reader.GetString(0)] = $true
        }
        $reader.Close()

        $script:N2STableColumns[$TableName] = $columns
    }
    catch {
        Write-Host "Warning: could not read columns of table $TableName : $($_.Exception.Message)" -ForegroundColor Yellow
    }

    return $columns
}

function ConvertTo-FlatRow {
    <#
    .SYNOPSIS
    Flattens a sub-document into column name / value pairs

    .DESCRIPTION
    Deeper sub-documents are flattened with a dotted column name, which matches
    the column names generated for nested objects. Arrays inside a sub-document
    are skipped: they have no table of their own.
    #>

    param (
        $Object,
        [string]$Prefix = ""
    )

    $row = [ordered]@{}

    if ($null -eq $Object) {
        return $row
    }

    $entries = @()

    if ($Object -is [System.Collections.IDictionary]) {
        foreach ($key in $Object.Keys) {
            $entries += [PSCustomObject]@{ Name = $key; Value = $Object[$key] }
        }
    }
    elseif ($Object -is [PSCustomObject]) {
        foreach ($property in $Object.PSObject.Properties) {
            $entries += [PSCustomObject]@{ Name = $property.Name; Value = $property.Value }
        }
    }

    foreach ($entry in $entries) {
        $columnName = "$Prefix$($entry.Name)"
        $value = $entry.Value

        if (Test-IsDocumentObject -Value $value) {
            $nestedRow = ConvertTo-FlatRow -Object $value -Prefix "$columnName."
            foreach ($nested in $nestedRow.GetEnumerator()) {
                $row[$nested.Key] = $nested.Value
            }
        }
        elseif ($null -ne $value -and $value -is [System.Collections.IEnumerable] -and $value -isnot [string]) {
            continue
        }
        else {
            $row[$columnName] = $value
        }
    }

    return $row
}

function Add-SQLRow {
    <#
    .SYNOPSIS
    Inserts one row, built from an ordered column / value map
    #>

    param (
        $Connection,
        [string]$TableName,
        $Row,
        [switch]$Replace
    )

    $columns = @($Row.Keys)
    $columnList = ($columns | ForEach-Object { '`' + $_ + '`' }) -join ', '
    $placeholders = ($columns | ForEach-Object { '?' }) -join ', '
    $verb = if ($Replace) { 'REPLACE INTO' } else { 'INSERT INTO' }

    $cmd = $Connection.CreateCommand()
    $cmd.CommandText = "$verb " + '`' + $TableName + '` (' + $columnList + ') VALUES (' + $placeholders + ')'

    foreach ($column in $columns) {
        $param = $cmd.CreateParameter()
        $param.Value = $Row[$column]
        $cmd.Parameters.Add($param) | Out-Null
    }

    $cmd.ExecuteNonQuery() | Out-Null
}

function Invoke-ChildTableMigration {
    <#
    .SYNOPSIS
    Writes the array or sub-document of one parent document to its child table
    #>

    param (
        $Connection,
        [string]$ChildTable,
        [string]$ParentKeyColumn,
        $ParentId,
        $Value,
        [string]$DatabaseType
    )

    $childColumns = Get-SQLTableColumns -Connection $Connection -TableName $ChildTable
    if ($childColumns.Count -eq 0) {
        return 0
    }

    # Remove rows of a previous run for this parent, so re-running stays idempotent
    $delete = $Connection.CreateCommand()
    $delete.CommandText = 'DELETE FROM `' + $ChildTable + '` WHERE `' + $ParentKeyColumn + '` = ?'
    $deleteParam = $delete.CreateParameter()
    $deleteParam.Value = $ParentId
    $delete.Parameters.Add($deleteParam) | Out-Null
    $delete.ExecuteNonQuery() | Out-Null

    $rowsWritten = 0

    if (Test-IsDocumentObject -Value $Value) {
        # Sub-document: exactly one child row
        $row = [ordered]@{}
        $row[$ParentKeyColumn] = $ParentId

        foreach ($entry in (ConvertTo-FlatRow -Object $Value).GetEnumerator()) {
            if ($childColumns.ContainsKey($entry.Key)) {
                $row[$entry.Key] = Convert-ToSQLValue -Value $entry.Value -DatabaseType $DatabaseType
            }
        }

        Add-SQLRow -Connection $Connection -TableName $ChildTable -Row $row
        $rowsWritten++
    }
    else {
        # Array: one child row per element, position kept in array_index
        $index = 0

        foreach ($item in $Value) {
            $row = [ordered]@{}
            $row[$ParentKeyColumn] = $ParentId

            if ($childColumns.ContainsKey('array_index')) {
                $row['array_index'] = $index
            }

            if (Test-IsDocumentObject -Value $item) {
                foreach ($entry in (ConvertTo-FlatRow -Object $item).GetEnumerator()) {
                    if ($childColumns.ContainsKey($entry.Key)) {
                        $row[$entry.Key] = Convert-ToSQLValue -Value $entry.Value -DatabaseType $DatabaseType
                    }
                }
            }
            elseif ($childColumns.ContainsKey('value')) {
                $row['value'] = Convert-ToSQLValue -Value $item -DatabaseType $DatabaseType
            }

            Add-SQLRow -Connection $Connection -TableName $ChildTable -Row $row
            $rowsWritten++
            $index++
        }
    }

    return $rowsWritten
}

function Invoke-DocumentMigration {
    <#
    .SYNOPSIS
    Migrates a single MongoDB document to SQL

    .DESCRIPTION
    Scalar fields become a row in the main table. Arrays and sub-documents are
    written to their own child table, linked by the parent _id.
    #>

    param (
        $Document,
        $Connection,
        [string]$TableName,
        [hashtable]$Schema,
        [string]$DatabaseType,
        $SQLSchema = $null,
        [string]$PrimaryKeyField = "_id"
    )

    try {
        # Split the document: scalars for the main table, arrays and
        # sub-documents for the child tables
        $flatFields = @{}
        $childFields = @{}

        if ($Document -is [System.Collections.IDictionary]) {
            foreach ($key in $Document.Keys) {
                $value = $Document[$key]

                if ((Test-IsDocumentObject -Value $value) -or
                    ($null -ne $value -and $value -is [System.Collections.IEnumerable] -and $value -isnot [string])) {
                    $childFields[$key] = $value
                }
                else {
                    $flatFields[$key] = $value
                }
            }
        }

        # Write only fields that have a column in the table
        $tableColumns = Get-SQLTableColumns -Connection $Connection -TableName $TableName

        $row = [ordered]@{}
        foreach ($field in $flatFields.Keys) {
            if ($tableColumns.Count -eq 0 -or $tableColumns.ContainsKey($field)) {
                $row[$field] = Convert-ToSQLValue -Value $flatFields[$field] -DatabaseType $DatabaseType
            }
        }

        if ($row.Count -eq 0) {
            Write-Host "Error migrating document: no matching columns in table $TableName" -ForegroundColor Red
            return $false
        }

        # REPLACE INTO instead of INSERT INTO to handle duplicates
        Add-SQLRow -Connection $Connection -TableName $TableName -Row $row -Replace

        # Child tables for arrays and sub-documents
        if ($null -ne $SQLSchema -and $childFields.Count -gt 0) {
            $parentId = Convert-ToSQLValue -Value $Document[$PrimaryKeyField] -DatabaseType $DatabaseType
            $parentKeyColumn = "${TableName}_${PrimaryKeyField}"

            foreach ($field in $childFields.Keys) {
                $childTable = "${TableName}_${field}"

                if ($SQLSchema.Tables -notcontains $childTable) {
                    continue
                }

                Invoke-ChildTableMigration -Connection $Connection `
                                           -ChildTable $childTable `
                                           -ParentKeyColumn $parentKeyColumn `
                                           -ParentId $parentId `
                                           -Value $childFields[$field] `
                                           -DatabaseType $DatabaseType | Out-Null
            }
        }

        return $true
    }
    catch {
        Write-Host "Error migrating document: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Convert-ToSQLValue {
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
    
    # Handle Boolean - FIXED
    if ($Value -is [bool]) {
        if ($DatabaseType -eq "MySQL") {
            if ($Value) { 
                return 1 
            } else { 
                return 0 
            }
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

    # Turn the T-SQL existence check into MySQL's DROP TABLE IF EXISTS
    $mysqlStatement = $mysqlStatement -replace "IF OBJECT_ID\('[^']+',\s*'U'\)\s*IS NOT NULL\s*DROP TABLE\s*", "DROP TABLE IF EXISTS "

    # Replace square brackets with backticks
    $mysqlStatement = $mysqlStatement -replace '\[', '`'
    $mysqlStatement = $mysqlStatement -replace '\]', '`'

    # Replace IDENTITY with AUTO_INCREMENT
    $mysqlStatement = $mysqlStatement -replace 'INT IDENTITY\(1,1\)', 'INT AUTO_INCREMENT'

    # MySQL has no VARCHAR(MAX): unbounded text becomes LONGTEXT
    $mysqlStatement = $mysqlStatement -replace '\bN?VARCHAR\s*\(\s*MAX\s*\)', 'LONGTEXT'

    # Replace BIT with TINYINT(1) for booleans
    $mysqlStatement = $mysqlStatement -replace '\sBIT\b', ' TINYINT(1)'

    # Replace DATETIME2 with DATETIME
    $mysqlStatement = $mysqlStatement -replace 'DATETIME2', 'DATETIME'

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
        
        # Load MySQL DLL (any installed connector version)
        if (-not (Initialize-MySQLAssembly)) {
            throw "MySql.Data connector not found. Please install MySQL Connector/NET."
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

        # Skip fields that are inside arrays (they have [] in path);
        # they are handled together with their array below
        if ($fieldPath -match '\[\]') {
            continue
        }

        # Skip fields of a nested object (dotted path);
        # they are handled together with their parent object below
        if ($fieldPath -like '*.*') {
            continue
        }

        # Top level field: an array becomes a child table, a sub-document
        # becomes its own table, everything else is a column of the main table
        if ($fieldInfo.IsArray) {
            $arrayFields[$fieldPath] = $fieldInfo
        }
        elseif ($fieldInfo.IsNested) {
            $nestedObjects[$fieldPath] = $fieldInfo
        }
        else {
            $flatFields[$fieldPath] = $fieldInfo
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
            # (-like is not usable here: [] is a wildcard character class)
            $arrayObjectFields = @{}
            $arrayPrefix = "$arrayPath[]."
            foreach ($fieldPath in $Schema.Keys) {
                if ($fieldPath.StartsWith($arrayPrefix)) {
                    $shortName = $fieldPath.Substring($arrayPrefix.Length)
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
        $sql += "IF OBJECT_ID('$TableName', 'U') IS NOT NULL DROP TABLE [$TableName];`n`n"
    }

    $sql += "-- Main table: $TableName`n"
    $sql += "CREATE TABLE [$TableName] (`n"

    $columns = @()

    foreach ($fieldName in ($Fields.Keys | Sort-Object)) {
        $fieldInfo = $Fields[$fieldName]
        $sqlType = Convert-MongoTypeToSQL -FieldInfo $fieldInfo -FieldName $fieldName

        $columnDef = "    [$fieldName] $sqlType"

        # Only the primary key is NOT NULL. The schema is derived from a sample,
        # so a field seen in every sampled document can still be missing from
        # documents outside the sample - MongoDB has no schema guarantee.
        if ($fieldName -eq $PrimaryKeyField) {
            $columnDef += " PRIMARY KEY NOT NULL"
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
        $sql += "IF OBJECT_ID('$TableName', 'U') IS NOT NULL DROP TABLE [$TableName];`n`n"
    }
    
    $sql += "-- Nested object table: $TableName`n"
    $sql += "CREATE TABLE [$TableName] (`n"
    
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
    $sql += "    FOREIGN KEY ([${ParentTable}_${ParentKeyField}]) REFERENCES [$ParentTable]([$ParentKeyField])`n"
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
        $sql += "IF OBJECT_ID('$TableName', 'U') IS NOT NULL DROP TABLE [$TableName];`n`n"
    }
    
    $sql += "-- Array of objects table: $TableName`n"
    $sql += "CREATE TABLE [$TableName] (`n"
    
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
    $sql += "    FOREIGN KEY ([${ParentTable}_${ParentKeyField}]) REFERENCES [$ParentTable]([$ParentKeyField])`n"
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
        $sql += "IF OBJECT_ID('$TableName', 'U') IS NOT NULL DROP TABLE [$TableName];`n`n"
    }
    
    $sql += "-- Array of primitives table: $TableName`n"
    $sql += "CREATE TABLE [$TableName] (`n"
    
    $columns = @()
    
    # Add ID column
    $columns += "    [id] INT IDENTITY(1,1) PRIMARY KEY"
    
    # Add foreign key to parent
    $columns += "    [${ParentTable}_${ParentKeyField}] VARCHAR(255) NOT NULL"
    
    # Add array index
    $columns += "    [array_index] INT NOT NULL"
    
    # Determine value type from array element types.
    # Mixed element types fall back to text, so no value can be rejected.
    $elementTypes = @($ArrayInfo.ArrayElementTypes.Keys | Where-Object { $_ -ne 'null' })
    $valueType = "VARCHAR(MAX)"

    if ($elementTypes.Count -eq 1) {
        switch ($elementTypes[0]) {
            'integer'  { $valueType = "INT" }
            'number'   { $valueType = "DECIMAL(18,2)" }
            'boolean'  { $valueType = "BIT" }
            'datetime' { $valueType = "DATETIME2" }
            'ObjectId' { $valueType = "VARCHAR(24)" }
            'string'   {
                if ($ArrayInfo.MaxElementLength -gt 255) {
                    $valueType = "VARCHAR(MAX)"
                } else {
                    $valueType = "VARCHAR(255)"
                }
            }
        }
    }
    elseif ($elementTypes.Count -gt 1 -and @($elementTypes | Where-Object { $_ -notin @('integer', 'number') }).Count -eq 0) {
        # Only numeric elements, but mixed integer/number
        $valueType = "DECIMAL(18,2)"
    }


    $columns += "    [value] $valueType"
    
    $sql += ($columns -join ",`n")
    $sql += ",`n"
    $sql += "    FOREIGN KEY ([${ParentTable}_${ParentKeyField}]) REFERENCES [$ParentTable]([$ParentKeyField])`n"
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
            # Size the column on the longest value seen during analysis.
            # SampleValues cannot be used for this: they are truncated to 50
            # characters for display, which made every string fit VARCHAR(255)
            # and long text (for example a storyline) fail on insert.
            $maxLength = 255
            if ($FieldInfo.MaxLength -gt 255) {
                $maxLength = "MAX"
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

function Test-MigrationValidation {
    <#
    .SYNOPSIS
    Validates data migration from MongoDB to SQL database
    
    .DESCRIPTION
    Performs comprehensive validation of migrated data:
    - Compares record counts betweaen source and destination
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
        if ($Value) { 
            return "1" 
        } else { 
            return "0" 
        }
    }
    
    # Handle numbers: compare on numeric value, not on formatting.
    # MySQL returns DECIMAL(18,2) as 8.30 where MongoDB has the double 8.3,
    # and the local culture writes the separator as a comma.
    if ($Value -is [int] -or $Value -is [long] -or $Value -is [double] -or $Value -is [decimal]) {
        return ([double]$Value).ToString([System.Globalization.CultureInfo]::InvariantCulture)
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
        "String" {
            # A value longer than 255 characters does not fit VARCHAR(255)
            if ($Value.Length -gt 255) {
                return "LONGTEXT"
            }
            return "VARCHAR(255)"
        }
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
        $discoveredCollections = @(Get-MongoDBCollections)
        
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
    
    $collections = @(Get-MongoDBCollections)
    
    if ($collections.Count -eq 0) {
        Write-Host "`n No collections found!" -ForegroundColor Red
        return
    }
    
    Write-Host "`n Found $($collections.Count) collection(s):" -ForegroundColor Green
    Write-Host ""
    
    foreach ($collectionName in $collections) {
        # Get document count
        try {
            Connect-Mdbc -ConnectionString $AppConfig.MongoDB.ConnectionString `
                         -DatabaseName $AppConfig.MongoDB.Database `
                         -CollectionName $collectionName
            
            $count = Get-MdbcData -Count
            Write-Host "  • " -NoNewline -ForegroundColor Cyan
            Write-Host "$collectionName " -NoNewline -ForegroundColor White
            Write-Host "($count documents)" -ForegroundColor Gray
        }
        catch {
            Write-Host "  • $collectionName (error reading count)" -ForegroundColor Yellow
        }
    }
}

function Menu-MigrateSingle {
    Write-Host "`n" + ("="*60) -ForegroundColor Cyan
    Write-Host "Migrate Single Collection" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan
    
    $collections = @(Get-MongoDBCollections)
    
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
    
    $collections = @(Get-MongoDBCollections)
    
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
    
    $collections = @(Get-MongoDBCollections)
    
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
    
    $collections = @(Get-MongoDBCollections)
    
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
    
    $collections = @(Get-MongoDBCollections)
    
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
    
    $collections = @(Get-MongoDBCollections)
    
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
    
    $collections = @(Get-MongoDBCollections)
    
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


Export-ModuleMember -Function Start-MigrationToolMenu, Invoke-MigrationWorkflow, Get-AppConfig