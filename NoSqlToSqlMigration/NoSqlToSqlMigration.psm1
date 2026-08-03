function Set-N2SOutputMode {
    <#
    .SYNOPSIS
    Chooses how the core reports its progress

    .DESCRIPTION
    Console - coloured output straight to the screen, for the interactive menu.
    Stream  - the regular PowerShell streams, so an automated caller can silence
              the progress ($InformationPreference), collect it (6>> file.log),
              or act on warnings and errors separately (3> and 2>).

    Console is the default, so the menu keeps looking exactly as it did.
    #>

    param (
        [Parameter(Mandatory=$true)]
        [ValidateSet('Console', 'Stream')]
        [string]$Mode
    )

    $script:N2SOutputMode = $Mode
}

function Write-N2SMessage {
    <#
    .SYNOPSIS
    Reports one message from the core, with a level instead of a colour

    .DESCRIPTION
    Core functions say what happened and how important it is, this function is
    the only place that decides how that reaches the user. Levels map onto the
    PowerShell streams, so detail can be hidden without hiding warnings, and a
    warning is a real warning instead of yellow text.

    .PARAMETER Message
    The text to report.

    .PARAMETER Level
    Header  - section title
    Step    - the step that is starting
    Info    - normal progress
    Success - something completed
    Detail  - extra detail, hidden unless asked for (-Verbose)
    Warning - something needs attention but the work continues
    Error   - something failed
    #>

    param (
        [Parameter(Position = 0)]
        [AllowEmptyString()]
        [string]$Message = '',

        [ValidateSet('Header', 'Step', 'Info', 'Success', 'Detail', 'Warning', 'Error')]
        [string]$Level = 'Info'
    )

    if ($script:N2SOutputMode -eq 'Stream') {
        switch ($Level) {
            'Detail'  { Write-Verbose $Message }
            'Warning' { Write-Warning $Message }
            'Error'   { Write-Error $Message }
            default   { Write-Information $Message }
        }

        return
    }

    # Console: the same colours the tool has always used
    $colour = switch ($Level) {
        'Header'  { 'Cyan' }
        'Step'    { 'Yellow' }
        'Success' { 'Green' }
        'Detail'  { 'Gray' }
        'Warning' { 'Yellow' }
        'Error'   { 'Red' }
        default   { 'White' }
    }

    Write-Host $Message -ForegroundColor $colour
}

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
        Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
        Write-N2SMessage "    MongoDB Schema Analysis - $CollectionName" -Level Header
        Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
        
        # Connect to MongoDB
        Write-N2SMessage "Connecting to MongoDB..." -Level Step
        Connect-Mdbc -ConnectionString $ConnectionString -DatabaseName $DatabaseName -CollectionName $CollectionName
        
        # Get total document count
        $totalDocs = Get-MdbcData -Count
        Write-N2SMessage "Total documents in collection: $totalDocs" -Level Detail
        
        # Determine actual sample size
        $actualSampleSize = [Math]::Min($SampleSize, $totalDocs)
        Write-N2SMessage "Analyzing $actualSampleSize documents...`n" -Level Detail
        
        # Get sample documents
        $documents = Get-MdbcData -Last $actualSampleSize
        
        # Initialize schema structure
        $schema = @{}
        
        # Analyze each document
        $docCount = 0
        foreach ($doc in $documents) {
            $docCount++
            Write-Progress -Activity "Analyzing documents" -Status "Document $docCount of $actualSampleSize" -PercentComplete (($docCount / $actualSampleSize) * 100)
            
            # Only of interest while debugging, so not on the normal output
            Write-Verbose "Document type: $($doc.GetType().FullName)"

            # Convert to proper object if needed
            if ($doc -is [MongoDB.Bson.BsonDocument]) {
                $doc = [MongoDB.Bson.BsonTypeMapper]::MapToDotNetValue($doc)
            }
            
            Analyze-DocumentStructure -Document $doc -Schema $schema -Path "" -TotalDocs $actualSampleSize
        }
        
        Write-Progress -Activity "Analyzing documents" -Completed
        
        # Generate and display results
        Write-N2SMessage "Schema Analysis Results:" -Level Success
        Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
        
        # Show-SchemaResults belongs to the presentation layer and writes straight
        # to the screen, so an automated run would still get the whole table even
        # with -Quiet. The schema is in the return value either way.
        if ($script:N2SOutputMode -ne 'Stream') {
            Show-SchemaResults -Schema $schema -TotalDocs $actualSampleSize
        }
        
        # Return schema object for further processing
        return $schema
    }
    catch {
        Write-N2SMessage "Error during schema analysis: $($_.Exception.Message)" -Level Error
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
        Write-N2SMessage "WARNING: Null document encountered at path: $Path" -Level Step
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
        Write-N2SMessage "WARNING: Unexpected document type: $($Document.GetType().FullName)" -Level Step
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

    if ($Value -is [System.Collections.IDictionary]) {
        return $true
    }

    if ($null -ne $Value.PSObject -and $Value.PSObject.BaseObject -is [System.Management.Automation.PSCustomObject]) {
        return $true
    }

    return $false
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

function Show-SchemaResults {
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
        [string]$Path
    )

    if (-not $Path) {
        # A path set by Invoke-N2SMigration wins over the default next to the module,
        # so an automated run can point at its own configuration file
        $Path = if ($script:N2SConfigPath) { $script:N2SConfigPath } else { "$PSScriptRoot\..\config.json" }
    }

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
            Write-N2SMessage "Testing MongoDB connection..." -Level Header

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


            Write-N2SMessage "MongoDB connection successful!" -Level Success
            Write-N2SMessage "Database: $DatabaseName" -Level Detail
            if ($CollectionName) {
                Write-N2SMessage "Collection: $CollectionName" -Level Detail
            }
            Write-N2SMessage "Document count: $count" -Level Detail

            return $true
        }
        catch {
            Write-N2SMessage "MongoDB connection failed!" -Level Error
            Write-N2SMessage "Error: $($_.Exception.Message)" -Level Error
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
            Write-N2SMessage "Testing MySQL connection..." -Level Header

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

            Write-N2SMessage "MySQL connection successful!" -Level Success
            Write-N2SMessage "Server: $Server`:$Port" -Level Detail
            Write-N2SMessage "Database: $Database" -Level Detail
            Write-N2SMessage "Version: $($connection.ServerVersion)" -Level Detail

            $connection.Close()
            return $true
        }
        catch {
            Write-N2SMessage "MySQL connection failed!" -Level Error
            Write-N2SMessage "Error: $($_.Exception.Message)" -Level Error
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
            Write-N2SMessage "Testing SQL Server connection..." -Level Header

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

            Write-N2SMessage "SQL Server connection successful!" -Level Success
            Write-N2SMessage "Server: $Server" -Level Detail
            Write-N2SMessage "Database: $Database" -Level Detail

            return $true
        }
        catch {
            Write-N2SMessage "SQL Server connection failed!" -Level Error
            Write-N2SMessage "Error: $($_.Exception.Message)" -Level Error
            return $false
        }
    }

    # Initialize All Database Connections
    function Initialize-DatabaseConnections {
        param(
            [ValidateSet("MySQL", "SQLServer")]
            [string]$DatabaseType = "MySQL"
        )

        Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
        Write-N2SMessage "    NoSQL to SQL Migration Tool - Connection Test" -Level Header
        Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header

        try {
            $config = Get-AppConfig
        }
        catch {
            Write-N2SMessage $_.Exception.Message -Level Error
            return $false
        }

        # MongoDB
        $mongoOk = Test-MongoDBConnection `
            -ConnectionString $config.MongoDB.ConnectionString `
            -DatabaseName $config.MongoDB.Database `
            -CollectionName $config.MongoDB.Collection

        Write-N2SMessage "" -Level Info

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

        Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header

        if ($mongoOk -and $sqlOk) {
            Write-N2SMessage "All database connections are successful!" -Level Success
            Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
            return $true
        }

        Write-N2SMessage "One or more database connections failed!" -Level Error
        Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
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
    
    Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
    Write-N2SMessage "    Data Migration - MongoDB to $DatabaseType" -Level Header
    Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
    
    # Initialize migration tracking. Ordered, so the fields always print in the
    # same sensible order instead of the arbitrary order of a hashtable.
    $migrationResult = [ordered]@{
        StartTime = Get-Date
        EndTime = $null
        Duration = $null
        DurationSeconds = 0
        TotalDocuments = 0
        MigratedDocuments = 0
        FailedDocuments = 0
        TablesCreated = @()
        RecordsInserted = @{}
        ConversionIssues = @()
        Errors = @()
    }
    
    try {
        # Step 1: Connect to databases
        Write-N2SMessage "Step 1: Establishing connections..." -Level Step
        
        # Get configuration
        $config = Get-AppConfig
        
        # Connect to MongoDB
        Connect-Mdbc -ConnectionString $config.MongoDB.ConnectionString `
                     -DatabaseName $config.MongoDB.Database `
                     -CollectionName $CollectionName
        
        $totalDocs = Get-MdbcData -Count
        $migrationResult.TotalDocuments = $totalDocs
        Write-N2SMessage " MongoDB connected: $totalDocs documents found" -Level Success
        
        # Connect to SQL
        $sqlConnection = Get-SQLConnectionObject -DatabaseType $DatabaseType
        $sqlConnection.Open()
        Write-N2SMessage " $DatabaseType connected" -Level Success
        
        # How to handle values that do not fit their column: Warn (default),
        # Skip or Fail. Read once, because this is checked per document.
        $script:N2SConversionPolicy = Get-ConversionErrorPolicy -Config $config
        $script:N2SConversionIssues = @()
        Write-N2SMessage " Conversion errors: $($script:N2SConversionPolicy)" -Level Detail

        # Step 2: Create tables
        Write-N2SMessage "`nStep 2: Creating tables..." -Level Step

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
                        Write-N2SMessage " Created table: $tableName" -Level Success
                    }
                }
                catch {
                    Write-N2SMessage "⚠ Table creation warning: $($_.Exception.Message)" -Level Step
                }
            }
        }

        if ($DatabaseType -eq "MySQL") {
            Invoke-SQLNonQuery -Connection $sqlConnection -CommandText "SET FOREIGN_KEY_CHECKS = 1" | Out-Null
        }
        
        # Read the column layout of every table now, so no SHOW COLUMNS has to run
        # while a transaction is open below
        foreach ($table in $SQLSchema.Tables) {
            Get-SQLTableColumns -Connection $sqlConnection -TableName $table | Out-Null
        }

        # Step 3: Migrate data
        Write-N2SMessage "`nStep 3: Migrating data..." -Level Step
        Write-N2SMessage "Processing $totalDocs documents in batches of $BatchSize..." -Level Detail

        $processedCount = 0
        $batchNumber = 0

        while ($processedCount -lt $totalDocs) {
            $batchNumber++
            $documents = Get-MdbcData -Skip $processedCount -First $BatchSize

            # Collect the rows of this batch and write them together: one round trip
            # per statement instead of per row. Measured on 9.900 rows: 48 seconds
            # became 8, of which about 1 second is the database itself.
            Start-SQLRowBuffer
            $transaction = $sqlConnection.BeginTransaction()

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
                    Write-N2SMessage " Failed to migrate document: $($doc._id)" -Level Error
                }
            }

            # Write the collected rows and close the batch
            try {
                $flush = Invoke-SQLRowBufferFlush -Connection $sqlConnection `
                                                  -Transaction $transaction `
                                                  -MainTable $SQLSchema.MainTable
                $transaction.Commit()

                # A row that still could not be written means its document did not
                # make it, however successful the conversion was
                foreach ($failedId in $flush.FailedDocuments) {
                    $migrationResult.MigratedDocuments--
                    $migrationResult.FailedDocuments++
                    $migrationResult.Errors += @{
                        Document  = $failedId
                        Error     = "row could not be written"
                        Timestamp = Get-Date
                    }
                }

                foreach ($flushError in $flush.Errors) {
                    Write-N2SMessage " $flushError" -Level Error
                }

                Write-N2SMessage " Batch $batchNumber complete: $processedCount/$totalDocs documents processed, $($flush.RowsWritten) row(s) in $($flush.Statements) statement(s)" -Level Detail
            }
            catch {
                # The batch as a whole could not be committed
                try { $transaction.Rollback() } catch { }

                $migrationResult.MigratedDocuments -= $documents.Count
                $migrationResult.FailedDocuments += $documents.Count
                $migrationResult.Errors += @{
                    Document  = "batch $batchNumber"
                    Error     = $_.Exception.Message
                    Timestamp = Get-Date
                }

                Write-N2SMessage " Batch $batchNumber failed and was rolled back: $($_.Exception.Message)" -Level Error
            }
            finally {
                Stop-SQLRowBuffer
            }
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
        $migrationResult.Duration = $duration.ToString('hh\:mm\:ss')
        $migrationResult.DurationSeconds = [math]::Round($duration.TotalSeconds, 2)

        Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
        Write-N2SMessage "Migration Complete!" -Level Success
        Write-N2SMessage "═══════════════════════════════════════════════════════" -Level Header
        Write-N2SMessage "Duration: $($migrationResult.Duration) ($($migrationResult.DurationSeconds) seconds)" -Level Detail
        Write-N2SMessage "Total documents: $($migrationResult.TotalDocuments)" -Level Detail
        Write-N2SMessage "Successfully migrated: $($migrationResult.MigratedDocuments)" -Level Success
        Write-N2SMessage "Failed: $($migrationResult.FailedDocuments)" -Level $(if ($migrationResult.FailedDocuments -gt 0) { 'Error' } else { 'Detail' })
        
        Write-N2SMessage "`nRecords per table:" -Level Step
        foreach ($table in $migrationResult.RecordsInserted.Keys | Sort-Object) {
            Write-N2SMessage "  $table : $($migrationResult.RecordsInserted[$table])" -Level Detail
        }

        # Values that did not fit their column, so nothing disappears unnoticed
        $migrationResult.ConversionIssues = @($script:N2SConversionIssues)

        if ($migrationResult.ConversionIssues.Count -gt 0) {
            $affectedDocuments = @($migrationResult.ConversionIssues | Select-Object -ExpandProperty Document -Unique).Count

            Write-N2SMessage "`nConversion problems: $($migrationResult.ConversionIssues.Count) value(s) in $affectedDocuments document(s)" -Level Step

            foreach ($issue in ($migrationResult.ConversionIssues | Select-Object -First 5)) {
                Write-N2SMessage "  $($issue.Table).$($issue.Field): $($issue.Reason) -> $($issue.Action)" -Level Step
            }

            if ($migrationResult.ConversionIssues.Count -gt 5) {
                Write-N2SMessage "  ... and $($migrationResult.ConversionIssues.Count - 5) more" -Level Step
            }

            $reportPath = ".\conversion_report_$($SQLSchema.MainTable)_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
            $written = Export-ConversionReport -Issues $migrationResult.ConversionIssues -OutputPath $reportPath

            if ($written) {
                Write-N2SMessage "  Full report: $written" -Level Detail
            }
        }
        
        if ($migrationResult.Errors.Count -gt 0) {
            Write-N2SMessage "`nErrors encountered:" -Level Error
            $migrationResult.Errors | ForEach-Object {
                Write-N2SMessage "  Document $($_.Document): $($_.Error)" -Level Error
            }
        }
        
        Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
        
        return $migrationResult
    }
    catch {
        Write-N2SMessage "`n Migration failed: $($_.Exception.Message)" -Level Error
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

    # Values are the column types (for example 'varchar(255)', 'datetime'), so the
    # conversion layer knows what a value has to fit into
    $columns = @{}

    try {
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = 'SHOW COLUMNS FROM `' + $TableName + '`'
        $reader = $cmd.ExecuteReader()

        while ($reader.Read()) {
            $columns[$reader.GetString(0)] = $reader.GetString(1)
        }
        $reader.Close()

        $script:N2STableColumns[$TableName] = $columns
    }
    catch {
        Write-N2SMessage "Warning: could not read columns of table $TableName : $($_.Exception.Message)" -Level Step
    }

    return $columns
}

function ConvertTo-SQLDateTime {
    <#
    .SYNOPSIS
    Parses a value into a DateTime, accepting the formats that occur in practice

    .DESCRIPTION
    A date can arrive as a real DateTime or as text in any notation. Day-first
    is tried before month-first, because month-day is mainly used in the US and is less common
    #>

    param (
        $Value
    )

    if ($Value -is [DateTime]) {
        return $Value
    }

    if ($null -eq $Value) {
        return $null
    }

    $text = $Value.ToString().Trim()
    if ($text -eq '') {
        return $null
    }

    $formats = @(
        'yyyy-MM-ddTHH:mm:ss.fffffffK', 'yyyy-MM-ddTHH:mm:ssK', 'yyyy-MM-ddTHH:mm:ss',
        'yyyy-MM-dd HH:mm:ss', 'yyyy-MM-dd', 'yyyy/MM/dd',
        'dd/MM/yyyy HH:mm:ss', 'dd/MM/yyyy', 'dd-MM-yyyy', 'dd.MM.yyyy',
        'MM/dd/yyyy HH:mm:ss', 'MM/dd/yyyy'
    )

    $styles = [System.Globalization.DateTimeStyles]::AllowWhiteSpaces
    $parsed = [DateTime]::MinValue

    foreach ($format in $formats) {
        if ([DateTime]::TryParseExact($text, $format, [System.Globalization.CultureInfo]::InvariantCulture, $styles, [ref]$parsed)) {
            return $parsed
        }
    }

    if ([DateTime]::TryParse($text, [System.Globalization.CultureInfo]::InvariantCulture, $styles, [ref]$parsed)) {
        return $parsed
    }

    if ([DateTime]::TryParse($text, [System.Globalization.CultureInfo]::CurrentCulture, $styles, [ref]$parsed)) {
        return $parsed
    }

    return $null
}

function ConvertTo-SQLTextValue {
    <#
    .SYNOPSIS
    Writes a value into a text column in a culture independent notation

    .DESCRIPTION
    A date lands in a text column whenever the field holds mixed types. Using
    ToString() would write it in the notation of the machine running the
    migration, so the same data would look different on another machine and
    would not sort correctly. Dates become ISO, numbers use a decimal point.
    #>

    param (
        $Value
    )

    if ($Value -is [DateTime]) {
        return $Value.ToString('yyyy-MM-dd HH:mm:ss', [System.Globalization.CultureInfo]::InvariantCulture)
    }

    if ($Value -is [double] -or $Value -is [float] -or $Value -is [decimal]) {
        return ([double]$Value).ToString([System.Globalization.CultureInfo]::InvariantCulture)
    }

    return $Value.ToString()
}

function Get-SQLColumnKind {
    <#
    .SYNOPSIS
    Works out once what kind of column this is, and remembers it

    .DESCRIPTION
    Parsing 'varchar(255)' into "text of at most 255 characters" is the same work
    for every value in that column. On a migration of ten thousand rows that is
    ten thousand times the same regular expression, so the answer is cached per
    column type.
    #>

    param (
        [string]$ColumnType
    )

    if ($null -eq $script:N2SColumnKinds) {
        $script:N2SColumnKinds = @{}
    }

    if ($script:N2SColumnKinds.ContainsKey($ColumnType)) {
        return $script:N2SColumnKinds[$ColumnType]
    }

    $base = ($ColumnType -replace '\(.*$', '').Trim().ToLowerInvariant()

    $kind = switch -Regex ($base) {
        '^(datetime|timestamp)$'                            { 'datetime' }
        '^date$'                                            { 'date' }
        '^(int|integer|bigint|smallint|mediumint|tinyint)$'  { 'int' }
        '^(decimal|numeric|float|double|real)$'              { 'decimal' }
        '^(char|varchar|nvarchar)$'                         { 'varchar' }
        default                                             { 'text' }
    }

    $length = 0
    if ($kind -eq 'varchar' -and $ColumnType -match '\((\d+)\)') {
        $length = [int]$matches[1]
    }

    $info = [PSCustomObject]@{
        Kind   = $kind
        Base   = $base
        Length = $length
    }

    $script:N2SColumnKinds[$ColumnType] = $info
    return $info
}

function ConvertTo-SQLColumnValue {
    <#
    .SYNOPSIS
    Converts one document value to the type of the column it goes into

    .DESCRIPTION
    Returns a result with Success, Value and Reason instead of throwing. A value
    that does not fit must never take down the document it belongs to, let alone
    the whole migration: the caller decides what happens with it.
    #>

    param (
        $Value,
        [string]$ColumnType,
        [string]$DatabaseType = "MySQL"
    )

    $result = @{
        Success = $true
        Value   = [DBNull]::Value
        Reason  = $null
    }

    if ($null -eq $Value) {
        return $result
    }

    # No column type known: keep the old behaviour
    if ([string]::IsNullOrWhiteSpace($ColumnType)) {
        $result.Value = Convert-ToSQLValue -Value $Value -DatabaseType $DatabaseType
        return $result
    }

    $column = Get-SQLColumnKind -ColumnType $ColumnType

    # Most values already have the type their column wants. Handing those straight
    # through skips the rest of this function: measured about a third faster per
    # value. On a whole migration that is a modest gain, not the decisive one.
    switch ($column.Kind) {
        'int' {
            if ($Value -is [int] -or $Value -is [long]) { $result.Value = $Value; return $result }
        }
        'decimal' {
            if ($Value -is [double] -or $Value -is [decimal]) { $result.Value = $Value; return $result }
        }
        'varchar' {
            if ($Value -is [string] -and ($column.Length -eq 0 -or $Value.Length -le $column.Length)) {
                $result.Value = $Value
                return $result
            }
        }
        'text' {
            if ($Value -is [string]) { $result.Value = $Value; return $result }
        }
        'datetime' {
            if ($Value -is [DateTime]) { $result.Value = $Value; return $result }
        }
    }

    # Normalize the MongoDB value first (ObjectId, BSON types, booleans)
    $value = Convert-ToSQLValue -Value $Value -DatabaseType $DatabaseType

    if ($value -is [DBNull]) {
        return $result
    }

    $baseType = $column.Base

    switch -Regex ($baseType) {
        '^(datetime|timestamp|date)$' {
            $parsed = ConvertTo-SQLDateTime -Value $value

            if ($null -eq $parsed) {
                $result.Success = $false
                $result.Reason = "'$value' is not a recognisable date for a $baseType column"
                return $result
            }

            if ($baseType -eq 'date') {
                $result.Value = $parsed.Date
            }
            else {
                $result.Value = $parsed
            }
            return $result
        }

        '^(int|integer|bigint|smallint|mediumint|tinyint)$' {
            $number = 0L

            if ($value -is [bool]) {
                $result.Value = [int]$value
                return $result
            }

            if ([long]::TryParse($value.ToString().Trim(), [System.Globalization.NumberStyles]::Integer,
                                 [System.Globalization.CultureInfo]::InvariantCulture, [ref]$number)) {
                $result.Value = $number
                return $result
            }

            $result.Success = $false
            $result.Reason = "'$value' is not a whole number for a $baseType column"
            return $result
        }

        '^(decimal|numeric|float|double|real)$' {
            $number = 0.0
            # A decimal comma is common in exported data, so try that too
            $text = $value.ToString().Trim()

            foreach ($candidate in @($text, ($text -replace ',', '.'))) {
                if ([double]::TryParse($candidate, [System.Globalization.NumberStyles]::Float,
                                       [System.Globalization.CultureInfo]::InvariantCulture, [ref]$number)) {
                    $result.Value = $number
                    return $result
                }
            }

            $result.Success = $false
            $result.Reason = "'$value' is not a number for a $baseType column"
            return $result
        }

        '^(char|varchar|nvarchar)$' {
            $text = ConvertTo-SQLTextValue -Value $value

            # A value longer than the column would be rejected by the database
            if ($ColumnType -match '\((\d+)\)') {
                $maxLength = [int]$matches[1]

                if ($text.Length -gt $maxLength) {
                    $result.Success = $false
                    $result.Reason = "value of $($text.Length) characters does not fit $ColumnType"
                    return $result
                }
            }

            $result.Value = $text
            return $result
        }

        default {
            # text, longtext, blob and anything else: store as text
            $result.Value = ConvertTo-SQLTextValue -Value $value
            return $result
        }
    }
}

function Add-ConversionIssue {
    <#
    .SYNOPSIS
    Records a value that could not be converted, so nothing is lost silently
    #>

    param (
        [string]$TableName,
        [string]$DocumentId,
        [string]$FieldName,
        [string]$Reason,
        [string]$Action
    )

    if ($null -eq $script:N2SConversionIssues) {
        $script:N2SConversionIssues = @()
    }

    $script:N2SConversionIssues += [PSCustomObject]@{
        Table    = $TableName
        Document = $DocumentId
        Field    = $FieldName
        Reason   = $Reason
        Action   = $Action
    }
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

function Start-SQLRowBuffer {
    <#
    .SYNOPSIS
    Starts collecting rows instead of writing them one at a time

    .DESCRIPTION
    Writing row by row costs one round trip to the database per row, and that used
    to be where nearly all the time of a migration went: measured on a local
    MySQL, row by row does about 200 rows per second, while rows collected into
    multi-row statements inside one transaction do more than 11,000. 

    While the buffer is active, Add-SQLRow and Invoke-ChildTableMigration write
    nothing; Invoke-SQLRowBufferFlush sends everything in as few statements as
    possible.
    #>

    $script:N2SRowBuffer = [ordered]@{}
    $script:N2SRowBufferDeletes = [ordered]@{}
}

function Stop-SQLRowBuffer {
    <#
    .SYNOPSIS
    Stops collecting; rows are written straight away again
    #>

    $script:N2SRowBuffer = $null
    $script:N2SRowBufferDeletes = $null
}

function Test-SQLRowBufferActive {
    <#
    .SYNOPSIS
    Tells whether rows are being collected at the moment
    #>

    return ($null -ne $script:N2SRowBuffer)
}

function Add-BufferedRow {
    <#
    .SYNOPSIS
    Adds one row to the buffer

    .DESCRIPTION
    Rows are grouped per table AND per set of columns: one statement can only
    carry rows that fill the same columns, and documents do not all hold the
    same fields.
    #>

    param (
        [string]$TableName,
        $Row,
        [switch]$Replace,
        [string]$DocumentId
    )

    $columns = @($Row.Keys)
    $key = "$TableName|$($columns -join ',')"

    if (-not $script:N2SRowBuffer.Contains($key)) {
        $script:N2SRowBuffer[$key] = [PSCustomObject]@{
            Table       = $TableName
            Columns     = $columns
            Replace     = [bool]$Replace
            Values      = [System.Collections.ArrayList]::new()
            DocumentIds = [System.Collections.ArrayList]::new()
        }
    }

    $group = $script:N2SRowBuffer[$key]
    $group.Values.Add(@($columns | ForEach-Object { $Row[$_] })) | Out-Null
    $group.DocumentIds.Add($DocumentId) | Out-Null
}

function Add-BufferedDelete {
    <#
    .SYNOPSIS
    Remembers that the rows of one parent have to be removed from a child table
    #>

    param (
        [string]$TableName,
        [string]$KeyColumn,
        $ParentId
    )

    $key = "$TableName|$KeyColumn"

    if (-not $script:N2SRowBufferDeletes.Contains($key)) {
        $script:N2SRowBufferDeletes[$key] = [PSCustomObject]@{
            Table     = $TableName
            KeyColumn = $KeyColumn
            ParentIds = [System.Collections.ArrayList]::new()
        }
    }

    $script:N2SRowBufferDeletes[$key].ParentIds.Add($ParentId) | Out-Null
}

function Invoke-SQLChunk {
    <#
    .SYNOPSIS
    Runs one statement with a list of values, and reports failure instead of throwing
    #>

    param (
        $Connection,
        $Transaction,
        [string]$CommandText,
        $Values
    )

    $cmd = $Connection.CreateCommand()
    $cmd.CommandText = $CommandText

    if ($Transaction) {
        $cmd.Transaction = $Transaction
    }

    foreach ($value in $Values) {
        $param = $cmd.CreateParameter()
        $param.Value = $value
        $cmd.Parameters.Add($param) | Out-Null
    }

    $cmd.ExecuteNonQuery() | Out-Null
}

function Invoke-SQLRowBufferFlush {
    <#
    .SYNOPSIS
    Writes everything in the buffer, in as few statements as possible

    .DESCRIPTION
    Order matters. Child rows are removed first, then the parent rows are written,
    then the child rows: a REPLACE on a parent row deletes and re-inserts it, and
    a child row still pointing at it would break the foreign key.

    A statement that fails is retried row by row, so one bad row costs its own row
    instead of the whole chunk, and the caller learns which document it was.
    #>

    param (
        $Connection,
        $Transaction,
        [string]$MainTable,
        [int]$MaxParametersPerStatement = 2000,
        [int]$MaxRowsPerStatement = 500
    )

    $result = @{
        Statements       = 0
        RowsWritten      = 0
        FailedRows       = 0
        FailedDocuments  = @()
        Errors           = @()
    }

    if (-not (Test-SQLRowBufferActive)) {
        return $result
    }

    # 1. Remove the old child rows of the parents in this batch
    foreach ($delete in $script:N2SRowBufferDeletes.Values) {
        $ids = @($delete.ParentIds | Sort-Object -Unique)

        for ($start = 0; $start -lt $ids.Count; $start += $MaxRowsPerStatement) {
            $slice = $ids[$start..([math]::Min($start + $MaxRowsPerStatement, $ids.Count) - 1)]
            $placeholders = ($slice | ForEach-Object { '?' }) -join ', '
            $sql = 'DELETE FROM `' + $delete.Table + '` WHERE `' + $delete.KeyColumn + '` IN (' + $placeholders + ')'

            try {
                Invoke-SQLChunk -Connection $Connection -Transaction $Transaction -CommandText $sql -Values $slice
                $result.Statements++
            }
            catch {
                $result.Errors += "Could not clear old rows in $($delete.Table): $($_.Exception.Message)"
            }
        }
    }

    # 2. Parent rows first, then the child tables
    $groups = @($script:N2SRowBuffer.Values | Sort-Object -Property @{ Expression = { $_.Table -ne $MainTable } }, Table)

    foreach ($group in $groups) {
        $columnList = ($group.Columns | ForEach-Object { '`' + $_ + '`' }) -join ', '
        $verb = if ($group.Replace) { 'REPLACE INTO' } else { 'INSERT INTO' }
        $rowPlaceholder = '(' + (($group.Columns | ForEach-Object { '?' }) -join ', ') + ')'

        # Keep a statement within both limits: parameters and rows
        $rowsPerStatement = [math]::Max(1, [math]::Floor($MaxParametersPerStatement / [math]::Max(1, $group.Columns.Count)))
        $rowsPerStatement = [math]::Min($rowsPerStatement, $MaxRowsPerStatement)

        for ($start = 0; $start -lt $group.Values.Count; $start += $rowsPerStatement) {
            $end = [math]::Min($start + $rowsPerStatement, $group.Values.Count) - 1
            $slice = @($group.Values[$start..$end])

            $sql = "$verb " + '`' + $group.Table + '` (' + $columnList + ') VALUES ' +
                   (($slice | ForEach-Object { $rowPlaceholder }) -join ', ')
            $values = @($slice | ForEach-Object { $_ } | ForEach-Object { $_ })

            try {
                Invoke-SQLChunk -Connection $Connection -Transaction $Transaction -CommandText $sql -Values $values
                $result.Statements++
                $result.RowsWritten += $slice.Count
            }
            catch {
                # Find out which row is at fault instead of losing the whole chunk
                $singleSql = "$verb " + '`' + $group.Table + '` (' + $columnList + ") VALUES $rowPlaceholder"

                for ($i = 0; $i -lt $slice.Count; $i++) {
                    try {
                        Invoke-SQLChunk -Connection $Connection -Transaction $Transaction -CommandText $singleSql -Values $slice[$i]
                        $result.Statements++
                        $result.RowsWritten++
                    }
                    catch {
                        $result.FailedRows++
                        $documentId = $group.DocumentIds[$start + $i]

                        if ($group.Table -eq $MainTable -and $documentId) {
                            $result.FailedDocuments += $documentId
                        }

                        $result.Errors += "$($group.Table): $($_.Exception.Message)"
                    }
                }
            }
        }
    }

    Start-SQLRowBuffer
    return $result
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
        [switch]$Replace,
        [string]$DocumentId
    )

    # While a buffer is active the row is collected and written later, together
    # with the other rows of this batch
    if (Test-SQLRowBufferActive) {
        Add-BufferedRow -TableName $TableName -Row $Row -Replace:$Replace -DocumentId $DocumentId
        return
    }

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

function Get-ConvertedChildValue {
    <#
    .SYNOPSIS
    Converts one array element or sub-document value for its child column

    .DESCRIPTION
    A child row belongs to a parent document that did migrate, so an
    unconvertible element is stored as NULL and recorded, never dropped silently.
    #>

    param (
        $Value,
        [string]$ColumnType,
        [string]$ChildTable,
        $ParentId,
        [string]$FieldName,
        [string]$DatabaseType
    )

    $converted = ConvertTo-SQLColumnValue -Value $Value -ColumnType $ColumnType -DatabaseType $DatabaseType

    if ($converted.Success) {
        return $converted.Value
    }

    Add-ConversionIssue -TableName $ChildTable -DocumentId "$ParentId" -FieldName $FieldName `
                        -Reason $converted.Reason -Action 'stored as NULL'

    return [DBNull]::Value
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

    # Remove rows of a previous run for this parent, so re-running stays idempotent.
    # While buffering, the deletes of the whole batch are combined into one
    # statement per child table.
    if (Test-SQLRowBufferActive) {
        Add-BufferedDelete -TableName $ChildTable -KeyColumn $ParentKeyColumn -ParentId $ParentId
    }
    else {
        $delete = $Connection.CreateCommand()
        $delete.CommandText = 'DELETE FROM `' + $ChildTable + '` WHERE `' + $ParentKeyColumn + '` = ?'
        $deleteParam = $delete.CreateParameter()
        $deleteParam.Value = $ParentId
        $delete.Parameters.Add($deleteParam) | Out-Null
        $delete.ExecuteNonQuery() | Out-Null
    }

    $rowsWritten = 0

    if (Test-IsDocumentObject -Value $Value) {
        # Sub-document: exactly one child row
        $row = [ordered]@{}
        $row[$ParentKeyColumn] = $ParentId

        foreach ($entry in (ConvertTo-FlatRow -Object $Value).GetEnumerator()) {
            if ($childColumns.ContainsKey($entry.Key)) {
                $row[$entry.Key] = Get-ConvertedChildValue -Value $entry.Value `
                                                           -ColumnType $childColumns[$entry.Key] `
                                                           -ChildTable $ChildTable `
                                                           -ParentId $ParentId `
                                                           -FieldName $entry.Key `
                                                           -DatabaseType $DatabaseType
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
                        $row[$entry.Key] = Get-ConvertedChildValue -Value $entry.Value `
                                                                   -ColumnType $childColumns[$entry.Key] `
                                                                   -ChildTable $ChildTable `
                                                                   -ParentId $ParentId `
                                                                   -FieldName $entry.Key `
                                                                   -DatabaseType $DatabaseType
                    }
                }
            }
            elseif ($childColumns.ContainsKey('value')) {
                $row['value'] = Get-ConvertedChildValue -Value $item `
                                                        -ColumnType $childColumns['value'] `
                                                        -ChildTable $ChildTable `
                                                        -ParentId $ParentId `
                                                        -FieldName 'value' `
                                                        -DatabaseType $DatabaseType
            }

            Add-SQLRow -Connection $Connection -TableName $ChildTable -Row $row
            $rowsWritten++
            $index++
        }
    }

    return $rowsWritten
}

function Get-ConversionErrorPolicy {
    <#
    .SYNOPSIS
    Reads what should happen with a value that does not fit its column

    .DESCRIPTION
    Warn  - store the field as NULL, keep the document, record the problem (default)
    Skip  - do not migrate the document, record the problem
    Fail  - stop the migration on the first unconvertible value
    #>

    param (
        $Config
    )

    $policy = $null

    if ($null -ne $Config -and $null -ne $Config.Migration) {
        $policy = $Config.Migration.OnConversionError
    }

    if ($policy -in @('Warn', 'Skip', 'Fail')) {
        return $policy
    }

    return 'Warn'
}

function Export-ConversionReport {
    <#
    .SYNOPSIS
    Writes the recorded conversion problems to a CSV file
    #>

    param (
        $Issues,
        [string]$OutputPath
    )

    if ($null -eq $Issues -or @($Issues).Count -eq 0) {
        return $null
    }

    try {
        @($Issues) | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding UTF8
        return $OutputPath
    }
    catch {
        Write-N2SMessage "Warning: could not write conversion report: $($_.Exception.Message)" -Level Step
        return $null
    }
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

        $documentId = if ($null -ne $Document[$PrimaryKeyField]) { $Document[$PrimaryKeyField].ToString() } else { '<unknown>' }
        $policy = $script:N2SConversionPolicy
        if ([string]::IsNullOrWhiteSpace($policy)) { $policy = 'Warn' }

        $row = [ordered]@{}
        foreach ($field in $flatFields.Keys) {
            if ($tableColumns.Count -gt 0 -and -not $tableColumns.ContainsKey($field)) {
                continue
            }

            $columnType = if ($tableColumns.Count -gt 0) { $tableColumns[$field] } else { '' }
            $converted = ConvertTo-SQLColumnValue -Value $flatFields[$field] `
                                                  -ColumnType $columnType `
                                                  -DatabaseType $DatabaseType

            if ($converted.Success) {
                $row[$field] = $converted.Value
                continue
            }

            # The value does not fit the column.
            # see Migration.OnConversionError in the config.
            switch ($policy) {
                'Fail' {
                    Add-ConversionIssue -TableName $TableName -DocumentId $documentId -FieldName $field `
                                        -Reason $converted.Reason -Action 'migration stopped'
                    throw "Conversion failed for field '$field' of document $documentId : $($converted.Reason)"
                }
                'Skip' {
                    Add-ConversionIssue -TableName $TableName -DocumentId $documentId -FieldName $field `
                                        -Reason $converted.Reason -Action 'document skipped'
                    Write-N2SMessage "Skipped document $documentId : $($converted.Reason)" -Level Step
                    return $false
                }
                default {
                    # Warn: keep the document, leave this one field empty
                    Add-ConversionIssue -TableName $TableName -DocumentId $documentId -FieldName $field `
                                        -Reason $converted.Reason -Action 'stored as NULL'
                    $row[$field] = [DBNull]::Value
                }
            }
        }

        if ($row.Count -eq 0) {
            Write-N2SMessage "Error migrating document: no matching columns in table $TableName" -Level Error
            return $false
        }

        # REPLACE INTO instead of INSERT INTO to handle duplicates.
        # The document id travels along, so a row that fails later can still be
        # reported as the document it came from.
        Add-SQLRow -Connection $Connection -TableName $TableName -Row $row -Replace -DocumentId $documentId

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
        Write-N2SMessage "Error migrating document: $($_.Exception.Message)" -Level Error
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
                $log += "  [$($err.Timestamp)] Document $($err.Document): $($err.Error)`n"
            }
        }
        
        $log | Out-File -FilePath $OutputPath -Encoding UTF8
        Write-N2SMessage "Migration log exported to: $OutputPath" -Level Success
        
        return $true
    }
    catch {
        Write-N2SMessage "Error exporting log: $($_.Exception.Message)" -Level Error
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
    
    Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
    Write-N2SMessage "    SQL Schema Generation - $TableName" -Level Header
    Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
    
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
    
    Write-N2SMessage "Analysis:" -Level Step
    Write-N2SMessage "  Flat fields: $($flatFields.Keys.Count)" -Level Detail
    Write-N2SMessage "  Nested objects: $($nestedObjects.Keys.Count)" -Level Detail
    Write-N2SMessage "  Array fields: $($arrayFields.Keys.Count)" -Level Detail
    Write-N2SMessage "" -Level Info
    
    # Generate main table
    Write-N2SMessage "Generating main table: $TableName" -Level Success
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
        Write-N2SMessage "Generating nested table: $nestedTableName" -Level Success
        
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
        Write-N2SMessage "Generating array table: $arrayTableName" -Level Success
        
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
    Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
    Write-N2SMessage "Schema Generation Complete!" -Level Success
    Write-N2SMessage "═══════════════════════════════════════════════════════" -Level Header
    Write-N2SMessage "Tables created: $($result.Tables.Count)" -Level Detail
    $result.Tables | ForEach-Object { Write-N2SMessage "  - $_" -Level Detail }
    
    if ($result.Relationships.Count -gt 0) {
        Write-N2SMessage "`nRelationships:" -Level Step
        $result.Relationships | ForEach-Object { Write-N2SMessage "  $_" -Level Detail }
    }
    Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
    
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

    # An empty collection would otherwise produce CREATE TABLE x () and fail.
    # The key column is always there, so the table can be filled later.
    if ($Fields.Keys.Count -eq 0) {
        $columns += "    [$PrimaryKeyField] VARCHAR(24) PRIMARY KEY NOT NULL"
    }

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
    
    # Special handling for _id field
    if ($FieldName -eq "_id") {
        return "VARCHAR(24)"
    }

    # Decide on ALL observed types, not on the most common one. MongoDB has no
    # schema, so the same field can hold a real date in one document and a date
    # written as text in the next. Picking the majority type gives a column that
    # the minority values can never enter, and those documents are lost.
    $observedTypes = @($FieldInfo.Types.Keys | Where-Object { $_ -ne 'null' })

    if ($observedTypes.Count -eq 0) {
        return "VARCHAR(255)"
    }

    if ($observedTypes.Count -eq 1) {
        $primaryType = $observedTypes[0]
    }
    elseif (@($observedTypes | Where-Object { $_ -notin @('integer', 'number') }).Count -eq 0) {
        # Only numbers, but integer and decimal mixed
        $primaryType = 'number'
    }
    else {
        # Mixed types, for example datetime and string: text can hold every value
        $primaryType = 'string'
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
        
        Write-N2SMessage "SQL schema exported to: $OutputPath" -Level Success
        return $true
    }
    catch {
        Write-N2SMessage "Error exporting SQL schema: $($_.Exception.Message)" -Level Error
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
    Write-N2SMessage "Step 1: Analyzing MongoDB collection..." -Level Step
    $schema = Get-MongoDBSchema -ConnectionString $config.MongoDB.ConnectionString `
                                -DatabaseName $config.MongoDB.Database `
                                -CollectionName $config.MongoDB.Collection `
                                -SampleSize 100
    
    # Generate SQL schema
    Write-N2SMessage "`nStep 2: Generating SQL schema..." -Level Step
    $sqlSchema = New-SQLSchema -Schema $schema `
                               -TableName $config.MongoDB.Collection `
                               -PrimaryKeyField "_id"
    
    # Display generated SQL
    Write-N2SMessage "`nGenerated SQL Statements:" -Level Header
    Write-N2SMessage "═══════════════════════════════════════════════════════" -Level Header
    foreach ($statement in $sqlSchema.Statements) {
        Write-N2SMessage $statement -Level Info
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
    
    Write-N2SMessage "`n$('=' * 60)" -Level Header
    Write-N2SMessage "  Complete MongoDB to $DatabaseType Migration Workflow" -Level Header
    Write-N2SMessage "$('=' * 60)`n" -Level Header
    
    # Get configuration
    $config = Get-AppConfig
    $collectionName = $config.MongoDB.Collection
    if (-not $collectionName) {
        throw "Collection name must be specified in config.json under MongoDB.Collection"
    }
    
    # Step 1: Analyze MongoDB schema
    Write-N2SMessage "Phase 1: Schema Analysis" -Level Step
    $schema = Get-MongoDBSchema -ConnectionString $config.MongoDB.ConnectionString `
                                -DatabaseName $config.MongoDB.Database `
                                -CollectionName $collectionName `
                                -SampleSize $SampleSize
    
    # Step 2: Generate SQL schema
    Write-N2SMessage "`nPhase 2: SQL Schema Generation" -Level Step
    $sqlSchema = New-SQLSchema -Schema $schema `
                               -TableName $collectionName `
                               -PrimaryKeyField "_id"
    
    # Export schema
    $schemaFile = ".\schema_$collectionName.sql"
    Export-SQLSchema -SchemaResult $sqlSchema -OutputPath $schemaFile
    
    # Step 3: Migrate data
    Write-N2SMessage "`nPhase 3: Data Migration" -Level Step
    $migrationResult = Start-DataMigration -Schema $schema `
                                           -SQLSchema $sqlSchema `
                                           -CollectionName $collectionName `
                                           -BatchSize $BatchSize `
                                           -DatabaseType $DatabaseType
    
    # Step 4: Export log
    $logFile = ".\migration_log_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"
    Export-MigrationLog -MigrationResult $migrationResult -OutputPath $logFile
    
    Write-N2SMessage "`n Complete migration workflow finished!" -Level Success
    Write-N2SMessage "  Schema file: $schemaFile" -Level Detail
    Write-N2SMessage "  Log file: $logFile" -Level Detail
    
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
    
    Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
    Write-N2SMessage "    Migration Validation - $TableName" -Level Header
    Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
    
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
        Write-N2SMessage "Step 1: Connecting to databases..." -Level Step
        
        # Get configuration
        $config = Get-AppConfig
        
        # MongoDB connection
        Connect-Mdbc -ConnectionString $config.MongoDB.ConnectionString `
                     -DatabaseName $config.MongoDB.Database `
                     -CollectionName $TableName
        
        $validationResult.MongoCount = Get-MdbcData -Count
        Write-N2SMessage " MongoDB: $($validationResult.MongoCount) documents" -Level Success
        
        # SQL connection
        $sqlConnection = Get-SQLConnectionObject -DatabaseType $DatabaseType
        $sqlConnection.Open()
        
        $sqlCmd = $sqlConnection.CreateCommand()
        $countQuery = "SELECT COUNT(*) FROM ``" + $TableName + "``"
        $sqlCmd.CommandText = $countQuery
        $validationResult.SQLCount = [int]$sqlCmd.ExecuteScalar()
        Write-N2SMessage " $DatabaseType : $($validationResult.SQLCount) records" -Level Success
        
        # Step 2: Compare record counts
        Write-N2SMessage "`nStep 2: Comparing record counts..." -Level Step
        
        if ($validationResult.MongoCount -eq $validationResult.SQLCount) {
            Write-N2SMessage " Record counts match!" -Level Success
            $validationResult.RecordCountMatch = $true
        }
        else {
            $diff = [Math]::Abs($validationResult.MongoCount - $validationResult.SQLCount)
            Write-N2SMessage " Record count mismatch! Difference: $diff records" -Level Error
            $validationResult.Issues += "Record count mismatch: MongoDB=$($validationResult.MongoCount), SQL=$($validationResult.SQLCount)"
        }
        
        # Step 3: Validate sample data
        Write-N2SMessage "`nStep 3: Validating sample data..." -Level Step
        
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
                    Write-N2SMessage " Document $docId not found in SQL" -Level Error
                }
                else {
                    # Compare fields
                    $comparisonResult = Compare-DocumentToRecord -MongoDocument $mongoDoc `
                                                                 -SQLRecord $sqlRecord `
                                                                 -DatabaseType $DatabaseType
                    
                    if ($comparisonResult.Match) {
                        $validationResult.SamplesPassed++
                        Write-N2SMessage " Document $docId validated successfully" -Level Success
                    }
                    else {
                        $validationResult.SamplesFailed++
                        $validationResult.Issues += "Document $docId has mismatches: $($comparisonResult.Differences -join ', ')"
                        Write-N2SMessage " Document $docId has differences: $($comparisonResult.Differences -join ', ')" -Level Error
                    }
                    
                    $validationResult.Details += $comparisonResult
                }
            }
            
            Write-Progress -Activity "Validating samples" -Completed
        }
        
        # Step 4: Data integrity checks
        Write-N2SMessage "`nStep 4: Checking data integrity..." -Level Step
        
        $integrityIssues = Test-DataIntegrity -Connection $sqlConnection `
                                              -TableName $TableName `
                                              -DatabaseType $DatabaseType
        
        if ($integrityIssues.Count -eq 0) {
            Write-N2SMessage " No integrity issues found" -Level Success
        }
        else {
            foreach ($issue in $integrityIssues) {
                Write-N2SMessage "⚠ $issue" -Level Step
                $validationResult.Warnings += $issue
            }
        }
        
        # Determine overall status
        if ($validationResult.Issues.Count -eq 0) {
            $validationResult.OverallStatus = "PASSED"
            $statusLevel = "Success"
        }
        elseif ($validationResult.SamplesPassed -gt $validationResult.SamplesFailed) {
            $validationResult.OverallStatus = "PARTIAL"
            $statusLevel = "Warning"
        }
        else {
            $validationResult.OverallStatus = "FAILED"
            $statusLevel = "Error"
        }
        
        # Display summary
        Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
        Write-N2SMessage "Validation Summary" -Level Header
        Write-N2SMessage "═══════════════════════════════════════════════════════" -Level Header
        Write-N2SMessage "Overall Status: $($validationResult.OverallStatus)" -Level $statusLevel
        Write-N2SMessage "Record Count Match: $(if ($validationResult.RecordCountMatch) { 'YES' } else { 'NO' })" -Level $(if ($validationResult.RecordCountMatch) { 'Success' } else { 'Error' })
        Write-N2SMessage "Samples Validated: $($validationResult.SamplesValidated)" -Level Detail
        Write-N2SMessage "  - Passed: $($validationResult.SamplesPassed)" -Level Success
        Write-N2SMessage "  - Failed: $($validationResult.SamplesFailed)" -Level $(if ($validationResult.SamplesFailed -gt 0) { 'Error' } else { 'Detail' })
        Write-N2SMessage "Issues Found: $($validationResult.Issues.Count)" -Level $(if ($validationResult.Issues.Count -gt 0) { 'Error' } else { 'Success' })
        Write-N2SMessage "Warnings: $($validationResult.Warnings.Count)" -Level $(if ($validationResult.Warnings.Count -gt 0) { 'Warning' } else { 'Detail' })
        
        if ($validationResult.Issues.Count -gt 0) {
            Write-N2SMessage "`nIssues:" -Level Error
            foreach ($issue in $validationResult.Issues) {
                Write-N2SMessage "  - $issue" -Level Error
            }
        }
        
        if ($validationResult.Warnings.Count -gt 0) {
            Write-N2SMessage "`nWarnings:" -Level Step
            foreach ($warning in $validationResult.Warnings) {
                Write-N2SMessage "  - $warning" -Level Step
            }
        }
        
        Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
        
        return $validationResult
    }
    catch {
        Write-N2SMessage "`n Validation failed: $($_.Exception.Message)" -Level Error
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
        Write-N2SMessage "Error retrieving SQL record: $($_.Exception.Message)" -Level Error
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

            # A date stored as text in MongoDB ends up as a real date in SQL.
            # Compare the dates, not the notation, otherwise every converted
            # value looks like a difference.
            if ($sqlValue -is [DateTime] -and $mongoValue -isnot [DateTime]) {
                $parsedMongoDate = ConvertTo-SQLDateTime -Value $mongoValue

                if ($null -ne $parsedMongoDate) {
                    $mongoValue = $parsedMongoDate
                }
            }

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
        Write-N2SMessage "Validation report exported to: $OutputPath" -Level Success
        
        return $true
    }
    catch {
        Write-N2SMessage "Error exporting validation report: $($_.Exception.Message)" -Level Error
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
    
    Write-N2SMessage "`n$('=' * 60)" -Level Header
    Write-N2SMessage "  Complete Migration Validation" -Level Header
    Write-N2SMessage "$('=' * 60)`n" -Level Header
    
    # Run validation
    $validationResult = Test-MigrationValidation -TableName $TableName `
                                                  -SampleSize $SampleSize `
                                                  -DatabaseType $DatabaseType
    
    # Export report
    $reportFile = ".\validation_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').html"
    Export-ValidationReport -ValidationResult $validationResult -OutputPath $reportFile
    
    Write-N2SMessage "`n Validation complete!" -Level Success
    Write-N2SMessage "  Report file: $reportFile" -Level Detail
    Write-N2SMessage "  Open the HTML file in your browser to view the detailed report.`n" -Level Detail
    
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
    
    Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
    Write-N2SMessage "    Incremental Sync - $TableName" -Level Header
    Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
    
    # Initialize sync result
    $syncResult = [ordered]@{
        TableName = $TableName
        IsFullSync = $ForceFullSync.IsPresent
        LastSyncTime = $null
        SyncTime = Get-Date
        EndTime = $null
        Duration = $null
        DurationSeconds = 0
        TotalProcessed = 0
        NewRecords = 0
        UpdatedRecords = 0
        DeletedRecords = 0
        UnchangedRecords = 0
        RepairedChildRecords = 0
        ChildRecords = @{}
        GhostChildTables = @()
        Warnings = @()
        Errors = @()
    }

    # Table layout is read back from the database while writing rows
    $script:N2STableColumns = @{}

    try {
        # Step 1: Load or create sync state
        $syncStateFile = ".\sync_state_$TableName.json"
        $syncState = Get-SyncState -FilePath $syncStateFile
        
        if ($ForceFullSync -or $null -eq $syncState) {
            Write-N2SMessage "Performing FULL SYNC..." -Level Step
            $syncResult.IsFullSync = $true
        }
        else {
            Write-N2SMessage "Performing INCREMENTAL SYNC since $($syncState.LastSyncTime)" -Level Step
            $syncResult.LastSyncTime = $syncState.LastSyncTime
        }
        
        # Step 2: Connect to databases
        Write-N2SMessage "`nStep 1: Connecting to databases..." -Level Step
        
        # Get configuration
        $config = Get-AppConfig
        
        # MongoDB
        Connect-Mdbc -ConnectionString $config.MongoDB.ConnectionString `
                     -DatabaseName $config.MongoDB.Database `
                     -CollectionName $TableName
        
        $mongoDocuments = Get-MdbcData
        Write-N2SMessage " MongoDB: $($mongoDocuments.Count) documents" -Level Success
        
        # SQL
        $sqlConnection = Get-SQLConnectionObject -DatabaseType $DatabaseType
        $sqlConnection.Open()
        Write-N2SMessage " SQL connected" -Level Success
        
        # Step 2.5: Check and update schema if needed
        Write-N2SMessage "`nStep 1.5: Checking for schema changes..." -Level Step
        $schemaUpdated = Update-SQLSchema -Connection $sqlConnection `
                                         -TableName $TableName `
                                         -MongoDocuments $mongoDocuments `
                                         -DatabaseType $DatabaseType
        
        if ($schemaUpdated) {
            Write-N2SMessage " Schema updated with new columns" -Level Success
        }
        else {
            Write-N2SMessage " Schema is up to date" -Level Detail
        }
 
        
        # Read the column layout now, so no SHOW COLUMNS has to run while a
        # transaction is open further down
        Get-SQLTableColumns -Connection $sqlConnection -TableName $TableName | Out-Null

        # Step 1.6: Find the child tables holding arrays and sub-documents
        $childTables = Get-ChildTableMap -Connection $sqlConnection `
                                         -TableName $TableName `
                                         -PrimaryKeyField "_id"

        $parentKeyColumn = "${TableName}__id"
        $childRowCounts = @{}

        if ($childTables.Count -gt 0) {
            Write-N2SMessage " Child tables: $(($childTables.Values | Sort-Object) -join ', ')" -Level Detail

            #  
            foreach ($fieldName in $childTables.Keys) {
                $childRowCounts[$fieldName] = Get-ChildRowCounts -Connection $sqlConnection `
                                                                 -ChildTable $childTables[$fieldName] `
                                                                 -ParentKeyColumn $parentKeyColumn
            }
        }

        # Warn about array or sub-document fields that have no child table yet.
        # Creating tables is the job of a Full Migration, not of a sync.
        $missingChildFields = @{}

        foreach ($doc in $mongoDocuments) {
            foreach ($key in $doc.Keys) {
                $value = $doc[$key]

                if ((Test-IsDocumentObject -Value $value) -or
                    ($null -ne $value -and $value -is [System.Collections.IEnumerable] -and $value -isnot [string])) {
                    if (-not $childTables.ContainsKey($key)) {
                        $missingChildFields[$key] = $true
                    }
                }
            }
        }

        foreach ($fieldName in ($missingChildFields.Keys | Sort-Object)) {
            Write-N2SMessage " Field '$fieldName' has no child table - run a Full Migration to create it" -Level Step
            $syncResult.Warnings += "Field '$fieldName' has no child table; run a Full Migration for $TableName"
        }

        # The other way round: a child table whose field is gone from every
        # document. Its rows describe something that no longer exists, and the
        # orphan check cannot see it because the collection itself still exists.
        # Every document was read above, so this is not a guess from a sample.
        $ghostTables = @(Get-GhostChildTable -Connection $sqlConnection `
                                             -TableName $TableName `
                                             -Documents $mongoDocuments)

        foreach ($ghost in $ghostTables) {
            Write-N2SMessage " Child table '$($ghost.Table)' still holds $($ghost.Rows) row(s), but field '$($ghost.Field)' is gone from every document" -Level Warning
            $syncResult.Warnings += "Child table '$($ghost.Table)' is left over from field '$($ghost.Field)'; clean it up with menu option 10"
        }

        $syncResult.GhostChildTables = @($ghostTables | Select-Object -ExpandProperty Table)

        # Step 3: Get current SQL records
        Write-N2SMessage "`nStep 2: Loading existing SQL records..." -Level Step
        $existingRecords = Get-AllSQLRecords -Connection $sqlConnection `
                                            -TableName $TableName `
                                            -DatabaseType $DatabaseType
        
        Write-N2SMessage " Loaded $($existingRecords.Count) existing SQL records" -Level Success
        
        # Step 4: Detect changes
        Write-N2SMessage "`nStep 3: Detecting changes..." -Level Step
        
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
                
                # The hash only describes MongoDB, so child rows that were changed
                # straight in SQL are picked up by the row count check
                $childDrift = $false
                if (-not $syncResult.IsFullSync -and $docHash -eq $lastHash) {
                    $childDrift = Test-ChildRowDrift -Document $doc `
                                                     -DocumentId $docId `
                                                     -ChildTables $childTables `
                                                     -ChildRowCounts $childRowCounts

                    if ($childDrift) {
                        $syncResult.RepairedChildRecords++
                    }
                }

                if ($syncResult.IsFullSync -or $docHash -ne $lastHash -or $childDrift) {
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
        
        Write-N2SMessage "  New documents: $($newDocs.Count)" -Level Success
        Write-N2SMessage "  Updated documents: $($updatedDocs.Count)" -Level Step
        Write-N2SMessage "  Deleted documents: $($deletedIds.Count)" -Level Info
        Write-N2SMessage "  Unchanged: $($syncResult.UnchangedRecords)" -Level Detail
        
        # Step 5: Sync changes
        Write-N2SMessage "`nStep 4: Syncing changes..." -Level Step
        
        $newSyncState = @{
            LastSyncTime = $syncResult.SyncTime
            DocumentHashes = @{}
        }
        
        # Insert new documents, a chunk at a time: the rows of a chunk are
        # collected and written together, which is what makes a sync of many
        # documents finish in seconds instead of minutes
        if ($newDocs.Count -gt 0) {
            Write-N2SMessage "  Inserting $($newDocs.Count) new records..." -Level Success
            $rowsWritten = 0

            foreach ($chunk in (Split-IntoChunk -Items $newDocs -Size 100)) {
                Start-SQLRowBuffer
                $transaction = $sqlConnection.BeginTransaction()

                foreach ($item in $chunk) {
                    try {
                        $success = Invoke-InsertDocument -Connection $sqlConnection `
                                                         -TableName $TableName `
                                                         -Document $item.Document `
                                                         -DatabaseType $DatabaseType

                        if ($success) {
                            Sync-DocumentChildTables -Connection $sqlConnection `
                                                     -TableName $TableName `
                                                     -Document $item.Document `
                                                     -ChildTables $childTables `
                                                     -DatabaseType $DatabaseType `
                                                     -ChildRowCounts $childRowCounts | Out-Null

                            $syncResult.NewRecords++
                            $newSyncState.DocumentHashes[$item.Id] = $item.Hash
                        }
                        else {
                            # No hash is stored, so the next sync retries this document
                            $syncResult.Errors += "Failed to insert document $($item.Id)"
                        }
                    }
                    catch {
                        $syncResult.Errors += "Failed to insert document $($item.Id): $($_.Exception.Message)"
                    }
                }

                $rowsWritten += Complete-SyncChunk -Connection $sqlConnection `
                                                   -Transaction $transaction `
                                                   -TableName $TableName `
                                                   -Items $chunk `
                                                   -SyncResult $syncResult `
                                                   -SyncState $newSyncState `
                                                   -CounterName 'NewRecords'
            }

            Write-N2SMessage "   Inserted $($syncResult.NewRecords) records ($rowsWritten row(s) in total)" -Level Success
        }
        
        # Update modified documents. The main row is an UPDATE and stays per
        # document, but its child rows are the bulk of the work and those are
        # collected per chunk.
        if ($updatedDocs.Count -gt 0) {
            Write-N2SMessage "  Updating $($updatedDocs.Count) modified records..." -Level Step
            $rowsWritten = 0

            foreach ($chunk in (Split-IntoChunk -Items $updatedDocs -Size 100)) {
                Start-SQLRowBuffer
                $transaction = $sqlConnection.BeginTransaction()

                foreach ($item in $chunk) {
                    try {
                        $success = Invoke-UpdateDocument -Connection $sqlConnection `
                                                         -TableName $TableName `
                                                         -Document $item.Document `
                                                         -DatabaseType $DatabaseType `
                                                         -Transaction $transaction

                        if ($success) {
                            # Child rows are rewritten completely for this document
                            Sync-DocumentChildTables -Connection $sqlConnection `
                                                     -TableName $TableName `
                                                     -Document $item.Document `
                                                     -ChildTables $childTables `
                                                     -DatabaseType $DatabaseType `
                                                     -ChildRowCounts $childRowCounts | Out-Null

                            $syncResult.UpdatedRecords++
                            $newSyncState.DocumentHashes[$item.Id] = $item.Hash
                        }
                        else {
                            # No hash is stored, so the next sync retries this document
                            $syncResult.Errors += "Failed to update document $($item.Id)"
                        }
                    }
                    catch {
                        $syncResult.Errors += "Failed to update document $($item.Id): $($_.Exception.Message)"
                    }
                }

                $rowsWritten += Complete-SyncChunk -Connection $sqlConnection `
                                                   -Transaction $transaction `
                                                   -TableName $TableName `
                                                   -Items $chunk `
                                                   -SyncResult $syncResult `
                                                   -SyncState $newSyncState `
                                                   -CounterName 'UpdatedRecords'
            }

            Write-N2SMessage "   Updated $($syncResult.UpdatedRecords) records ($rowsWritten child row(s) rewritten)" -Level Step
        }
        
        # Delete removed documents
        if ($deletedIds.Count -gt 0) {
            Write-N2SMessage "  Deleting $($deletedIds.Count) removed records..." -Level Step
            
            foreach ($id in $deletedIds) {
                try {
                    # Child rows first: the parent row cannot go while they
                    # still reference it
                    Remove-DocumentChildRows -Connection $sqlConnection `
                                             -TableName $TableName `
                                             -Id $id `
                                             -ChildTables $childTables

                    $success = Invoke-DeleteDocument -Connection $sqlConnection `
                                                     -TableName $TableName `
                                                     -Id $id `
                                                     -DatabaseType $DatabaseType
                    
                    if ($success) {
                        $syncResult.DeletedRecords++
                    }
                    else {
                        $syncResult.Errors += "Failed to delete document $id"
                    }
                }
                catch {
                    $syncResult.Errors += "Failed to delete document $id : $($_.Exception.Message)"
                }
            }
            
            Write-N2SMessage "   Deleted $($syncResult.DeletedRecords) records" -Level Info
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

        # Read back the child table row counts for the summary
        foreach ($childTable in $childTables.Values) {
            $rowCount = Get-SQLTableRowCount -Connection $sqlConnection -TableName $childTable
            if ($null -ne $rowCount) {
                $syncResult.ChildRecords[$childTable] = $rowCount
            }
        }

        # How long the whole sync took, so a slow run can be compared with a fast one
        $syncResult.EndTime = Get-Date
        $syncDuration = $syncResult.EndTime - $syncResult.SyncTime
        $syncResult.Duration = $syncDuration.ToString('hh\:mm\:ss')
        $syncResult.DurationSeconds = [math]::Round($syncDuration.TotalSeconds, 2)

        # Display summary
        Write-N2SMessage "`n═══════════════════════════════════════════════════════" -Level Header
        Write-N2SMessage "Sync Complete!" -Level Success
        Write-N2SMessage "═══════════════════════════════════════════════════════" -Level Header
        Write-N2SMessage "Sync Type: $(if ($syncResult.IsFullSync) { 'FULL' } else { 'INCREMENTAL' })" -Level Detail
        Write-N2SMessage "Duration: $($syncResult.Duration) ($($syncResult.DurationSeconds) seconds)" -Level Detail
        Write-N2SMessage "Total Processed: $($syncResult.TotalProcessed)" -Level Detail
        Write-N2SMessage "New Records: $($syncResult.NewRecords)" -Level Success
        Write-N2SMessage "Updated Records: $($syncResult.UpdatedRecords)" -Level Step
        Write-N2SMessage "Deleted Records: $($syncResult.DeletedRecords)" -Level Info
        Write-N2SMessage "Unchanged: $($syncResult.UnchangedRecords)" -Level Detail

        if ($syncResult.RepairedChildRecords -gt 0) {
            Write-N2SMessage "Repaired child rows for: $($syncResult.RepairedChildRecords) document(s)" -Level Step
        }

        if ($syncResult.ChildRecords.Count -gt 0) {
            Write-N2SMessage "`nRows per child table:" -Level Detail
            foreach ($childTable in ($syncResult.ChildRecords.Keys | Sort-Object)) {
                Write-N2SMessage "  $childTable : $($syncResult.ChildRecords[$childTable])" -Level Detail
            }
            Write-N2SMessage "" -Level Info
        }

        Write-N2SMessage "Errors: $($syncResult.Errors.Count)" -Level $(if ($syncResult.Errors.Count -gt 0) { 'Error' } else { 'Detail' })

        if ($syncResult.Warnings.Count -gt 0) {
            Write-N2SMessage "`nWarnings:" -Level Step
            foreach ($warning in $syncResult.Warnings) {
                Write-N2SMessage "  - $warning" -Level Step
            }
        }

        if ($syncResult.Errors.Count -gt 0) {
            Write-N2SMessage "`nErrors:" -Level Error
            foreach ($err in $syncResult.Errors) {
                Write-N2SMessage "  - $err" -Level Error
            }
        }
        
        Write-N2SMessage "═══════════════════════════════════════════════════════`n" -Level Header
        
        return $syncResult
    }
    catch {
        Write-N2SMessage "`n Sync failed: $($_.Exception.Message)" -Level Error
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
            Write-N2SMessage "Warning: Could not load sync state, performing full sync" -Level Step
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
        Write-N2SMessage "`n Sync state saved to: $FilePath" -Level Success
    }
    catch {
        Write-N2SMessage "Warning: Could not save sync state: $($_.Exception.Message)" -Level Step
    }
}

function Add-HashableString {
    <#
    .SYNOPSIS
    Appends the text form of a value to a StringBuilder

    .DESCRIPTION
    This is the fallback route, for values that cannot hand over their own BSON:
    a hashtable built in a test, or a PSCustomObject. Documents that come from
    MongoDB are hashed from their BSON bytes in Get-DocumentHash instead.

    Writing into one buffer avoids joining strings per level, but that is not
    where the time goes. On a large document the cost is one function call per
    value, tens of thousands of them, and no amount of string tuning fixes that.
    That is exactly why this route is only the fallback.
    #>

    param (
        $Value,
        [System.Text.StringBuilder]$Builder
    )

    if ($null -eq $Value) {
        [void]$Builder.Append('null')
        return
    }
    # The checks are inline and ordered by how often they occur. Calling a helper
    # per value, or sorting keys with Sort-Object, costs a cmdlet call per node,
    # and a document with a few hundred sub-documents has tens of thousands.
    if ($Value -is [System.Collections.IDictionary]) {
        [void]$Builder.Append('{')

        # Keys sorted, so the same content always gives the same text
        $keys = [string[]]@($Value.Keys)
        [Array]::Sort($keys, [System.StringComparer]::OrdinalIgnoreCase)

        $first = $true
        foreach ($key in $keys) {
            if (-not $first) { [void]$Builder.Append(';') }
            $first = $false

            [void]$Builder.Append($key).Append('=')
            Add-HashableString -Value $Value[$key] -Builder $Builder
        }

        [void]$Builder.Append('}')
        return
    }

    if ($Value -is [string]) {
        [void]$Builder.Append($Value)
        return
    }

    if ($Value -is [double] -or $Value -is [float] -or $Value -is [decimal]) {
        [void]$Builder.Append(([double]$Value).ToString([System.Globalization.CultureInfo]::InvariantCulture))
        return
    }

    if ($Value -is [DateTime]) {
        [void]$Builder.Append($Value.ToString("o", [System.Globalization.CultureInfo]::InvariantCulture))
        return
    }

    if ($Value -is [System.Collections.IEnumerable]) {
        # Element order is part of the content: reordering an array is a change
        [void]$Builder.Append('[')

        $first = $true
        foreach ($item in $Value) {
            if (-not $first) { [void]$Builder.Append(';') }
            $first = $false

            Add-HashableString -Value $item -Builder $Builder
        }

        [void]$Builder.Append(']')
        return
    }

    if ($null -ne $Value.PSObject -and $Value.PSObject.BaseObject -is [System.Management.Automation.PSCustomObject]) {
        [void]$Builder.Append('{')

        $names = [string[]]@($Value.PSObject.Properties.Name)
        [Array]::Sort($names, [System.StringComparer]::OrdinalIgnoreCase)

        $first = $true
        foreach ($name in $names) {
            if (-not $first) { [void]$Builder.Append(';') }
            $first = $false

            [void]$Builder.Append($name).Append('=')
            Add-HashableString -Value $Value.PSObject.Properties[$name].Value -Builder $Builder
        }

        [void]$Builder.Append('}')
        return
    }

    [void]$Builder.Append($Value.ToString())
}

function ConvertTo-HashableString {
    <#
    .SYNOPSIS
    Builds a stable text representation of a value, arrays and sub-documents included

    .DESCRIPTION
    Keys are sorted so the same content always produces the same text. Numbers and
    dates are written culture independent, otherwise the same value would hash
    differently depending on the regional settings of the machine.

    Used by Get-DocumentHash for values that are not MongoDB documents, and handy
    on its own to see what a document looks like to the change detection.
    #>

    param (
        $Value
    )

    $builder = [System.Text.StringBuilder]::new()
    Add-HashableString -Value $Value -Builder $builder

    return $builder.ToString()
}

function Get-DocumentHash {
    <#
    .SYNOPSIS
    Calculates a hash of a document to detect changes

    .DESCRIPTION
    Covers the whole document, including arrays and sub-documents. Hashing only
    the scalar fields would hide a changed array, so a document whose ratings
    changed would never be flagged for sync.

    A document that comes from MongoDB can hand over its own BSON, and hashing
    those bytes is native work. Walking every value from PowerShell costs a
    function call per value: on a document with a few hundred sub-documents that
    took fifteen seconds, against a fraction of a second now. Anything else, such
    as a hashtable built in a test, still takes the text route.
    #>

    param (
        $Document
    )

    try {
        $bytes = $null

        if ($null -ne $Document -and $null -ne $Document.PSObject.Methods['ToBsonDocument']) {
            $bson = $Document.ToBsonDocument()

            try {
                # Fastest: the raw bytes, without a large string in between
                $bytes = [MongoDB.Bson.BsonExtensionMethods]::ToBson[MongoDB.Bson.BsonDocument]($bson)
            }
            catch {
                # Calling a generic method this way needs a recent PowerShell, so
                # fall back to the JSON form of the same document
                $bytes = [System.Text.Encoding]::UTF8.GetBytes($bson.ToString())
            }
        }

        if ($null -eq $bytes) {
            $bytes = [System.Text.Encoding]::UTF8.GetBytes((ConvertTo-HashableString -Value $Document))
        }

        $md5 = [System.Security.Cryptography.MD5]::Create()
        $hashBytes = $md5.ComputeHash([byte[]]$bytes)

        return [System.BitConverter]::ToString($hashBytes).Replace("-", "")
    }
    catch {
        Write-N2SMessage "Warning: Could not calculate hash for document" -Level Step
        return [guid]::NewGuid().ToString()
    }
}

function Get-ChildTableMap {
    <#
    .SYNOPSIS
    Finds the child tables of a main table, indexed by the document field they hold

    .DESCRIPTION
    Read from the database instead of from a generated schema, so a sync does not
    need to re-analyze the collection. A table only counts as a child table when it
    actually has the parent key column, so an unrelated table whose name happens to
    start with the same prefix is left alone.
    #>

    param (
        $Connection,
        [string]$TableName,
        [string]$PrimaryKeyField = "_id"
    )

    $childTables = @{}
    $parentKeyColumn = "${TableName}_${PrimaryKeyField}"
    $candidates = @()

    try {
        # In LIKE, _ matches any single character, so it has to be escaped
        $pattern = ($TableName -replace '_', '\_') + '\_%'

        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = "SHOW TABLES LIKE '$pattern'"
        $reader = $cmd.ExecuteReader()

        while ($reader.Read()) {
            $candidates += $reader.GetString(0)
        }
        $reader.Close()
    }
    catch {
        Write-N2SMessage "Warning: could not list child tables of $TableName : $($_.Exception.Message)" -Level Step
        return $childTables
    }

    foreach ($candidate in $candidates) {
        $columns = Get-SQLTableColumns -Connection $Connection -TableName $candidate

        if ($columns.ContainsKey($parentKeyColumn)) {
            $fieldName = $candidate.Substring($TableName.Length + 1)
            $childTables[$fieldName] = $candidate
        }
    }

    return $childTables
}

function Get-ChildRowCounts {
    <#
    .SYNOPSIS
    Returns the number of child rows per parent document, in one query
    #>

    param (
        $Connection,
        [string]$ChildTable,
        [string]$ParentKeyColumn
    )

    $counts = @{}

    try {
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = 'SELECT `' + $ParentKeyColumn + '`, COUNT(*) FROM `' + $ChildTable + '` GROUP BY `' + $ParentKeyColumn + '`'
        $reader = $cmd.ExecuteReader()

        while ($reader.Read()) {
            $counts[$reader.GetValue(0).ToString()] = [int]$reader.GetValue(1)
        }
        $reader.Close()
    }
    catch {
        Write-N2SMessage "Warning: could not count rows of $ChildTable : $($_.Exception.Message)" -Level Step
    }

    return $counts
}

function Get-ExpectedChildRowCount {
    <#
    .SYNOPSIS
    Number of child rows a document should have for one field
    #>

    param (
        $Document,
        [string]$FieldName
    )

    if ($null -eq $Document -or $Document.Keys -notcontains $FieldName) {
        return 0
    }

    $value = $Document[$FieldName]

    if ($null -eq $value) {
        return 0
    }

    if (Test-IsDocumentObject -Value $value) {
        return 1
    }

    if ($value -is [System.Collections.IEnumerable] -and $value -isnot [string]) {
        return @($value).Count
    }

    return 0
}

function Test-ChildRowDrift {
    <#
    .SYNOPSIS
    Tells whether the child rows in SQL no longer match the document

    .DESCRIPTION
    Compares row counts only. That catches child rows removed or added straight in
    SQL, which the document hash cannot see because the hash describes MongoDB.
    A changed value inside an existing child row is not detected here: that would
    mean reading every child row on every sync.
    #>

    param (
        $Document,
        [string]$DocumentId,
        [hashtable]$ChildTables,
        [hashtable]$ChildRowCounts
    )

    foreach ($fieldName in $ChildTables.Keys) {
        $expected = Get-ExpectedChildRowCount -Document $Document -FieldName $fieldName

        $actual = 0
        if ($ChildRowCounts.ContainsKey($fieldName) -and $ChildRowCounts[$fieldName].ContainsKey($DocumentId)) {
            $actual = $ChildRowCounts[$fieldName][$DocumentId]
        }

        if ($expected -ne $actual) {
            return $true
        }
    }

    return $false
}

function Sync-DocumentChildTables {
    <#
    .SYNOPSIS
    Rewrites the child rows of one document

    .DESCRIPTION
    Every child table of the document is rewritten completely for this parent:
    Invoke-ChildTableMigration first removes the existing rows, so the result is
    the same no matter how often it runs. A field that disappeared from the
    document leaves an empty child table behind for that parent.
    #>

    param (
        $Connection,
        [string]$TableName,
        $Document,
        [hashtable]$ChildTables,
        [string]$DatabaseType,
        [string]$PrimaryKeyField = "_id",
        [hashtable]$ChildRowCounts
    )

    if ($null -eq $ChildTables -or $ChildTables.Count -eq 0) {
        return 0
    }

    $parentId = Convert-ToSQLValue -Value $Document[$PrimaryKeyField] -DatabaseType $DatabaseType
    $parentKeyColumn = "${TableName}_${PrimaryKeyField}"
    $rowsWritten = 0

    foreach ($fieldName in $ChildTables.Keys) {
        $value = @()
        $hasField = ($Document.Keys -contains $fieldName -and $null -ne $Document[$fieldName])

        # Nothing in the document and nothing in the table for this parent means
        # there is nothing to clear and nothing to write. This only helps for
        # documents that lack the field; where every document fills every child
        # field there is nothing to skip and it changes nothing.
        if (-not $hasField -and $null -ne $ChildRowCounts -and $ChildRowCounts.ContainsKey($fieldName)) {
            $existingRows = 0

            if ($ChildRowCounts[$fieldName].ContainsKey("$parentId")) {
                $existingRows = $ChildRowCounts[$fieldName]["$parentId"]
            }

            if ($existingRows -eq 0) {
                continue
            }
        }

        if ($hasField) {
            $value = $Document[$fieldName]
        }

        $rowsWritten += Invoke-ChildTableMigration -Connection $Connection `
                                                   -ChildTable $ChildTables[$fieldName] `
                                                   -ParentKeyColumn $parentKeyColumn `
                                                   -ParentId $parentId `
                                                   -Value $value `
                                                   -DatabaseType $DatabaseType
    }

    return $rowsWritten
}

function Remove-DocumentChildRows {
    <#
    .SYNOPSIS
    Removes the child rows of a deleted document
    #>

    param (
        $Connection,
        [string]$TableName,
        [string]$Id,
        [hashtable]$ChildTables,
        [string]$PrimaryKeyField = "_id"
    )

    if ($null -eq $ChildTables -or $ChildTables.Count -eq 0) {
        return
    }

    $parentKeyColumn = "${TableName}_${PrimaryKeyField}"

    foreach ($childTable in $ChildTables.Values) {
        try {
            $cmd = $Connection.CreateCommand()
            $cmd.CommandText = 'DELETE FROM `' + $childTable + '` WHERE `' + $parentKeyColumn + '` = ?'

            $param = $cmd.CreateParameter()
            $param.Value = $Id
            $cmd.Parameters.Add($param) | Out-Null

            $cmd.ExecuteNonQuery() | Out-Null
        }
        catch {
            Write-N2SMessage "Warning: could not delete child rows in $childTable : $($_.Exception.Message)" -Level Step
        }
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
            Write-N2SMessage "  Found $($missingColumns.Count) new field(s): $($missingColumns.Name -join ', ')" -Level Step
            
            foreach ($column in $missingColumns) {
                $dataType = Get-SQLDataType -Value $column.SampleValue -DatabaseType $DatabaseType
                
                # Add column as NULLABLE to allow missing values in existing/new records
                $alterSQL = "ALTER TABLE " + $TableName + " ADD COLUMN " + $column.Name + " " + $dataType + " NULL"
                
                $cmd = $Connection.CreateCommand()
                $cmd.CommandText = $alterSQL
                $cmd.ExecuteNonQuery() | Out-Null
                
                Write-N2SMessage "   Added column: $($column.Name) ($dataType NULL)" -Level Success
            }
            
            return $true
        }
        
        return $false
    }
    catch {
        Write-N2SMessage "Warning: Could not update schema: $($_.Exception.Message)" -Level Step
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
        Write-N2SMessage "Error loading SQL records: $($_.Exception.Message)" -Level Error
    }
    
    return $records
}

function Split-IntoChunk {
    <#
    .SYNOPSIS
    Cuts a list into chunks of at most Size items

    .DESCRIPTION
    A sync buffers the rows of a chunk before writing them. Chunks keep memory
    use bounded and keep a transaction from growing without limit.
    #>

    param (
        $Items,
        [int]$Size = 100
    )

    $chunks = @()
    $all = @($Items)

    for ($start = 0; $start -lt $all.Count; $start += $Size) {
        $end = [math]::Min($start + $Size, $all.Count) - 1
        $chunks += , @($all[$start..$end])
    }

    return $chunks
}

function Complete-SyncChunk {
    <#
    .SYNOPSIS
    Writes the buffered rows of a sync chunk and corrects the counters

    .DESCRIPTION
    The counters are raised while the documents are processed, before the rows
    are actually written. A row that fails at the flush has to be taken back off
    the counter, and its hash must not be saved: otherwise the next sync thinks
    the document is up to date and the failure becomes permanent.
    #>

    param (
        $Connection,
        $Transaction,
        [string]$TableName,
        $Items,
        $SyncResult,
        $SyncState,
        [string]$CounterName
    )

    try {
        $flush = Invoke-SQLRowBufferFlush -Connection $Connection `
                                          -Transaction $Transaction `
                                          -MainTable $TableName
        $Transaction.Commit()

        foreach ($failedId in $flush.FailedDocuments) {
            if ($SyncState.DocumentHashes.ContainsKey($failedId)) {
                $SyncResult[$CounterName]--
                $SyncState.DocumentHashes.Remove($failedId)
            }

            $SyncResult.Errors += "Row of document $failedId could not be written"
        }

        foreach ($flushError in $flush.Errors) {
            $SyncResult.Errors += $flushError
        }

        return $flush.RowsWritten
    }
    catch {
        try { $Transaction.Rollback() } catch { }

        foreach ($item in $Items) {
            if ($SyncState.DocumentHashes.ContainsKey($item.Id)) {
                $SyncResult[$CounterName]--
                $SyncState.DocumentHashes.Remove($item.Id)
            }
        }

        $SyncResult.Errors += "A chunk was rolled back: $($_.Exception.Message)"
        return 0
    }
    finally {
        Stop-SQLRowBuffer
    }
}

function Invoke-InsertDocument {
    <#
    .SYNOPSIS
    Inserts a new document into SQL

    .DESCRIPTION
    Uses the same conversion layer as a full migration, so a document that
    arrives through a sync is treated exactly like one that arrives through a
    migration. Writing goes through Add-SQLRow, which means the row joins the
    batch when a row buffer is active.
    #>

    param (
        $Connection,
        [string]$TableName,
        $Document,
        [string]$DatabaseType
    )

    try {
        # Cached, so this costs no query per document
        $tableColumns = Get-SQLTableColumns -Connection $Connection -TableName $TableName

        if ($tableColumns.Count -eq 0) {
            Write-N2SMessage "Insert error: no columns found for table $TableName" -Level Error
            return $false
        }

        # Scalar fields only; arrays and sub-documents belong in a child table
        $documentFields = @{}

        if ($Document -is [System.Collections.IDictionary]) {
            foreach ($key in $Document.Keys) {
                $value = $Document[$key]

                if (-not (Test-IsDocumentObject -Value $value) -and
                    ($null -eq $value -or $value -isnot [System.Collections.IEnumerable] -or $value -is [string])) {
                    $documentFields[$key] = $value
                }
            }
        }

        $documentId = if ($null -ne $Document['_id']) { $Document['_id'].ToString() } else { '<unknown>' }

        # Every column of the table, so the row fits the buffer's grouping;
        # a field the document does not have becomes NULL
        $row = [ordered]@{}

        foreach ($column in $tableColumns.Keys) {
            if (-not $documentFields.ContainsKey($column)) {
                $row[$column] = [DBNull]::Value
                continue
            }

            $converted = ConvertTo-SQLColumnValue -Value $documentFields[$column] `
                                                  -ColumnType $tableColumns[$column] `
                                                  -DatabaseType $DatabaseType

            if ($converted.Success) {
                $row[$column] = $converted.Value
            }
            else {
                Add-ConversionIssue -TableName $TableName -DocumentId $documentId -FieldName $column `
                                    -Reason $converted.Reason -Action 'stored as NULL'
                $row[$column] = [DBNull]::Value
            }
        }

        Add-SQLRow -Connection $Connection -TableName $TableName -Row $row -DocumentId $documentId
        return $true
    }
    catch {
        Write-N2SMessage "Insert error: $($_.Exception.Message)" -Level Error
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
        [string]$DatabaseType,
        $Transaction
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
        
        # Build UPDATE. A document without scalar fields (only _id, or only arrays
        # and sub-documents) has nothing to set: the main row is already correct and
        # an empty SET clause would be a syntax error.
        $setClauses = @()

        foreach ($field in $flatFields.Keys) {
            $setClauses += '`' + $field + '` = ?'
        }

        if ($setClauses.Count -eq 0) {
            return $true
        }

        $updateSQL = "UPDATE " + ('`' + $TableName + '`') + " SET " + ($setClauses -join ', ') + ' WHERE `_id` = ?'

        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = $updateSQL

        # The caller may have a transaction open for this chunk
        if ($Transaction) {
            $cmd.Transaction = $Transaction
        }
        
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
        Write-N2SMessage "Update error: $($_.Exception.Message)" -Level Error
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
        $cmd.CommandText = "DELETE FROM " + ('`' + $TableName + '`') + ' WHERE `_id` = ?'
        
        $param = $cmd.CreateParameter()
        $param.Value = $Id
        $cmd.Parameters.Add($param) | Out-Null
        
        $cmd.ExecuteNonQuery() | Out-Null
        return $true
    }
    catch {
        Write-N2SMessage "Delete error: $($_.Exception.Message)" -Level Error
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
                $report += "  - $err`n"
            }
        }
        
        $report | Out-File -FilePath $OutputPath -Encoding UTF8
        Write-N2SMessage "Sync report exported to: $OutputPath" -Level Success
        
        return $true
    }
    catch {
        Write-N2SMessage "Error exporting report: $($_.Exception.Message)" -Level Error
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
    
    Write-N2SMessage "`n$('=' * 60)" -Level Header
    Write-N2SMessage "  Scheduled Sync - $TableName" -Level Header
    Write-N2SMessage "$('=' * 60)`n" -Level Header
    
    # Run sync
    $syncResult = Start-IncrementalSync -TableName $TableName `
                                        -DatabaseType $DatabaseType `
                                        -ForceFullSync:$ForceFullSync
    
    # Export report
    $reportFile = ".\sync_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"
    Export-SyncReport -SyncResult $syncResult -OutputPath $reportFile
    
    Write-N2SMessage "`n Scheduled sync complete!" -Level Success
    Write-N2SMessage "  Report: $reportFile" -Level Detail
    
    return $syncResult
}


function Invoke-N2SMigration {
    <#
    .SYNOPSIS
    Non-interactive entry point for automated environments

    .DESCRIPTION
    Runs a migration, sync or validation without ever asking a question, and
    returns the result object with an ExitCode. Meant for Task Scheduler, cron
    or CI, where there is no keyboard to answer a prompt.

    Everything is driven by parameters and the configuration file, and the
    console output can be redirected or silenced by the caller.

    .PARAMETER Collections
    Collections to process. Empty means every collection in the database.

    .PARAMETER Operation
    FullMigration, IncrementalSync, ValidationOnly or SchemaOnly.

    .PARAMETER DatabaseType
    MySQL or SQLServer.

    .PARAMETER SampleSize
    Number of documents to analyse for the schema. Use a value at least as large
    as the collection to be sure no field is missed.

    .PARAMETER ConfigPath
    Path to the configuration file. Defaults to config.json next to the module.

    .PARAMETER Quiet
    Suppress the progress output; warnings and errors are still reported.

    .PARAMETER RemoveOrphanTables
    Drop SQL tables whose MongoDB collection no longer exists, including their
    data. Without this switch such tables are only reported. In an automated run
    there is nobody to answer a confirmation, so giving this switch counts as the
    confirmation itself.

    .OUTPUTS
    The workflow result, including ExitCode (0 = fine, 1 = a collection failed).

    .EXAMPLE
    # Scheduled sync of every collection, exit code for the scheduler
    $result = Invoke-N2SMigration -Operation IncrementalSync
    exit $result.ExitCode

    .EXAMPLE
    # Full migration of one collection, output to a log file
    Invoke-N2SMigration -Collections users -Operation FullMigration 6>> .\migration.log
    #>

    [CmdletBinding()]
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
        [int]$SampleSize = 100,

        [Parameter(Mandatory=$false)]
        [string]$ConfigPath,

        [Parameter(Mandatory=$false)]
        [switch]$Quiet,

        [Parameter(Mandatory=$false)]
        [switch]$RemoveOrphanTables
    )

    # An unreachable database or a broken configuration should come out as a
    # non-zero exit code, not as an unhandled exception in a scheduler log
    $result = @{
        Operation     = $Operation
        TotalSuccess  = 0
        TotalFailed   = 0
        TotalWarnings = 0
        Collections   = @()
        ExitCode      = 2
    }

    try {
        if ($ConfigPath) {
            if (-not (Test-Path $ConfigPath)) {
                throw "Config file not found: $ConfigPath"
            }

            # Get-AppConfig reads this path when no argument is given
            $script:N2SConfigPath = $ConfigPath
        }

        # Report through the PowerShell streams instead of straight to the
        # screen, so this run can be silenced, logged or filtered by severity
        Set-N2SOutputMode -Mode Stream

        if ($Quiet) {
            $InformationPreference = 'SilentlyContinue'
        }
        else {
            $InformationPreference = 'Continue'
        }

        if ($RemoveOrphanTables) {
            # There is no keyboard here to answer a confirmation, so passing
            # -RemoveOrphanTables to an automated run IS the confirmation
            Write-Warning "Orphan tables will be dropped without asking, including their data"
            $ConfirmPreference = 'None'
        }

        $workflowResult = Invoke-MigrationWorkflow -Collections $Collections `
                                                   -Operation $Operation `
                                                   -DatabaseType $DatabaseType `
                                                   -SampleSize $SampleSize `
                                                   -Force `
                                                   -RemoveOrphanTables:$RemoveOrphanTables

        if ($null -eq $workflowResult) {
            Write-Warning "No collections were processed"
            $result.ExitCode = 1
            return $result
        }

        return $workflowResult
    }
    catch {
        Write-Error "Migration run failed: $($_.Exception.Message)"
        $result.Error = $_.Exception.Message
        return $result
    }
    finally {
        # The menu expects coloured output on screen again
        Set-N2SOutputMode -Mode Console
    }
}

function Get-OrphanSQLTable {
    <#
    .SYNOPSIS
    Finds SQL tables that no longer have a MongoDB collection behind them

    .DESCRIPTION
    A collection that is removed from MongoDB leaves its SQL table behind: a sync
    of all collections works from the list in MongoDB, so a table whose
    collection is gone is never visited again. Child tables are named
    <collection>_<field>, so they belong to their parent collection.

    .PARAMETER Connection
    An open SQL connection.

    .PARAMETER Collections
    The collections that do exist in MongoDB.

    .OUTPUTS
    One object per orphan table, with its name and row count.
    #>

    param (
        $Connection,
        [string[]]$Collections
    )

    $orphans = @()
    $tables = @()

    try {
        $cmd = $Connection.CreateCommand()
        $cmd.CommandText = "SHOW TABLES"
        $reader = $cmd.ExecuteReader()

        while ($reader.Read()) {
            $tables += $reader.GetString(0)
        }
        $reader.Close()
    }
    catch {
        Write-N2SMessage "Warning: could not list tables: $($_.Exception.Message)" -Level Warning
        return $orphans
    }

    foreach ($table in $tables) {
        $owned = $false

        foreach ($collectionName in $Collections) {
            if ($table -eq $collectionName -or $table.StartsWith("${collectionName}_")) {
                $owned = $true
                break
            }
        }

        if (-not $owned) {
            $rowCount = Get-SQLTableRowCount -Connection $Connection -TableName $table

            $orphans += [PSCustomObject]@{
                Table = $table
                Rows  = if ($null -ne $rowCount) { $rowCount } else { 0 }
            }
        }
    }

    return $orphans
}

function Get-DocumentChildFieldName {
    <#
    .SYNOPSIS
    Names of the fields in these documents that need a child table

    .DESCRIPTION
    Arrays and sub-documents are the fields that get their own table. Every other
    field is a column of the main table.
    #>

    param (
        $Documents
    )

    $fields = @{}

    foreach ($document in $Documents) {
        if ($document -isnot [System.Collections.IDictionary]) {
            continue
        }

        foreach ($key in $document.Keys) {
            $value = $document[$key]

            if ((Test-IsDocumentObject -Value $value) -or
                ($null -ne $value -and $value -is [System.Collections.IEnumerable] -and $value -isnot [string])) {
                $fields[$key] = $true
            }
        }
    }

    return $fields
}

function Get-GhostChildTable {
    <#
    .SYNOPSIS
    Child tables of a collection whose field no longer exists in any document

    .DESCRIPTION
    The collection itself is still there, so the orphan check does not see these.
    Yet a field that disappeared from every document leaves its child table
    behind with rows that describe something that no longer exists.

    Only pass documents that cover the whole collection: a field that happens to
    be missing from a sample would otherwise look like it is gone.
    #>

    param (
        $Connection,
        [string]$TableName,
        $Documents,
        [string]$PrimaryKeyField = "_id"
    )

    $ghosts = @()
    $childTables = Get-ChildTableMap -Connection $Connection -TableName $TableName -PrimaryKeyField $PrimaryKeyField

    if ($childTables.Count -eq 0) {
        return $ghosts
    }

    $presentFields = Get-DocumentChildFieldName -Documents $Documents

    foreach ($fieldName in $childTables.Keys) {
        if (-not $presentFields.ContainsKey($fieldName)) {
            $table = $childTables[$fieldName]
            $rowCount = Get-SQLTableRowCount -Connection $Connection -TableName $table

            $ghosts += [PSCustomObject]@{
                Table = $table
                Field = $fieldName
                Rows  = if ($null -ne $rowCount) { $rowCount } else { 0 }
            }
        }
    }

    return $ghosts
}

function Remove-OrphanSQLTable {
    <#
    .SYNOPSIS
    Drops one orphan table, after confirmation

    .DESCRIPTION
    Dropping a table deletes the table and every row in it, and the data cannot
    come back from MongoDB because the collection is gone. So this asks for
    confirmation first. An automated run that means it can pass -Confirm:$false,
    and -WhatIf shows what would happen without touching anything.
    #>

    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'High')]
    param (
        $Connection,
        [Parameter(Mandatory = $true)]
        [string]$TableName,
        [int]$RowCount = 0
    )

    $target = "table '$TableName' with $RowCount row(s)"
    $action = "DROP TABLE - deletes the table and its data permanently"

    if (-not $PSCmdlet.ShouldProcess($target, $action)) {
        Write-N2SMessage " Kept table '$TableName'" -Level Info
        return $false
    }

    try {
        Invoke-SQLNonQuery -Connection $Connection -CommandText ('DROP TABLE IF EXISTS `' + $TableName + '`') | Out-Null
        Write-N2SMessage " Dropped table '$TableName' ($RowCount row(s) deleted)" -Level Warning
        return $true
    }
    catch {
        Write-N2SMessage " Could not drop table '$TableName': $($_.Exception.Message)" -Level Error
        return $false
    }
}

function Get-OrphanTableDropOrder {
    <#
    .SYNOPSIS
    Puts child tables before their parent, so a foreign key cannot block the drop
    #>

    param (
        $Orphans
    )

    $names = @($Orphans | Select-Object -ExpandProperty Table)

    return @($Orphans | Sort-Object -Property @{
        Expression = {
            # A table that starts with the name of another orphan is a child
            $isChild = $false
            foreach ($other in $names) {
                if ($_.Table -ne $other -and $_.Table.StartsWith("${other}_")) {
                    $isChild = $true
                    break
                }
            }
            -not $isChild
        }
    }, Table)
}

function Get-CollectionResultStatus {
    <#
    .SYNOPSIS
    Decides whether the work for one collection really succeeded

    .DESCRIPTION
    Not throwing is not the same as succeeding: a sync catches its own errors and
    returns a result, so the returned result has to be inspected. Warnings do not
    count as failure - a missing child table is a hint, not a broken sync.
    #>

    param (
        $Details
    )

    $status = @{
        Success = $true
        Reason  = $null
        Warning = $null
    }

    if ($null -eq $Details -or $Details -isnot [System.Collections.IDictionary]) {
        return $status
    }

    # Incremental sync
    if ($Details.Contains('Sync') -and $null -ne $Details['Sync']) {
        $syncErrors = @($Details['Sync'].Errors)

        if ($syncErrors.Count -gt 0) {
            $status.Success = $false
            $status.Reason = "sync reported $($syncErrors.Count) error(s): " + ($syncErrors -join '; ')
            return $status
        }

        # Things worth knowing but not failures: a field without a child table,
        # a child table without a field
        $syncWarnings = @($Details['Sync'].Warnings)

        if ($syncWarnings.Count -gt 0) {
            $status.Warning = "sync reported $($syncWarnings.Count) warning(s): " + ($syncWarnings -join '; ')
        }
    }

    # Full migration, also used when a sync falls back to one because the
    # table does not exist yet
    if ($Details.Contains('Migration') -and $null -ne $Details['Migration']) {
        $migration = $Details['Migration']

        if ($migration.FailedDocuments -gt 0) {
            $status.Success = $false
            $status.Reason = "$($migration.FailedDocuments) of $($migration.TotalDocuments) documents failed to migrate"
            return $status
        }

        # Values that did not fit their column were handled on purpose
        # (see Migration.OnConversionError), so this is a warning, not a failure
        $conversionIssues = @($migration.ConversionIssues)

        if ($conversionIssues.Count -gt 0) {
            $status.Warning = "$($conversionIssues.Count) value(s) could not be converted; see the conversion report"
        }
    }

    # Validation, from a full migration as well as from a validation-only run
    if ($Details.Contains('Validation') -and $null -ne $Details['Validation']) {
        $validation = $Details['Validation']
        $issueCount = @($validation.Issues).Count

        if ($validation.OverallStatus -in @('FAILED', 'ERROR')) {
            $status.Success = $false
            $status.Reason = "validation $($validation.OverallStatus) with $issueCount issue(s)"
            return $status
        }

        # Fewer rows in SQL than documents in MongoDB means data is missing,
        # whatever the sample check says about the rest
        if ($validation.RecordCountMatch -eq $false) {
            $status.Success = $false
            $status.Reason = "record counts do not match: MongoDB=$($validation.MongoCount), SQL=$($validation.SQLCount)"
            return $status
        }

        if ($validation.OverallStatus -eq 'PARTIAL') {
            $partialWarning = "validation PARTIAL with $issueCount issue(s)"

            # Keep an earlier warning about conversions: both matter
            if ($status.Warning) {
                $status.Warning = "$($status.Warning); $partialWarning"
            }
            else {
                $status.Warning = $partialWarning
            }
        }
    }

    return $status
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
        [int]$SampleSize = 100,

        [Parameter(Mandatory=$false)]
        [switch]$Force,

        [Parameter(Mandatory=$false)]
        [switch]$RemoveOrphanTables
    )

    Write-N2SMessage "`n$('=' * 70)" -Level Header
    Write-N2SMessage "  NoSQL to SQL Migration Tool - Multi-Collection Workflow" -Level Header
    Write-N2SMessage "$('=' * 70)`n" -Level Header
    
    # Load configuration
    $script:AppConfig = Get-AppConfig

    # "This table is unused" only holds when the whole database was looked at.
    # After a run on one collection, other tables are simply none of its business.
    $coversWholeDatabase = ($Collections.Count -eq 0)

    # Get collections to process
    if ($Collections.Count -eq 0) {
        Write-N2SMessage "Discovering collections..." -Level Step
        $discoveredCollections = @(Get-MongoDBCollections)
        
        if ($discoveredCollections.Count -eq 0) {
            Write-N2SMessage "No collections found in database." -Level Error
            return
        }
        
        Write-N2SMessage "Found $($discoveredCollections.Count) collection(s): $($discoveredCollections -join ', ')" -Level Success

        # Ask for confirmation, unless the caller already decided. Without -Force
        # this prompt blocks a scheduled task, which has no keyboard to answer it.
        $response = if ($Force) { 'Y' } else { Read-Host "`nProcess ALL collections? (Y/N)" }

        if ($response -ne 'Y' -and $response -ne 'y') {
            Write-N2SMessage "Operation cancelled." -Level Step
            return
        }
        
        $Collections = $discoveredCollections
    }
    
    Write-N2SMessage "`nOperation: $Operation" -Level Header
    Write-N2SMessage "Collections: $($Collections -join ', ')" -Level Header
    Write-N2SMessage "Database Type: $DatabaseType" -Level Header
    Write-N2SMessage "" -Level Info
    
    # Overall results. Ordered, so the fields always print in a sensible order:
    # a plain hashtable has no order and showed EndTime before StartTime.
    $overallResults = [ordered]@{
        Operation = $Operation
        StartTime = Get-Date
        EndTime = $null
        Duration = $null
        DurationSeconds = 0
        Collections = @()
        TotalSuccess = 0
        TotalWarnings = 0
        TotalFailed = 0
        OrphanTables = @()
        OrphanTablesRemoved = @()
        # 0 = everything fine, 1 = one or more collections failed.
        # A caller in an automated environment can use this as its exit code.
        ExitCode = 0
    }
    
    # Process each collection
    foreach ($collectionName in $Collections) {
        Write-N2SMessage "`n$('─' * 70)" -Level Detail
        Write-N2SMessage "Processing Collection: $collectionName" -Level Step
        Write-N2SMessage ("─"*70) -Level Detail
        
        $collectionResult = @{
            Name = $collectionName
            Success = $false
            Error = $null
            Warning = $null
            Details = $null
        }

        try {
            switch ($Operation) {
                "FullMigration" {
                    $collectionResult.Details = Invoke-FullMigration -CollectionName $collectionName `
                                                                     -DatabaseType $DatabaseType `
                                                                     -SampleSize $SampleSize
                }

                "IncrementalSync" {
                    $collectionResult.Details = Invoke-IncrementalMigration -CollectionName $collectionName `
                                                                            -DatabaseType $DatabaseType `
                                                                            -SampleSize $SampleSize
                }

                "ValidationOnly" {
                    $collectionResult.Details = Invoke-ValidationOnly -CollectionName $collectionName `
                                                                      -DatabaseType $DatabaseType `
                                                                      -SampleSize $SampleSize
                }

                "SchemaOnly" {
                    $collectionResult.Details = Invoke-SchemaOnly -CollectionName $collectionName `
                                                                  -SampleSize $SampleSize
                }
            }

            # Not throwing is not the same as succeeding: failed documents, a
            # failed validation or a count mismatch all mean this collection
            # did not finish correctly, whatever operation produced them.
            $status = Get-CollectionResultStatus -Details $collectionResult.Details
            $collectionResult.Success = $status.Success
            $collectionResult.Error = $status.Reason
            $collectionResult.Warning = $status.Warning
            
            if ($collectionResult.Success) {
                $overallResults.TotalSuccess++

                if ($collectionResult.Warning) {
                    $overallResults.TotalWarnings++
                    Write-N2SMessage " $collectionName completed with warnings: $($collectionResult.Warning)" -Level Step
                }
                else {
                    Write-N2SMessage " $collectionName completed successfully" -Level Success
                }
            }
            else {
                $overallResults.TotalFailed++
                Write-N2SMessage " $collectionName completed with errors: $($collectionResult.Error)" -Level Error
            }
        }
        catch {
            $collectionResult.Error = $_.Exception.Message
            $overallResults.TotalFailed++
            Write-N2SMessage " $collectionName failed: $($_.Exception.Message)" -Level Error
        }
        
        $overallResults.Collections += $collectionResult
    }
    
    # Tables whose collection no longer exists in MongoDB. Only worth saying
    # after a run over the whole database, or when the caller asked for the
    # cleanup: a run on one collection says nothing about the other tables.
    # For a one-off check there is menu option 10.
    if ($Operation -ne 'SchemaOnly' -and ($coversWholeDatabase -or $RemoveOrphanTables)) {
        try {
            $existingCollections = @(Get-MongoDBCollections)
            $orphanConnection = Get-SQLConnectionObject -DatabaseType $DatabaseType
            $orphanConnection.Open()

            try {
                $orphans = @(Get-OrphanSQLTable -Connection $orphanConnection -Collections $existingCollections)
                $overallResults.OrphanTables = @($orphans | Select-Object -ExpandProperty Table)

                if ($orphans.Count -gt 0) {
                    Write-N2SMessage "`nTables without a MongoDB collection:" -Level Warning

                    foreach ($orphan in $orphans) {
                        Write-N2SMessage "  $($orphan.Table) ($($orphan.Rows) row(s))" -Level Warning
                    }

                    if ($RemoveOrphanTables) {
                        Write-N2SMessage "  These tables and their data will be dropped permanently." -Level Warning

                        foreach ($orphan in (Get-OrphanTableDropOrder -Orphans $orphans)) {
                            try {
                                if (Remove-OrphanSQLTable -Connection $orphanConnection -TableName $orphan.Table -RowCount $orphan.Rows) {
                                    $overallResults.OrphanTablesRemoved += $orphan.Table
                                }
                            }
                            catch {
                                # A run without a keyboard cannot answer the
                                # confirmation; one refusal must not stop the rest
                                Write-N2SMessage " Kept table '$($orphan.Table)': $($_.Exception.Message)" -Level Warning
                                Write-N2SMessage "  Use -Confirm:`$false to drop tables in an automated run" -Level Info
                            }
                        }
                    }
                    else {
                        Write-N2SMessage "  Left untouched. Use -RemoveOrphanTables to drop them." -Level Info
                    }
                }
            }
            finally {
                $orphanConnection.Close()
            }
        }
        catch {
            Write-N2SMessage "Warning: could not check for orphan tables: $($_.Exception.Message)" -Level Warning
        }
    }

    # Display overall summary
    $overallResults.EndTime = Get-Date
    $duration = $overallResults.EndTime - $overallResults.StartTime
    $overallResults.Duration = $duration.ToString('hh\:mm\:ss')
    $overallResults.DurationSeconds = [math]::Round($duration.TotalSeconds, 2)

    Write-N2SMessage "`n$('=' * 70)" -Level Header
    Write-N2SMessage "Overall Summary" -Level Header
    Write-N2SMessage ("="*70) -Level Header
    Write-N2SMessage "Duration: $($overallResults.Duration) ($($overallResults.DurationSeconds) seconds)" -Level Detail
    Write-N2SMessage "Collections Processed: $($Collections.Count)" -Level Detail
    # An automated caller should be able to act on this without reading output
    if ($overallResults.TotalFailed -gt 0) {
        $overallResults.ExitCode = 1
    }

    Write-N2SMessage "Successful: $($overallResults.TotalSuccess)" -Level Success
    Write-N2SMessage "With warnings: $($overallResults.TotalWarnings)" -Level $(if ($overallResults.TotalWarnings -gt 0) { 'Warning' } else { 'Detail' })
    Write-N2SMessage "Failed: $($overallResults.TotalFailed)" -Level $(if ($overallResults.TotalFailed -gt 0) { 'Error' } else { 'Detail' })
    Write-N2SMessage "Exit code: $($overallResults.ExitCode)" -Level $(if ($overallResults.ExitCode -ne 0) { 'Error' } else { 'Detail' })

    Write-N2SMessage "`nCollection Results:" -Level Step
    foreach ($result in $overallResults.Collections) {
        $level = if (-not $result.Success) { 'Error' } elseif ($result.Warning) { 'Warning' } else { 'Success' }
        Write-N2SMessage "  $($result.Name)" -Level $level

        if ($result.Error) {
            Write-N2SMessage "    Error: $($result.Error)" -Level Error
        }

        if ($result.Warning) {
            Write-N2SMessage "    Warning: $($result.Warning)" -Level Step
        }
    }

    Write-N2SMessage "$('=' * 70)`n" -Level Header
    
    # Export overall report
    $reportFile = ".\workflow_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').json"
    $overallResults | ConvertTo-Json -Depth 10 | Out-File -FilePath $reportFile -Encoding UTF8
    Write-N2SMessage "Workflow report exported to: $reportFile`n" -Level Success
    
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
                Write-N2SMessage "  Found collection: $name" -Level Detail
                $collectionNames += $name
            }
        }

        if ($collectionNames.Count -eq 0) {
            Write-N2SMessage "  Warning: No collections found in database $($script:AppConfig.MongoDB.Database)" -Level Step
        }

        return $collectionNames
    }
    catch {
        Write-N2SMessage "Error retrieving collections: $($_.Exception.Message)" -Level Error
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
        Write-N2SMessage "`n[1/4] Analyzing MongoDB schema..." -Level Header
        $result.Schema = Get-MongoDBSchema -ConnectionString $script:AppConfig.MongoDB.ConnectionString `
                                          -DatabaseName $script:AppConfig.MongoDB.Database `
                                          -CollectionName $CollectionName `
                                          -SampleSize $SampleSize
        
        # Step 2: Generate SQL schema
        Write-N2SMessage "`n[2/4] Generating SQL schema..." -Level Header
        $result.SQLSchema = New-SQLSchema -Schema $result.Schema `
                                         -TableName $CollectionName `
                                         -PrimaryKeyField "_id"
        
        Export-SQLSchema -SchemaResult $result.SQLSchema `
                        -OutputPath ".\schema_$CollectionName.sql" | Out-Null
        
        # Step 3: Migrate data
        Write-N2SMessage "`n[3/4] Migrating data..." -Level Header
        $result.Migration = Start-DataMigration -Schema $result.Schema `
                                               -SQLSchema $result.SQLSchema `
                                               -CollectionName $CollectionName `
                                               -BatchSize 100 `
                                               -DatabaseType $DatabaseType
        
        # Step 4: Validate
        Write-N2SMessage "`n[4/4] Validating migration..." -Level Header
        $result.Validation = Test-MigrationValidation -TableName $CollectionName `
                                                      -SampleSize 10 `
                                                      -DatabaseType $DatabaseType
        
        return $result
    }
    catch {
        Write-N2SMessage "Error in full migration: $($_.Exception.Message)" -Level Error
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
            Write-N2SMessage "Table doesn't exist, performing full migration..." -Level Step
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
        Write-N2SMessage "Error in incremental migration: $($_.Exception.Message)" -Level Error
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
        Write-N2SMessage "Error in validation: $($_.Exception.Message)" -Level Error
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
        Write-N2SMessage "Error in schema analysis: $($_.Exception.Message)" -Level Error
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

        [switch]$FullMigration,

        [int]$SampleSize = 100
    )

    $operation = if ($FullMigration) { "FullMigration" } else { "IncrementalSync" }

    Invoke-MigrationWorkflow -Collections @($CollectionName) `
                            -Operation $operation `
                            -DatabaseType $DatabaseType `
                            -SampleSize $SampleSize
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
            "10" { Menu-CleanupOrphanTables }
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
    Write-Host "│  MAINTENANCE                                               │" -ForegroundColor Yellow
    Write-Host "├────────────────────────────────────────────────────────────┤" -ForegroundColor DarkGray
    Write-Host "│ [10] Clean Up Tables Without a Collection                   │" -ForegroundColor White
    Write-Host "│                                                            │" -ForegroundColor DarkGray
    Write-Host "│  [0] Exit                                                  │" -ForegroundColor Red
    Write-Host "└────────────────────────────────────────────────────────────┘" -ForegroundColor DarkGray
}

function Menu-TestConnections {
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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
        
        $sampleSize = Read-SampleSize -DocumentCount (Get-CollectionDocumentCount -CollectionName $collectionName)

        $confirm = Read-Host "`nThis will perform a FULL MIGRATION (Schema + Data). Continue? (Y/N)"

        if ($confirm -eq 'Y' -or $confirm -eq 'y') {
            Migrate-Collection -CollectionName $collectionName -FullMigration -SampleSize $sampleSize
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
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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
    
    $sampleSize = Read-SampleSize
    $confirm = Read-Host "`nMigrate these collections? (Y/N)"
    
    if ($confirm -eq 'Y' -or $confirm -eq 'y') {
        Invoke-MigrationWorkflow -Collections $selectedCollections -Operation FullMigration -SampleSize $sampleSize
    }
    else {
        Write-Host "`nOperation cancelled." -ForegroundColor Yellow
    }
}

function Menu-MigrateAll {
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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
    $sampleSize = Read-SampleSize
    $confirm = Read-Host "`nAre you sure? (Y/N)"
    
    if ($confirm -eq 'Y' -or $confirm -eq 'y') {
        Invoke-MigrationWorkflow -Collections $collections -Operation FullMigration -SampleSize $sampleSize
    }
    else {
        Write-Host "`nOperation cancelled." -ForegroundColor Yellow
    }
}

function Menu-SyncSingle {
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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

function Get-CollectionDocumentCount {
    <#
    .SYNOPSIS
    Number of documents in a collection, or 0 when it cannot be read
    #>

    param (
        [string]$CollectionName
    )

    try {
        Connect-Mdbc -ConnectionString $script:AppConfig.MongoDB.ConnectionString `
                     -DatabaseName $script:AppConfig.MongoDB.Database `
                     -CollectionName $CollectionName

        return [int](Get-MdbcData -Count)
    }
    catch {
        return 0
    }
}

function Read-SampleSize {
    <#
    .SYNOPSIS
    Asks how many documents to analyse for the schema

    .DESCRIPTION
    The schema comes from a sample, so a field that only appears in a later
    document gets no column. Analysing everything is the safe answer; on a very
    large collection it costs time, which is why it is a question and not a fixed
    number. Pressing enter keeps the suggested value.
    #>

    param (
        [int]$DocumentCount = 0
    )

    $suggested = if ($DocumentCount -gt 0) { $DocumentCount } else { 100 }

    Write-Host "`nThe schema is built from a sample. A field that appears only outside" -ForegroundColor Gray
    Write-Host "the sample gets no column, so analysing every document is the safe choice." -ForegroundColor Gray

    $answer = Read-Host "Documents to analyse (enter for $suggested)"

    if ([string]::IsNullOrWhiteSpace($answer)) {
        return $suggested
    }

    $parsed = 0
    if ([int]::TryParse($answer.Trim(), [ref]$parsed) -and $parsed -gt 0) {
        return $parsed
    }

    Write-Host "Not a number, using $suggested" -ForegroundColor Yellow
    return $suggested
}

function Menu-CleanupOrphanTables {
    <#
    .SYNOPSIS
    Menu item: show tables without a collection and offer to drop them

    .DESCRIPTION
    Asking the questions is the job of the menu, so this is where the user is
    shown exactly what is about to be lost. The dropping itself is done by
    Remove-OrphanSQLTable, which asks for its own confirmation per table.
    #>

    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
    Write-Host "Clean Up Tables Without a Collection" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Cyan

    $collections = @(Get-MongoDBCollections)

    if ($collections.Count -eq 0) {
        Write-Host "`nNo collections found in MongoDB." -ForegroundColor Yellow
        Write-Host "Stopping: without that list every table would look unused." -ForegroundColor Yellow
        return
    }

    $connection = $null

    try {
        $connection = Get-SQLConnectionObject -DatabaseType "MySQL"
        $connection.Open()

        $orphans = @(Get-OrphanSQLTable -Connection $connection -Collections $collections)

        # Second kind: the collection still exists, but a field that had its own
        # child table is gone from every document. Reading all documents is the
        # only way to be sure, and being sure matters before dropping anything.
        Write-Host "`nChecking the child tables of each collection..." -ForegroundColor Gray


        foreach ($collectionName in $collections) {
            Connect-Mdbc -ConnectionString $script:AppConfig.MongoDB.ConnectionString `
                         -DatabaseName $script:AppConfig.MongoDB.Database `
                         -CollectionName $collectionName

            $documents = @(Get-MdbcData)

            foreach ($ghost in (Get-GhostChildTable -Connection $connection -TableName $collectionName -Documents $documents)) {
                $orphans += [PSCustomObject]@{
                    Table = $ghost.Table
                    Rows  = $ghost.Rows
                    Field = $ghost.Field
                }
            }
        }

        if ($orphans.Count -eq 0) {
            Write-Host "`nEvery table still has something behind it in MongoDB. Nothing to clean up." -ForegroundColor Green
            return
        }

        Write-Host "`nThese tables have nothing behind them in MongoDB anymore:" -ForegroundColor Yellow

        foreach ($orphan in $orphans) {
            Write-Host "  $($orphan.Table)" -NoNewline -ForegroundColor White
            Write-Host " - $($orphan.Rows) row(s)" -NoNewline -ForegroundColor Gray

            if ($orphan.PSObject.Properties.Name -contains 'Field' -and $orphan.Field) {
                Write-Host " (field '$($orphan.Field)' no longer exists)" -ForegroundColor DarkGray
            }
            else {
                Write-Host " (collection is gone)" -ForegroundColor DarkGray
            }
        }

        Write-Host "`nDropping a table deletes the table and every row in it." -ForegroundColor Red
        Write-Host "The data cannot be restored, because what it described no longer exists in MongoDB." -ForegroundColor Red

        $answer = Read-Host "`nDrop these tables? Type YES to continue"

        if ($answer -ne 'YES') {
            Write-Host "`nNothing was dropped." -ForegroundColor Green
            return
        }

        # Child tables first, so a foreign key cannot block the drop
        $dropped = 0

        foreach ($orphan in (Get-OrphanTableDropOrder -Orphans $orphans)) {
            if (Remove-OrphanSQLTable -Connection $connection -TableName $orphan.Table -RowCount $orphan.Rows) {
                $dropped++
            }
        }

        Write-Host "`nDropped $dropped of $($orphans.Count) table(s)." -ForegroundColor Yellow
    }
    catch {
        Write-Host "`nCleanup failed: $($_.Exception.Message)" -ForegroundColor Red
    }
    finally {
        if ($connection -and $connection.State -eq 'Open') {
            $connection.Close()
        }
    }
}

function Menu-SchemaOnly {
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
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

        $sampleSize = Read-SampleSize -DocumentCount (Get-CollectionDocumentCount -CollectionName $collectionName)

        Invoke-MigrationWorkflow -Collections @($collectionName) -Operation SchemaOnly -SampleSize $sampleSize
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


Export-ModuleMember -Function Start-MigrationToolMenu, Invoke-MigrationWorkflow, Invoke-N2SMigration, Get-AppConfig