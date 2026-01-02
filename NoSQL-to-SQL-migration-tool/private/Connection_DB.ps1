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

            try {
                Add-Type -AssemblyName "MySql.Data" -ErrorAction Stop
            }
            catch {
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

            Add-Type -AssemblyName "MySql.Data" -ErrorAction SilentlyContinue

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
