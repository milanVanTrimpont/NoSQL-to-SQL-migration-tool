<#
.SYNOPSIS
    Unit tests for Connection_DB.ps1 functions.
.DESCRIPTION
    This script contains Pester tests for the functions defined in Connection_DB.ps1.
#>

# Load the Connection_DB.ps1 script
BeforeAll {
    $privatePath = Join-Path $PSScriptRoot "..\private"

    . (Join-Path $privatePath "Config.ps1")
    . (Join-Path $privatePath "Connection_DB.ps1")
}

Describe "Test-MongoDBConnection" {

    BeforeEach {
        Mock Connect-Mdbc {}
        Mock Get-MdbcData { @(1,2,3) }
        Mock Write-Host {}
    }
    
    It "Returns true when MongoDB connection succeeds" {
        Test-MongoDBConnection `
            -ConnectionString "mongodb://fake" `
            -DatabaseName "testdb" `
            -CollectionName "users" |
            Should -BeTrue
    }

    It "Returns false when MongoDB connection throws" {
        Mock Connect-Mdbc { throw "Mongo error" }

        Test-MongoDBConnection `
            -ConnectionString "mongodb://fake" `
            -DatabaseName "testdb" `
            -CollectionName "users" |
            Should -BeFalse
    }
}

Describe "Test-MySQLConnection" {

    BeforeEach {
        Mock Add-Type {}

        Mock New-Object -ParameterFilter {
            $TypeName -eq 'MySql.Data.MySqlClient.MySqlConnection'
        } {
            $conn = [pscustomobject]@{
                ConnectionString = ""
            }

            $conn | Add-Member ScriptMethod Open { return }
            $conn | Add-Member ScriptMethod Close { return }

            return $conn
        }

        Mock Write-Host {}
    }

    It "Returns true when MySQL connection succeeds" {
        Test-MySQLConnection `
            -Server "db" `
            -Database "testdb" `
            -Username "user" `
            -Password "pass" |
            Should -BeTrue
    }

    It "Returns false when MySQL driver is missing" {
        Mock Add-Type { throw "Driver missing" }

        Test-MySQLConnection `
            -Server "db" `
            -Database "testdb" `
            -Username "user" `
            -Password "pass" |
            Should -BeFalse
    }
}

Describe "Test-SQLServerConnection" {

    BeforeEach {
        Mock New-Object -ParameterFilter {
            $TypeName -eq 'System.Data.SqlClient.SqlConnection'
        } {
            $conn = [pscustomobject]@{
                ConnectionString = ""
            }

            $conn | Add-Member ScriptMethod Open { return }
            $conn | Add-Member ScriptMethod Close { return }

            return $conn
        }

        Mock Write-Host {}
    }

    It "Returns true when SQL Server connection succeeds" {
        Test-SQLServerConnection `
            -Server "sqlserver" `
            -Database "testdb" |
            Should -BeTrue
    }

    It "Returns false when SQL Server connection throws" {
        Mock New-Object -ParameterFilter {
            $TypeName -eq 'System.Data.SqlClient.SqlConnection'
        } { throw "SQL error" }

        Test-SQLServerConnection `
            -Server "sqlserver" `
            -Database "testdb" |
            Should -BeFalse
    }
}

Describe "Initialize-DatabaseConnections" {

    BeforeEach {
        Mock Get-AppConfig {
            @{
                MongoDB = @{
                    ConnectionString = "mongodb://fake"
                    Database = "testdb"
                    Collection = "users"
                }
                MySQL = @{
                    Server = "db"
                    Database = "testdb"
                    Port = 3306
                    Username = "u"
                    Password = "p"
                }
            }
        }

        Mock Test-MongoDBConnection { $true }
        Mock Test-MySQLConnection { $true }
        Mock Test-SQLServerConnection { $true }

        Mock Write-Host {}
    }

    It "Returns true when all connections succeed (MySQL)" {
        Initialize-DatabaseConnections -DatabaseType "MySQL" |
            Should -BeTrue
    }

    It "Returns false when MongoDB connection fails" {
        Mock Test-MongoDBConnection { $false }

        Initialize-DatabaseConnections -DatabaseType "MySQL" |
            Should -BeFalse
    }
}

Describe "Get-SQLConnection" {

    It "Returns MySQL connection object when DatabaseType is MySQL" {
        Mock Add-Type {}

        Mock New-Object -ParameterFilter {
            $TypeName -eq 'MySql.Data.MySqlClient.MySqlConnection'
        } {
            [pscustomobject]@{ ConnectionString = "" }
        }

        $config = @{
            MySQL = @{
                Server = "db"
                Port = 3306
                Database = "testdb"
                Username = "u"
                Password = "p"
            }
        }

        Get-SQLConnection -Config $config -DatabaseType "MySQL" |
            Should -Not -BeNullOrEmpty
    }

    It "Returns SQL Server connection object when DatabaseType is SQLServer" {
        Mock New-Object -ParameterFilter {
            $TypeName -eq 'System.Data.SqlClient.SqlConnection'
        } {
            [pscustomobject]@{ ConnectionString = "" }
        }

        $config = @{
            SQLServer = @{
                Server = "sqlserver"
                Database = "testdb"
            }
        }

        Get-SQLConnection -Config $config -DatabaseType "SQLServer" |
            Should -Not -BeNullOrEmpty
    }
}
