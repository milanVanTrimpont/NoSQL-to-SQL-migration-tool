<#
.SYNOPSIS
End to end tests against a real MongoDB and MySQL.

.DESCRIPTION
These tests migrate a temporary collection and check what actually lands in the
database, including the cases that used to lose documents. They need running
databases, so they are tagged Integration and skipped by default:

    pwsh -File .\Tests\Invoke-Tests.ps1 -Integration

Everything they create is named n2s_test_* and is removed afterwards.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    $script:TestCollection = "n2s_test_integration"
    $script:Config = Get-AppConfig

    function Get-TestSqlConnection {
        $connection = New-Object MySql.Data.MySqlClient.MySqlConnection
        $connection.ConnectionString = "Server=$($script:Config.MySQL.Server);Port=$($script:Config.MySQL.Port);" +
                                       "Database=$($script:Config.MySQL.Database);Uid=$($script:Config.MySQL.Username);" +
                                       "Pwd=$($script:Config.MySQL.Password);SslMode=Disabled;AllowPublicKeyRetrieval=True;"
        $connection.Open()
        return $connection
    }

    function Get-TestRowCount {
        param([string]$Table)

        $connection = Get-TestSqlConnection
        try {
            $command = $connection.CreateCommand()
            $command.CommandText = "SELECT COUNT(*) FROM ``$Table``"
            return [int]$command.ExecuteScalar()
        }
        catch {
            return -1
        }
        finally {
            $connection.Close()
        }
    }

    function Remove-TestTables {
        $connection = Get-TestSqlConnection
        try {
            foreach ($table in @("$($script:TestCollection)_genres", "$($script:TestCollection)_ratings", $script:TestCollection)) {
                $command = $connection.CreateCommand()
                $command.CommandText = "DROP TABLE IF EXISTS ``$table``"
                $command.ExecuteNonQuery() | Out-Null
            }
        }
        finally {
            $connection.Close()
        }
    }
}

Describe "Full migration against real databases" -Tag Integration {

    BeforeAll {
        Connect-Mdbc -ConnectionString $script:Config.MongoDB.ConnectionString `
                     -DatabaseName $script:Config.MongoDB.Database `
                     -CollectionName $script:TestCollection -NewCollection

        # A long text, an array, and a date written in three different ways:
        # exactly the mix that used to lose documents
        Add-MdbcData @{ _id = [MongoDB.Bson.ObjectId]::GenerateNewId(); title = "Long"
                        storyline = ("x" * 900); genres = @("Drama", "Crime"); ratings = @(8, 9, 10)
                        created = [datetime]"2020-01-02" }
        Add-MdbcData @{ _id = [MongoDB.Bson.ObjectId]::GenerateNewId(); title = "Text date"
                        storyline = "short"; genres = @("Comedy"); ratings = @(5)
                        created = "06/05/2022" }
        Add-MdbcData @{ _id = [MongoDB.Bson.ObjectId]::GenerateNewId(); title = "No date"
                        storyline = "short"; genres = @(); ratings = @()
                        created = "onbekend" }

        $script:Result = Invoke-N2SMigration -Collections @($script:TestCollection) `
                                             -Operation FullMigration -SampleSize 100 -Quiet
    }

    AfterAll {
        Connect-Mdbc -ConnectionString $script:Config.MongoDB.ConnectionString `
                     -DatabaseName $script:Config.MongoDB.Database `
                     -CollectionName $script:TestCollection
        Remove-MdbcCollection -Name $script:TestCollection
        Remove-TestTables
    }

    It "migrates every document, whatever format the values are in" {
        Get-TestRowCount -Table $script:TestCollection | Should -Be 3
    }

    It "stores a text of 900 characters without losing the document" {
        $connection = Get-TestSqlConnection
        try {
            $command = $connection.CreateCommand()
            $command.CommandText = "SELECT LENGTH(storyline) FROM ``$($script:TestCollection)`` WHERE title = 'Long'"
            [int]$command.ExecuteScalar() | Should -Be 900
        }
        finally {
            $connection.Close()
        }
    }

    It "puts the array elements in their own child table" {
        Get-TestRowCount -Table "$($script:TestCollection)_genres" | Should -Be 3
        Get-TestRowCount -Table "$($script:TestCollection)_ratings" | Should -Be 4
    }

    It "ends with exit code 0" {
        $script:Result.ExitCode | Should -Be 0
    }
}

Describe "Sync repairs child rows that were removed in SQL" -Tag Integration {

    BeforeAll {
        Connect-Mdbc -ConnectionString $script:Config.MongoDB.ConnectionString `
                     -DatabaseName $script:Config.MongoDB.Database `
                     -CollectionName $script:TestCollection -NewCollection

        Add-MdbcData @{ _id = [MongoDB.Bson.ObjectId]::GenerateNewId(); title = "Heat"; ratings = @(1, 2, 3, 4, 5) }

        Invoke-N2SMigration -Collections @($script:TestCollection) -Operation FullMigration -SampleSize 100 -Quiet | Out-Null
    }

    AfterAll {
        Connect-Mdbc -ConnectionString $script:Config.MongoDB.ConnectionString `
                     -DatabaseName $script:Config.MongoDB.Database `
                     -CollectionName $script:TestCollection
        Remove-MdbcCollection -Name $script:TestCollection
        Remove-TestTables

        Remove-Item (Join-Path (Get-Location) "sync_state_$($script:TestCollection).json") -ErrorAction SilentlyContinue
    }

    It "puts the removed rows back" {
        $table = "$($script:TestCollection)_ratings"
        Get-TestRowCount -Table $table | Should -Be 5

        $connection = Get-TestSqlConnection
        try {
            $command = $connection.CreateCommand()
            $command.CommandText = "DELETE FROM ``$table`` LIMIT 3"
            $command.ExecuteNonQuery() | Out-Null
        }
        finally {
            $connection.Close()
        }

        Get-TestRowCount -Table $table | Should -Be 2

        Invoke-N2SMigration -Collections @($script:TestCollection) -Operation IncrementalSync -Quiet | Out-Null

        Get-TestRowCount -Table $table | Should -Be 5
    }
}
