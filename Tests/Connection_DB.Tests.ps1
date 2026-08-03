<#
.SYNOPSIS
Tests for the connection checks, the configuration and the entry point.

.DESCRIPTION
The database calls are mocked, so these tests say something about the code and
not about whether a database happens to be running.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "Test-MongoDBConnection" {

    It "reports success when MongoDB answers" {
        InModuleScope NoSqlToSqlMigration {
            Mock Connect-Mdbc { }
            Mock Get-MdbcData { 3 }

            Test-MongoDBConnection -ConnectionString "mongodb://fake" -DatabaseName "db" -CollectionName "films" |
                Should -BeTrue
        }
    }

    It "reports failure instead of throwing when MongoDB does not answer" {
        InModuleScope NoSqlToSqlMigration {
            Mock Connect-Mdbc { throw "no route to host" }

            Test-MongoDBConnection -ConnectionString "mongodb://fake" -DatabaseName "db" -CollectionName "films" |
                Should -BeFalse
        }
    }

    It "works without a collection name" {
        InModuleScope NoSqlToSqlMigration {
            Mock Connect-Mdbc { }
            Mock Get-MdbcData { 0 }

            Test-MongoDBConnection -ConnectionString "mongodb://fake" -DatabaseName "db" | Should -BeTrue
        }
    }
}

Describe "Get-AppConfig" {

    It "reads a configuration file" {
        $path = Join-Path $TestDrive "config.json"
        @{ MongoDB = @{ ConnectionString = "mongodb://x"; Database = "db" } } | ConvertTo-Json | Set-Content $path

        $config = Get-AppConfig -Path $path

        $config.MongoDB.Database | Should -Be "db"
    }

    It "says clearly when the file is missing" {
        { Get-AppConfig -Path (Join-Path $TestDrive "nope.json") } | Should -Throw "*not found*"
    }
}

Describe "Invoke-N2SMigration" {

    It "reports a configuration problem as exit code 2 instead of throwing" {
        # A scheduled task should get a usable exit code, not a stack trace
        $result = Invoke-N2SMigration -Collections @("films") -ConfigPath (Join-Path $TestDrive "nope.json") -ErrorAction SilentlyContinue

        $result.ExitCode | Should -Be 2
    }

    It "never asks a question, even without a collection name" {
        InModuleScope NoSqlToSqlMigration {
            Mock Invoke-MigrationWorkflow { @{ TotalSuccess = 1; TotalFailed = 0; ExitCode = 0; Collections = @() } }
            Mock Read-Host { throw "a question was asked in an automated run" }

            $result = Invoke-N2SMigration -Operation ValidationOnly

            $result.ExitCode | Should -Be 0
            Should -Not -Invoke Read-Host
        }
    }

    It "passes -Force to the workflow so the confirmation is skipped" {
        InModuleScope NoSqlToSqlMigration {
            Mock Invoke-MigrationWorkflow { @{ TotalSuccess = 0; TotalFailed = 0; ExitCode = 0; Collections = @() } }

            Invoke-N2SMigration -Operation ValidationOnly | Out-Null

            Should -Invoke Invoke-MigrationWorkflow -ParameterFilter { $Force -eq $true } -Times 1
        }
    }
}

Describe "Write-N2SMessage" {

    It "writes a warning to the warning stream in Stream mode" {
        InModuleScope NoSqlToSqlMigration {
            Set-N2SOutputMode -Mode Stream

            $warning = Write-N2SMessage "let op" -Level Warning 3>&1

            $warning.Message | Should -Be "let op"
        }
    }

    It "writes an error to the error stream in Stream mode" {
        InModuleScope NoSqlToSqlMigration {
            Set-N2SOutputMode -Mode Stream

            $errorRecord = Write-N2SMessage "mislukt" -Level Error 2>&1

            $errorRecord.Exception.Message | Should -Be "mislukt"
        }
    }

    It "keeps detail out of the way unless it is asked for" {
        InModuleScope NoSqlToSqlMigration {
            Set-N2SOutputMode -Mode Stream
            $VerbosePreference = 'SilentlyContinue'

            $output = Write-N2SMessage "detail" -Level Detail 4>&1

            $output | Should -BeNullOrEmpty
        }
    }
}
