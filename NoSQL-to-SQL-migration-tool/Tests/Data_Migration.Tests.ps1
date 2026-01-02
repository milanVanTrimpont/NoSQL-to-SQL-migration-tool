<#
.SYNOPSIS
    Unit tests for Migration_Validation.ps1 functions.
.DESCRIPTION
    This script contains Pester tests for the functions defined in Migration_Validation.ps1.
#>
BeforeAll {
    # Load the migration script
    $scriptPath = Join-Path $PSScriptRoot "..\private\Data_Migration.ps1"

    if (-not (Test-Path $scriptPath)) {
        throw "Test setup error: Data_Migration.ps1 not found at $scriptPath"
    }

    . $scriptPath
}


Describe "Test-MigrationValidation" {

    BeforeEach {
        # standard mocks
        Mock Get-AppConfig {
            @{
                MongoDB = @{
                    ConnectionString = "mongodb://fake"
                    Database = "testdb"
                }
            }
        }

        Mock Connect-Mdbc {}
        Mock Get-MdbcData {
            param([switch]$Count, [int]$Last)

            if ($Count) {
                return 5
            }

            # Sample Mongo docs
            return @(
                @{ _id = "1"; name = "Jan"; age = 30 }
                @{ _id = "2"; name = "Piet"; age = 40 }
            )
        }

        Mock Get-SQLConnectionObject {
            $conn = New-Object PSObject -Property @{
                State = "Closed"
            }

            $conn | Add-Member -MemberType ScriptMethod -Name Open -Value { $this.State = "Open" }
            $conn | Add-Member -MemberType ScriptMethod -Name Close -Value { $this.State = "Closed" }

            $conn | Add-Member -MemberType ScriptMethod -Name CreateCommand -Value {
                $cmd = New-Object PSObject
                $cmd | Add-Member -MemberType NoteProperty -Name CommandText -Value ""
                $cmd | Add-Member -MemberType ScriptMethod -Name ExecuteScalar -Value { 5 }
                return $cmd
            }

            return $conn
        }

        Mock Get-SQLRecord {
            param($Connection, $TableName, $Id)

            return @{
                _id  = $Id
                name = if ($Id -eq "1") { "Jan" } else { "Piet" }
                age  = if ($Id -eq "1") { 30 } else { 40 }
            }
        }

        Mock Compare-DocumentToRecord {
            @{
                DocumentId     = "1"
                Match          = $true
                Differences    = @()
                FieldsCompared = 3
            }
        }

        Mock Test-DataIntegrity {
            return @()
        }

        Mock Write-Host {}
        Mock Write-Progress {}
    }

    It "Geeft PASSED terug als alles klopt" {
        $result = Test-MigrationValidation -TableName "klanten" -SampleSize 2

        $result.OverallStatus | Should -Be "PASSED"
        $result.RecordCountMatch | Should -BeTrue
        $result.SamplesFailed | Should -Be 0
        $result.Issues.Count | Should -Be 0
    }

    It "Detecteert mismatch in record count" {
        Mock Get-MdbcData {
            param([switch]$Count)
            if ($Count) { return 10 }
        }

        $result = Test-MigrationValidation -TableName "klanten"

        $result.RecordCountMatch | Should -BeFalse
        $result.Issues | Should -ContainMatch "Record count mismatch"
    }

    It "Zet status op FAILED als samples falen" {
        Mock Compare-DocumentToRecord {
            @{
                DocumentId     = "1"
                Match          = $false
                Differences    = @("age mismatch")
                FieldsCompared = 3
            }
        }

        $result = Test-MigrationValidation -TableName "klanten" -SampleSize 1

        $result.SamplesFailed | Should -BeGreaterThan 0
        $result.OverallStatus | Should -Be "FAILED"
    }

    It "Zet status op ERROR bij exception" {

        Mock Get-SQLConnectionObject {
            $conn = New-Object PSObject
            $conn | Add-Member ScriptMethod Open { throw "SQL open failed" }
            $conn | Add-Member ScriptMethod Close {}
            $conn | Add-Member NoteProperty State "Closed"
            return $conn
        }

        $result = Test-MigrationValidation -TableName "klanten"

        $result.OverallStatus | Should -Be "ERROR"
        $result.Issues[0] | Should -Match "Validation error"
    }

}

Describe "Normalize-ValueForComparison" {

    It "Converteert boolean true naar 1" {
        Normalize-ValueForComparison -Value $true -DatabaseType "MySQL" | Should -Be "1"
    }

    It "Converteert DateTime correct" {
        $dt = Get-Date "2024-01-01 12:30:00"
        Normalize-ValueForComparison -Value $dt -DatabaseType "MySQL" |
            Should -Be "2024-01-01 12:30:00"
    }

    It "Geeft lege string bij null" {
        Normalize-ValueForComparison -Value $null -DatabaseType "MySQL" | Should -Be ""
    }
}

Describe "Compare-DocumentToRecord" {

    It "Geeft Match=true bij gelijke velden" {
        $mongo = @{ _id = "1"; name = "Jan"; age = 30 }
        $sql   = @{ _id = "1"; name = "Jan"; age = 30 }

        $result = Compare-DocumentToRecord -MongoDocument $mongo -SQLRecord $sql -DatabaseType "MySQL"

        $result.Match | Should -BeTrue
        $result.Differences.Count | Should -Be 0
    }

    It "Detecteert ontbrekend veld in SQL" {
        $mongo = @{ _id = "1"; name = "Jan"; age = 30 }
        $sql   = @{ _id = "1"; name = "Jan" }

        $result = Compare-DocumentToRecord -MongoDocument $mongo -SQLRecord $sql -DatabaseType "MySQL"

        $result.Match | Should -BeFalse
        $result.Differences | Should -Contain "age missing in SQL"
    }
}
