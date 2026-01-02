<#
<#
.SYNOPSIS
    Unit tests for Migration_Validation.ps1 functions.
.DESCRIPTION
    This script contains Pester tests for the functions defined in Migration_Validation.ps1.
#>
BeforeAll {
    . "$PSScriptRoot\..\private\Migration_Validation.ps1"
}

Describe "Start-IncrementalSync" {

    BeforeEach {

        # standard mocks
        Mock Write-Host {}
        Mock Connect-Mdbc {}

        Mock Get-AppConfig {
            @{
                MongoDB = @{
                    ConnectionString = "mongodb://fake"
                    Database = "testdb"
                }
            }
        }

        # Fake SQL connection
        Mock Get-SQLConnectionObject {
            $conn = New-Object PSObject -Property @{ State = "Closed" }
            $conn | Add-Member ScriptMethod Open { $this.State = "Open" }
            $conn | Add-Member ScriptMethod Close { $this.State = "Closed" }
            return $conn
        }

        Mock Update-SQLSchema { $false }

        Mock Save-SyncState {}
    }

    Context "Full sync (geen sync state)" {

        BeforeEach {
            Mock Get-SyncState { $null }

            Mock Get-MdbcData {
                @(
                    @{ _id = "1"; name = "Jan" }
                    @{ _id = "2"; name = "Piet" }
                )
            }

            Mock Get-AllSQLRecords {
                @{ }
            }

            Mock Get-DocumentHash { "HASH" }

            Mock Invoke-InsertDocument { $true }
            Mock Invoke-UpdateDocument { $true }
            Mock Invoke-DeleteDocument { $true }
        }

        It "Voert FULL sync uit en insert nieuwe records" {
            $result = Start-IncrementalSync -TableName "klanten"

            $result.IsFullSync | Should -BeTrue
            $result.NewRecords | Should -Be 2
            $result.UpdatedRecords | Should -Be 0
            $result.DeletedRecords | Should -Be 0
            $result.Errors.Count | Should -Be 0
        }
    }

    Context "Incremental sync met wijzigingen" {

        BeforeEach {
            Mock Get-SyncState {
                @{
                    LastSyncTime = (Get-Date).AddHours(-1)
                    DocumentHashes = @{
                        "1" = "OLDHASH"
                        "2" = "SAMEHASH"
                        "3" = "TODELETE"
                    }
                }
            }

            Mock Get-MdbcData {
                @(
                    @{ _id = "1"; name = "Jan gewijzigd" } # updated
                    @{ _id = "2"; name = "Piet" }           # unchanged
                    @{ _id = "4"; name = "Klaas" }          # new
                )
            }

            Mock Get-AllSQLRecords {
                @{
                    "1" = $true
                    "2" = $true
                    "3" = $true
                }
            }

            Mock Get-DocumentHash {
                param($Document)
                switch ($Document._id) {
                    "1" { "NEWHASH" }
                    "2" { "SAMEHASH" }
                    "4" { "HASH4" }
                }
            }

            Mock Invoke-InsertDocument { $true }
            Mock Invoke-UpdateDocument { $true }
            Mock Invoke-DeleteDocument { $true }
        }

        It "Detecteert new, updated, deleted en unchanged records correct" {
            $result = Start-IncrementalSync -TableName "klanten"

            $result.IsFullSync | Should -BeFalse
            $result.NewRecords | Should -Be 1
            $result.UpdatedRecords | Should -Be 1
            $result.DeletedRecords | Should -Be 1
            $result.UnchangedRecords | Should -Be 1
            $result.TotalProcessed | Should -Be 3
            $result.Errors.Count | Should -Be 0
        }
    }

    Context "Foutafhandeling" {

        BeforeEach {
            Mock Get-SyncState { $null }
            Mock Get-MdbcData { throw "Mongo failure" }
        }

        It "Vangt exceptions en vult Errors" {
            $result = Start-IncrementalSync -TableName "klanten"

            $result.Errors.Count | Should -Be 1
            $result.Errors[0] | Should -Match "Sync error"
        }
    }
}
