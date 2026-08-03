<#
.SYNOPSIS
Tests for tables that are left behind when a collection disappears.

.DESCRIPTION
Removing a collection from MongoDB leaves its SQL table behind. These tests
check that such a table is found and reported, and that dropping it really
needs a confirmation: data that no longer exists in MongoDB cannot come back.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }

    # A connection that answers SHOW TABLES from a fixed list and reports one
    # row for every table
    function New-TableListConnection {
        param([string[]]$Tables)

        $connection = [PSCustomObject]@{ Tables = $Tables; Dropped = [System.Collections.ArrayList]::new() }

        $connection | Add-Member -MemberType ScriptMethod -Name CreateCommand -Value {
            $command = [PSCustomObject]@{
                CommandText = ''
                Tables      = $this.Tables
                Dropped     = $this.Dropped
            }

            $command | Add-Member -MemberType ScriptMethod -Name ExecuteReader -Value {
                $reader = [PSCustomObject]@{ Rows = $this.Tables; Index = -1 }
                $reader | Add-Member -MemberType ScriptMethod -Name Read -Value {
                    $this.Index++
                    return ($this.Index -lt $this.Rows.Count)
                }
                $reader | Add-Member -MemberType ScriptMethod -Name GetString -Value {
                    param($i)
                    return $this.Rows[$this.Index]
                }
                $reader | Add-Member -MemberType ScriptMethod -Name Close -Value { }
                return $reader
            }

            $command | Add-Member -MemberType ScriptMethod -Name ExecuteScalar -Value { return 1 }

            $command | Add-Member -MemberType ScriptMethod -Name ExecuteNonQuery -Value {
                $this.Dropped.Add($this.CommandText) | Out-Null
                return 1
            }

            return $command
        }

        return $connection
    }
}

Describe "Get-OrphanSQLTable" {

    It "does not report a table that matches a collection" {
        $connection = New-TableListConnection -Tables @("films")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            $orphans = @(Get-OrphanSQLTable -Connection $Connection -Collections @("films"))
            $orphans.Count | Should -Be 0
        }
    }

    It "does not report a child table whose parent collection still exists" {
        $connection = New-TableListConnection -Tables @("films", "films_genres", "films_ratings")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            $orphans = @(Get-OrphanSQLTable -Connection $Connection -Collections @("films"))
            $orphans.Count | Should -Be 0
        }
    }

    It "reports a table whose collection is gone, with its row count" {
        $connection = New-TableListConnection -Tables @("films", "users")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            $orphans = @(Get-OrphanSQLTable -Connection $Connection -Collections @("films"))

            $orphans.Count | Should -Be 1
            $orphans[0].Table | Should -Be "users"
            $orphans[0].Rows | Should -Be 1
        }
    }

    It "reports the child tables of a collection that is gone as well" {
        $connection = New-TableListConnection -Tables @("films", "users", "users_roles")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            $orphans = @(Get-OrphanSQLTable -Connection $Connection -Collections @("films"))

            @($orphans | Select-Object -ExpandProperty Table) | Should -Contain "users"
            @($orphans | Select-Object -ExpandProperty Table) | Should -Contain "users_roles"
        }
    }
}

Describe "Get-DocumentChildFieldName" {

    It "names the fields that need a child table" {
        InModuleScope NoSqlToSqlMigration {
            $documents = @(
                @{ _id = "1"; title = "Heat"; genres = @("Crime"); address = @{ city = "Gent" } }
                @{ _id = "2"; title = "Alien"; ratings = @(8, 9) }
            )

            $fields = Get-DocumentChildFieldName -Documents $documents

            $fields.ContainsKey('genres') | Should -BeTrue
            $fields.ContainsKey('address') | Should -BeTrue
            $fields.ContainsKey('ratings') | Should -BeTrue
            # A scalar belongs in the main table
            $fields.ContainsKey('title') | Should -BeFalse
        }
    }

    It "returns nothing for documents without arrays or sub-documents" {
        InModuleScope NoSqlToSqlMigration {
            $fields = Get-DocumentChildFieldName -Documents @(@{ _id = "1"; title = "Heat" })

            $fields.Count | Should -Be 0
        }
    }
}

Describe "Get-GhostChildTable" {

    It "reports a child table whose field is gone from every document" {
        # The collection still exists, so the orphan check cannot see this one
        $connection = New-TableListConnection -Tables @("films", "films_genres", "films_tags")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-ChildTableMap { @{ genres = "films_genres"; tags = "films_tags" } }

            $documents = @(@{ _id = "1"; genres = @("Crime") })
            $ghosts = @(Get-GhostChildTable -Connection $Connection -TableName "films" -Documents $documents)

            $ghosts.Count | Should -Be 1
            $ghosts[0].Table | Should -Be "films_tags"
            $ghosts[0].Field | Should -Be "tags"
        }
    }

    It "reports nothing while the field is still in use" {
        $connection = New-TableListConnection -Tables @("films", "films_genres")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-ChildTableMap { @{ genres = "films_genres" } }

            $documents = @(@{ _id = "1"; genres = @("Crime") })

            @(Get-GhostChildTable -Connection $Connection -TableName "films" -Documents $documents).Count |
                Should -Be 0
        }
    }

    It "reports nothing when the collection has no child tables" {
        $connection = New-TableListConnection -Tables @("films")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-ChildTableMap { @{} }

            @(Get-GhostChildTable -Connection $Connection -TableName "films" -Documents @(@{ _id = "1" })).Count |
                Should -Be 0
        }
    }
}

Describe "Sort-OrphanTableForDrop" {

    It "puts a child table before its parent, so a foreign key cannot block the drop" {
        InModuleScope NoSqlToSqlMigration {
            $orphans = @(
                [PSCustomObject]@{ Table = "users"; Rows = 1 }
                [PSCustomObject]@{ Table = "users_roles"; Rows = 3 }
            )

            $sorted = @(Sort-OrphanTableForDrop -Orphans $orphans)

            $sorted[0].Table | Should -Be "users_roles"
            $sorted[1].Table | Should -Be "users"
        }
    }
}

Describe "Remove-OrphanSQLTable" {

    It "drops the table when the caller confirms" {
        $connection = New-TableListConnection -Tables @("users")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Remove-OrphanSQLTable -Connection $Connection -TableName "users" -RowCount 1 -Confirm:$false |
                Should -BeTrue
        }

        $connection.Dropped -join ' ' | Should -Match 'DROP TABLE IF EXISTS `users`'
    }

    It "drops nothing in WhatIf mode" {
        $connection = New-TableListConnection -Tables @("users")

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Remove-OrphanSQLTable -Connection $Connection -TableName "users" -RowCount 1 -WhatIf |
                Should -BeFalse
        }

        $connection.Dropped.Count | Should -Be 0
    }

    It "names the table and the row count in what it asks" {
        # The question has to say what is about to be lost
        InModuleScope NoSqlToSqlMigration {
            $command = Get-Command Remove-OrphanSQLTable
            $command.Parameters.ContainsKey('WhatIf') | Should -BeTrue
            $command.Parameters.ContainsKey('Confirm') | Should -BeTrue
        }
    }
}
