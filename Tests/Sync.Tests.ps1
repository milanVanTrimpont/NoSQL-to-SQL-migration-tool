<#
.SYNOPSIS
Tests for change detection and the child tables during a sync.

.DESCRIPTION
A sync has to notice two kinds of change: something changed in MongoDB, and
something changed in SQL. The first is caught by the document hash, the second
by comparing child row counts.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "Get-DocumentHash" {

    It "gives the same hash for the same document" {
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "abc"; title = "Heat"; ratings = @(8, 9) }

            Get-DocumentHash -Document $document | Should -Be (Get-DocumentHash -Document $document)
        }
    }

    It "notices a changed scalar field" {
        InModuleScope NoSqlToSqlMigration {
            $before = Get-DocumentHash -Document @{ _id = "abc"; title = "Heat" }
            $after = Get-DocumentHash -Document @{ _id = "abc"; title = "Heat 2" }

            $before | Should -Not -Be $after
        }
    }

    It "notices an extra element in an array" {
        # Regression: the hash only covered scalar fields, so a rating added in
        # MongoDB never marked the document as changed and never reached SQL.
        InModuleScope NoSqlToSqlMigration {
            $before = Get-DocumentHash -Document @{ _id = "abc"; ratings = @(8, 9) }
            $after = Get-DocumentHash -Document @{ _id = "abc"; ratings = @(8, 9, 10) }

            $before | Should -Not -Be $after
        }
    }

    It "notices a changed field inside a sub-document" {
        InModuleScope NoSqlToSqlMigration {
            $before = Get-DocumentHash -Document @{ _id = "abc"; address = @{ city = "Gent" } }
            $after = Get-DocumentHash -Document @{ _id = "abc"; address = @{ city = "Brugge" } }

            $before | Should -Not -Be $after
        }
    }

    It "does not depend on the order of the fields" {
        InModuleScope NoSqlToSqlMigration {
            $one = Get-DocumentHash -Document ([ordered]@{ a = 1; b = 2 })
            $two = Get-DocumentHash -Document ([ordered]@{ b = 2; a = 1 })

            $one | Should -Be $two
        }
    }

    It "does depend on the order of array elements" {
        InModuleScope NoSqlToSqlMigration {
            $one = Get-DocumentHash -Document @{ tags = @("a", "b") }
            $two = Get-DocumentHash -Document @{ tags = @("b", "a") }

            $one | Should -Not -Be $two
        }
    }
}

Describe "ConvertTo-HashableString" {

    It "writes a number the same way on every machine" {
        InModuleScope NoSqlToSqlMigration {
            ConvertTo-HashableString -Value 8.6 | Should -Be "8.6"
        }
    }

    It "writes a date in a fixed notation" {
        InModuleScope NoSqlToSqlMigration {
            ConvertTo-HashableString -Value ([datetime]"2020-01-02T10:00:00") | Should -Match '^2020-01-02T10:00:00'
        }
    }

    It "marks null" {
        InModuleScope NoSqlToSqlMigration {
            ConvertTo-HashableString -Value $null | Should -Be "null"
        }
    }
}

Describe "Get-ExpectedChildRowCount" {

    It "counts the elements of an array" {
        InModuleScope NoSqlToSqlMigration {
            Get-ExpectedChildRowCount -Document @{ genres = @("a", "b", "c") } -FieldName "genres" | Should -Be 3
        }
    }

    It "counts a sub-document as one row" {
        InModuleScope NoSqlToSqlMigration {
            Get-ExpectedChildRowCount -Document @{ address = @{ city = "Gent" } } -FieldName "address" | Should -Be 1
        }
    }

    It "counts a missing field as zero" {
        InModuleScope NoSqlToSqlMigration {
            Get-ExpectedChildRowCount -Document @{ title = "Heat" } -FieldName "genres" | Should -Be 0
        }
    }
}

Describe "Test-ChildRowDrift" {

    It "sees no drift when the counts match" {
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "abc"; genres = @("a", "b") }
            $childTables = @{ genres = "films_genres" }
            $counts = @{ genres = @{ abc = 2 } }

            Test-ChildRowDrift -Document $document -DocumentId "abc" -ChildTables $childTables -ChildRowCounts $counts |
                Should -BeFalse
        }
    }

    It "sees drift when rows were removed straight from SQL" {
        # This is the case that a hash can never catch: MongoDB did not change
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "abc"; genres = @("a", "b") }
            $childTables = @{ genres = "films_genres" }
            $counts = @{ genres = @{ abc = 0 } }

            Test-ChildRowDrift -Document $document -DocumentId "abc" -ChildTables $childTables -ChildRowCounts $counts |
                Should -BeTrue
        }
    }

    It "sees drift when the child table holds too many rows" {
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "abc"; genres = @("a") }
            $childTables = @{ genres = "films_genres" }
            $counts = @{ genres = @{ abc = 5 } }

            Test-ChildRowDrift -Document $document -DocumentId "abc" -ChildTables $childTables -ChildRowCounts $counts |
                Should -BeTrue
        }
    }

    It "sees no drift when there are no child tables" {
        InModuleScope NoSqlToSqlMigration {
            Test-ChildRowDrift -Document @{ _id = "abc" } -DocumentId "abc" -ChildTables @{} -ChildRowCounts @{} |
                Should -BeFalse
        }
    }
}

Describe "Get-ChildTableMap" {

    It "only accepts tables that carry the parent key column" {
        # A table whose name happens to start with the same prefix must be
        # left alone
        $connection = [PSCustomObject]@{}
        $connection | Add-Member -MemberType ScriptMethod -Name CreateCommand -Value {
            $command = [PSCustomObject]@{ CommandText = '' }
            $command | Add-Member -MemberType ScriptMethod -Name ExecuteReader -Value {
                # A stand-in for SHOW TABLES: it walks its own row list
                $reader = [PSCustomObject]@{
                    Rows  = @("films_genres", "films_archive")
                    Index = -1
                }
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
            return $command
        }

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-SQLTableColumns {
                if ($TableName -eq 'films_genres') {
                    return @{ films__id = 'varchar(24)'; array_index = 'int'; value = 'varchar(255)' }
                }
                return @{ id = 'int'; note = 'varchar(255)' }
            }

            $map = Get-ChildTableMap -Connection $Connection -TableName "films" -PrimaryKeyField "_id"

            $map.ContainsKey('genres') | Should -BeTrue
            $map.ContainsKey('archive') | Should -BeFalse
        }
    }
}

Describe "Get-CollectionResultStatus" {

    It "calls a clean run a success" {
        InModuleScope NoSqlToSqlMigration {
            $details = @{ Migration = @{ FailedDocuments = 0; TotalDocuments = 10; ConversionIssues = @() } }

            (Get-CollectionResultStatus -Details $details).Success | Should -BeTrue
        }
    }

    It "calls a run with failed documents a failure" {
        InModuleScope NoSqlToSqlMigration {
            $details = @{ Migration = @{ FailedDocuments = 3; TotalDocuments = 10; ConversionIssues = @() } }
            $status = Get-CollectionResultStatus -Details $details

            $status.Success | Should -BeFalse
            $status.Reason | Should -Match '3 of 10'
        }
    }

    It "calls a sync with errors a failure" {
        # Regression: a sync caught its own errors and returned normally, after
        # which the workflow reported "completed successfully"
        InModuleScope NoSqlToSqlMigration {
            $details = @{ Sync = @{ Errors = @("Failed to insert document abc") } }
            $status = Get-CollectionResultStatus -Details $details

            $status.Success | Should -BeFalse
            $status.Reason | Should -Match 'sync reported 1 error'
        }
    }

    It "calls a failed validation a failure" {
        InModuleScope NoSqlToSqlMigration {
            $details = @{ Validation = @{ OverallStatus = 'FAILED'; Issues = @("x"); RecordCountMatch = $true } }

            (Get-CollectionResultStatus -Details $details).Success | Should -BeFalse
        }
    }

    It "calls a count mismatch a failure, even when most samples pass" {
        InModuleScope NoSqlToSqlMigration {
            $details = @{ Validation = @{ OverallStatus = 'PARTIAL'; Issues = @("x"); RecordCountMatch = $false
                                          MongoCount = 10; SQLCount = 8 } }
            $status = Get-CollectionResultStatus -Details $details

            $status.Success | Should -BeFalse
            $status.Reason | Should -Match 'record counts do not match'
        }
    }

    It "treats a conversion problem as a warning, not a failure" {
        # Storing the value as NULL is what OnConversionError = Warn asks for
        InModuleScope NoSqlToSqlMigration {
            $details = @{ Migration = @{ FailedDocuments = 0; TotalDocuments = 10
                                         ConversionIssues = @([PSCustomObject]@{ Field = 'created' }) } }
            $status = Get-CollectionResultStatus -Details $details

            $status.Success | Should -BeTrue
            $status.Warning | Should -Match 'could not be converted'
        }
    }

    It "keeps both warnings when there are two" {
        InModuleScope NoSqlToSqlMigration {
            $details = @{
                Migration  = @{ FailedDocuments = 0; TotalDocuments = 10
                                ConversionIssues = @([PSCustomObject]@{ Field = 'created' }) }
                Validation = @{ OverallStatus = 'PARTIAL'; Issues = @("x"); RecordCountMatch = $true }
            }
            $status = Get-CollectionResultStatus -Details $details

            $status.Success | Should -BeTrue
            $status.Warning | Should -Match 'could not be converted'
            $status.Warning | Should -Match 'PARTIAL'
        }
    }
}
