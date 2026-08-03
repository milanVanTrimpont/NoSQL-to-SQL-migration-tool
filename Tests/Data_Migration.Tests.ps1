<#
.SYNOPSIS
Tests for the conversion layer and for migrating a single document.

.DESCRIPTION
The conversion layer is what keeps a difference in format from costing a
document, so most of these tests describe a value that does not fit its column
and what the tool does with it. No database is needed: the SQL connection is a
stand-in that records the statements it is given.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "ConvertTo-SQLDateTime" {

    It "keeps a value that is already a date" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLDateTime -Value ([datetime]"2020-01-02T10:00:00")
            $result.Year | Should -Be 2020
        }
    }

    It "reads an ISO date" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLDateTime -Value "2023-11-30"
            $result.ToString('yyyy-MM-dd') | Should -Be "2023-11-30"
        }
    }

    It "reads a day-first date" {
        # 06/05/2022 is 6 May here, not 5 June
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLDateTime -Value "06/05/2022"
            $result.ToString('yyyy-MM-dd') | Should -Be "2022-05-06"
        }
    }

    It "reads a date with a time" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLDateTime -Value "2020-01-02 14:30:00"
            $result.Hour | Should -Be 14
        }
    }

    It "returns nothing for a value that is not a date" {
        InModuleScope NoSqlToSqlMigration {
            ConvertTo-SQLDateTime -Value "onbekend" | Should -BeNullOrEmpty
        }
    }

    It "returns nothing for an empty value" {
        InModuleScope NoSqlToSqlMigration {
            ConvertTo-SQLDateTime -Value "" | Should -BeNullOrEmpty
        }
    }
}

Describe "ConvertTo-SQLColumnValue" {

    It "puts a text date into a datetime column as a real date" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value "06/05/2022" -ColumnType "datetime"

            $result.Success | Should -BeTrue
            $result.Value.ToString('yyyy-MM-dd') | Should -Be "2022-05-06"
        }
    }

    It "reports a value that is not a date instead of failing the document" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value "onbekend" -ColumnType "datetime"

            $result.Success | Should -BeFalse
            $result.Reason | Should -Match 'not a recognisable date'
        }
    }

    It "reads a decimal comma as a decimal point" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value "8,6" -ColumnType "decimal(18,2)"

            $result.Success | Should -BeTrue
            $result.Value | Should -Be 8.6
        }
    }

    It "reports text that cannot be a number" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value "veel" -ColumnType "int"

            $result.Success | Should -BeFalse
            $result.Reason | Should -Match 'not a whole number'
        }
    }

    It "reports a value that is longer than its column" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value ("x" * 300) -ColumnType "varchar(255)"

            $result.Success | Should -BeFalse
            $result.Reason | Should -Match 'does not fit'
        }
    }

    It "writes a date into a text column in ISO notation" {
        # Otherwise the same data looks different on a machine with other
        # regional settings, and it does not sort correctly
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value ([datetime]"2020-01-02T10:00:00") -ColumnType "varchar(255)"

            $result.Success | Should -BeTrue
            $result.Value | Should -Be "2020-01-02 10:00:00"
        }
    }

    It "turns null into a database null" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value $null -ColumnType "varchar(255)"

            $result.Success | Should -BeTrue
            $result.Value | Should -BeOfType [System.DBNull]
        }
    }

    It "stores a boolean as 1 in an integer column" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value $true -ColumnType "tinyint(1)"

            $result.Success | Should -BeTrue
            $result.Value | Should -Be 1
        }
    }
}

Describe "Get-ConversionErrorPolicy" {

    It "defaults to Warn" {
        InModuleScope NoSqlToSqlMigration {
            Get-ConversionErrorPolicy -Config ([PSCustomObject]@{ Migration = [PSCustomObject]@{} }) | Should -Be 'Warn'
        }
    }

    It "accepts the setting from the configuration" {
        InModuleScope NoSqlToSqlMigration {
            $config = [PSCustomObject]@{ Migration = [PSCustomObject]@{ OnConversionError = 'Skip' } }
            Get-ConversionErrorPolicy -Config $config | Should -Be 'Skip'
        }
    }

    It "ignores an unknown setting" {
        InModuleScope NoSqlToSqlMigration {
            $config = [PSCustomObject]@{ Migration = [PSCustomObject]@{ OnConversionError = 'Explode' } }
            Get-ConversionErrorPolicy -Config $config | Should -Be 'Warn'
        }
    }
}

Describe "Invoke-DocumentMigration" {

    BeforeAll {
        # A stand-in for a SQL connection that records every statement it is
        # given. Built here and handed to the module scope as a parameter,
        # because a function from the test file is not visible in that scope.
        function New-FakeConnection {
            $log = [System.Collections.ArrayList]::new()

            $connection = [PSCustomObject]@{ Log = $log }
            $connection | Add-Member -MemberType ScriptMethod -Name CreateCommand -Value {
                $command = [PSCustomObject]@{
                    CommandText = ''
                    Parameters  = [System.Collections.ArrayList]::new()
                    Log         = $this.Log
                }

                $command | Add-Member -MemberType ScriptMethod -Name CreateParameter -Value {
                    [PSCustomObject]@{ ParameterName = ''; Value = $null }
                }

                $command | Add-Member -MemberType ScriptMethod -Name ExecuteNonQuery -Value {
                    $this.Log.Add([PSCustomObject]@{
                        Sql    = $this.CommandText
                        Values = @($this.Parameters | ForEach-Object { $_.Value })
                    }) | Out-Null
                    return 1
                }

                return $command
            }

            return $connection
        }
    }

    It "writes the scalar fields to the main table" {
        $connection = New-FakeConnection

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-SQLTableColumns { @{ _id = 'varchar(24)'; title = 'varchar(255)'; year = 'varchar(255)' } }

            $document = @{ _id = "abc"; title = "Heat"; year = "1995" }

            Invoke-DocumentMigration -Document $document -Connection $Connection -TableName "films" `
                                     -Schema @{} -DatabaseType "MySQL" | Should -BeTrue
        }

        $insert = $connection.Log | Where-Object { $_.Sql -match 'REPLACE INTO' }
        $insert | Should -Not -BeNullOrEmpty
        $insert.Values | Should -Contain "Heat"
    }

    It "skips a field that has no column in the table" {
        # The schema comes from a sample, so a document can hold a field that
        # was never seen. That must not cost the whole document.
        $connection = New-FakeConnection

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-SQLTableColumns { @{ _id = 'varchar(24)'; title = 'varchar(255)' } }

            $document = @{ _id = "abc"; title = "Heat"; unexpected = "surprise" }

            Invoke-DocumentMigration -Document $document -Connection $Connection -TableName "films" `
                                     -Schema @{} -DatabaseType "MySQL" | Should -BeTrue
        }

        $insert = $connection.Log | Where-Object { $_.Sql -match 'REPLACE INTO' }
        $insert.Sql | Should -Not -Match 'unexpected'
    }

    It "stores an unconvertible value as NULL and keeps the document" {
        $connection = New-FakeConnection

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-SQLTableColumns { @{ _id = 'varchar(24)'; created = 'datetime' } }
            $script:N2SConversionPolicy = 'Warn'
            $script:N2SConversionIssues = @()

            $document = @{ _id = "abc"; created = "onbekend" }

            Invoke-DocumentMigration -Document $document -Connection $Connection -TableName "films" `
                                     -Schema @{} -DatabaseType "MySQL" | Should -BeTrue

            $script:N2SConversionIssues.Count | Should -Be 1
            $script:N2SConversionIssues[0].Action | Should -Be 'stored as NULL'
        }
    }

    It "skips the document when the policy says Skip" {
        $connection = New-FakeConnection

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-SQLTableColumns { @{ _id = 'varchar(24)'; created = 'datetime' } }
            $script:N2SConversionPolicy = 'Skip'
            $script:N2SConversionIssues = @()

            $document = @{ _id = "abc"; created = "onbekend" }

            Invoke-DocumentMigration -Document $document -Connection $Connection -TableName "films" `
                                     -Schema @{} -DatabaseType "MySQL" | Should -BeFalse

            $script:N2SConversionIssues[0].Action | Should -Be 'document skipped'
            $script:N2SConversionPolicy = 'Warn'
        }
    }

    It "sends arrays to their child table instead of the main table" {
        $connection = New-FakeConnection

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Mock Get-SQLTableColumns { @{ _id = 'varchar(24)'; title = 'varchar(255)' } }
            Mock Invoke-ChildTableMigration { return 2 }

            $document = @{ _id = "abc"; title = "Heat"; genres = @("Crime", "Drama") }
            $sqlSchema = @{ MainTable = "films"; Tables = @("films", "films_genres") }

            Invoke-DocumentMigration -Document $document -Connection $Connection -TableName "films" `
                                     -Schema @{} -DatabaseType "MySQL" -SQLSchema $sqlSchema | Should -BeTrue

            Should -Invoke Invoke-ChildTableMigration -Times 1 -Exactly
        }

        $insert = $connection.Log | Where-Object { $_.Sql -match 'REPLACE INTO' }
        $insert.Sql | Should -Not -Match 'genres'
    }
}

Describe "ConvertTo-ComparableValue" {

    It "compares numbers by value, not by notation" {
        # MySQL returns DECIMAL(18,2) as 8.30 where MongoDB holds 8.3
        InModuleScope NoSqlToSqlMigration {
            $mongo = ConvertTo-ComparableValue -Value 8.3 -DatabaseType "MySQL"
            $sql = ConvertTo-ComparableValue -Value ([decimal]8.30) -DatabaseType "MySQL"

            $mongo | Should -Be $sql
        }
    }

    It "turns a boolean into 1" {
        InModuleScope NoSqlToSqlMigration {
            ConvertTo-ComparableValue -Value $true -DatabaseType "MySQL" | Should -Be "1"
        }
    }

    It "turns null into an empty string" {
        InModuleScope NoSqlToSqlMigration {
            ConvertTo-ComparableValue -Value $null -DatabaseType "MySQL" | Should -Be ""
        }
    }
}

Describe "Compare-DocumentToRecord" {

    It "reports a match when the values are the same" {
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "abc"; title = "Heat" }
            $record = @{ _id = "abc"; title = "Heat" }

            (Compare-DocumentToRecord -MongoDocument $document -SQLRecord $record -DatabaseType "MySQL").Match | Should -BeTrue
        }
    }

    It "reports a field that is missing in SQL" {
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "abc"; title = "Heat" }
            $record = @{ _id = "abc" }

            $result = Compare-DocumentToRecord -MongoDocument $document -SQLRecord $record -DatabaseType "MySQL"

            $result.Match | Should -BeFalse
            $result.Differences -join ' ' | Should -Match 'title missing in SQL'
        }
    }

    It "accepts a text date that was stored as a real date" {
        # Otherwise every converted value looks like a difference
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "abc"; created = "06/05/2022" }
            $record = @{ _id = "abc"; created = [datetime]"2022-05-06" }

            (Compare-DocumentToRecord -MongoDocument $document -SQLRecord $record -DatabaseType "MySQL").Match | Should -BeTrue
        }
    }
}
