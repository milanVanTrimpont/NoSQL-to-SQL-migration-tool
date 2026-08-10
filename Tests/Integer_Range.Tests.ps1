<#
.SYNOPSIS
Tests for the size of a whole number column.

.DESCRIPTION
INT stops at 2147483647, so a timestamp in milliseconds does not fit and MySQL
refuses the row with "Out of range value for column 'timestamp_ms'". One field
then costs the whole table its data, which is why the column is sized on the
values the analysis saw.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "Add-DocumentToSchema and the range of a number" {

    It "keeps the largest whole number it saw" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            Add-DocumentToSchema -Document @{ timestamp_ms = [int64]1767184524000 } -Schema $schema -TotalDocs 1

            $schema['timestamp_ms'].MaxInteger | Should -Be 1767184524000
        }
    }

    It "keeps the smallest one as well" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            Add-DocumentToSchema -Document @{ offset = -5000000000 } -Schema $schema -TotalDocs 1

            $schema['offset'].MinInteger | Should -Be -5000000000
        }
    }

    It "tracks the range of the values in an array too" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            Add-DocumentToSchema -Document @{ stamps = @([int64]1767184524000, [int64]1767184525000) } `
                                 -Schema $schema -TotalDocs 1

            $schema['stamps'].MaxInteger | Should -Be 1767184525000
        }
    }

    It "leaves a normal number where it is" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            Add-DocumentToSchema -Document @{ year = 1995 } -Schema $schema -TotalDocs 1

            $schema['year'].MaxInteger | Should -Be 1995
        }
    }
}

Describe "Get-SQLIntegerType" {

    It "gives INT for a number that fits" {
        InModuleScope NoSqlToSqlMigration {
            Get-SQLIntegerType -FieldInfo @{ MaxInteger = 1995; MinInteger = 0 } | Should -Be "INT"
        }
    }

    It "gives BIGINT for a timestamp in milliseconds" {
        InModuleScope NoSqlToSqlMigration {
            Get-SQLIntegerType -FieldInfo @{ MaxInteger = 1767184524000; MinInteger = 0 } | Should -Be "BIGINT"
        }
    }

    It "gives BIGINT for a number that is too negative" {
        InModuleScope NoSqlToSqlMigration {
            Get-SQLIntegerType -FieldInfo @{ MaxInteger = 0; MinInteger = -5000000000 } | Should -Be "BIGINT"
        }
    }

    It "gives INT right up to the edge" {
        InModuleScope NoSqlToSqlMigration {
            Get-SQLIntegerType -FieldInfo @{ MaxInteger = 2147483647; MinInteger = 0 } | Should -Be "INT"
            Get-SQLIntegerType -FieldInfo @{ MaxInteger = 2147483648; MinInteger = 0 } | Should -Be "BIGINT"
        }
    }

    It "falls back to INT when nothing was tracked" {
        InModuleScope NoSqlToSqlMigration {
            Get-SQLIntegerType -FieldInfo @{} | Should -Be "INT"
            Get-SQLIntegerType -FieldInfo $null | Should -Be "INT"
        }
    }
}

Describe "The column that comes out of it" {

    It "gives a millisecond timestamp a BIGINT column" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $document = @{ _id = "1"; timestamp_ms = [int64]1767184524000; year = 2026 }
            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $create = (New-SQLSchema -Schema $schema -TableName "events").Statements -join "`n"

            $create | Should -Match '\[timestamp_ms\] BIGINT'
            $create | Should -Match '\[year\] INT'
        }
    }

    It "gives an array of millisecond timestamps a BIGINT value column" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $document = @{ _id = "1"; stamps = @([int64]1767184524000) }
            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $create = (New-SQLSchema -Schema $schema -TableName "events").Statements -join "`n"

            $create | Should -Match '\[value\] BIGINT'
        }
    }

    It "survives the conversion to MySQL" {
        InModuleScope NoSqlToSqlMigration {
            $mysql = Convert-ToMySQLSyntax -SQLStatement "CREATE TABLE [events] (`n    [timestamp_ms] BIGINT`n);"

            $mysql | Should -Match 'BIGINT'
            $mysql | Should -Not -Match 'TINYINT'
        }
    }

    It "counts a BIGINT as eight bytes of the row" {
        InModuleScope NoSqlToSqlMigration {
            Get-SQLColumnRowBytes -ColumnDefinition "    [timestamp_ms] BIGINT" | Should -Be 8
        }
    }
}

Describe "Get-SQLDataType for a column added during a sync" {

    It "gives BIGINT for a value that does not fit INT" {
        InModuleScope NoSqlToSqlMigration {
            Get-SQLDataType -Value ([int64]1767184524000) -DatabaseType "MySQL" | Should -Be "BIGINT"
        }
    }

    It "still gives INT for a normal number" {
        InModuleScope NoSqlToSqlMigration {
            Get-SQLDataType -Value 1995 -DatabaseType "MySQL" | Should -Be "INT"
        }
    }
}

Describe "ConvertTo-SQLColumnValue with a big number" {

    It "accepts a long for a bigint column" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value ([int64]1767184524000) -ColumnType "bigint"

            $result.Success | Should -BeTrue
            $result.Value | Should -Be 1767184524000
        }
    }

    It "reads a big number written as text" {
        InModuleScope NoSqlToSqlMigration {
            $result = ConvertTo-SQLColumnValue -Value "1767184524000" -ColumnType "bigint"

            $result.Success | Should -BeTrue
            $result.Value | Should -Be 1767184524000
        }
    }
}
