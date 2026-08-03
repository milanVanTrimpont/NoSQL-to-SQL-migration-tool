<#
.SYNOPSIS
Tests for the SQL schema generation and the translation to MySQL.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "Convert-MongoTypeToSQL" {

    It "maps a short string to VARCHAR(255)" {
        InModuleScope NoSqlToSqlMigration {
            $field = @{ Types = @{ string = 10 }; MaxLength = 40 }
            Convert-MongoTypeToSQL -FieldInfo $field -FieldName "title" | Should -Be "VARCHAR(255)"
        }
    }

    It "maps a long string to VARCHAR(MAX)" {
        # Regression: a storyline of more than 255 characters was rejected by the
        # database because the column was sized on the shortened sample values.
        InModuleScope NoSqlToSqlMigration {
            $field = @{ Types = @{ string = 10 }; MaxLength = 900 }
            Convert-MongoTypeToSQL -FieldInfo $field -FieldName "storyline" | Should -Be "VARCHAR(MAX)"
        }
    }

    It "keeps _id at VARCHAR(24)" {
        InModuleScope NoSqlToSqlMigration {
            $field = @{ Types = @{ ObjectId = 10 }; MaxLength = 24 }
            Convert-MongoTypeToSQL -FieldInfo $field -FieldName "_id" | Should -Be "VARCHAR(24)"
        }
    }

    It "maps a datetime-only field to DATETIME2" {
        InModuleScope NoSqlToSqlMigration {
            $field = @{ Types = @{ datetime = 10 }; MaxLength = 19 }
            Convert-MongoTypeToSQL -FieldInfo $field -FieldName "created" | Should -Be "DATETIME2"
        }
    }

    It "falls back to text when a field holds dates as well as text" {
        # Regression and the reason for the penalty: the column type used to be
        # the most common type, so a date written as text could never be stored
        # and that document was lost.
        InModuleScope NoSqlToSqlMigration {
            $field = @{ Types = @{ datetime = 8; string = 2 }; MaxLength = 19 }
            Convert-MongoTypeToSQL -FieldInfo $field -FieldName "created" | Should -Be "VARCHAR(255)"
        }
    }

    It "uses DECIMAL when a field holds integers as well as decimals" {
        InModuleScope NoSqlToSqlMigration {
            $field = @{ Types = @{ integer = 2; number = 120 }; MaxLength = 3 }
            Convert-MongoTypeToSQL -FieldInfo $field -FieldName "imdbRating" | Should -Be "DECIMAL(18,2)"
        }
    }

    It "ignores null when deciding the type" {
        InModuleScope NoSqlToSqlMigration {
            $field = @{ Types = @{ integer = 5; null = 3 }; MaxLength = 3 }
            Convert-MongoTypeToSQL -FieldInfo $field -FieldName "age" | Should -Be "INT"
        }
    }
}

Describe "New-TableDefinition" {

    It "puts the primary key first and marks only that column NOT NULL" {
        InModuleScope NoSqlToSqlMigration {
            $fields = @{
                _id   = @{ Types = @{ ObjectId = 2 }; MaxLength = 24; Count = 2 }
                title = @{ Types = @{ string = 1 }; MaxLength = 10; Count = 1 }
            }

            $sql = New-TableDefinition -TableName "films" -Fields $fields -PrimaryKeyField "_id" -Schema $fields -IncludeDrop $true

            $sql | Should -Match '\[_id\] VARCHAR\(24\) PRIMARY KEY NOT NULL'
            # A field seen in every sampled document can still be missing from a
            # document outside the sample, so it must stay nullable
            $sql | Should -Not -Match '\[title\][^\n]*NOT NULL'
        }
    }

    It "still produces a valid table for an empty collection" {
        # Regression: no fields produced CREATE TABLE x () and a syntax error,
        # after which the validation crashed on a table that did not exist.
        InModuleScope NoSqlToSqlMigration {
            $sql = New-TableDefinition -TableName "leeg" -Fields @{} -PrimaryKeyField "_id" -Schema @{} -IncludeDrop $true

            $sql | Should -Match '\[_id\] VARCHAR\(24\) PRIMARY KEY NOT NULL'
            $sql | Should -Not -Match 'CREATE TABLE \[leeg\] \(\s*\)'
        }
    }
}

Describe "New-SQLSchema" {

    It "creates a child table for an array field" {
        # Regression: arrays were dropped silently, so actors, genres and
        # ratings never reached the database at all.
        InModuleScope NoSqlToSqlMigration {
            $schema = @{
                _id    = @{ Types = @{ ObjectId = 1 }; Count = 1; IsArray = $false; IsNested = $false; MaxLength = 24 }
                title  = @{ Types = @{ string = 1 }; Count = 1; IsArray = $false; IsNested = $false; MaxLength = 10 }
                genres = @{ Types = @{ array = 1 }; Count = 1; IsArray = $true; IsNested = $false
                            ArrayElementTypes = @{ string = 3 }; MaxElementLength = 8; MaxLength = 0 }
            }

            $result = New-SQLSchema -Schema $schema -TableName "films" -PrimaryKeyField "_id"

            $result.Tables | Should -Contain "films"
            $result.Tables | Should -Contain "films_genres"
            $result.Relationships.Count | Should -Be 1
        }
    }

    It "creates a child table with the fields of objects inside an array" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{
                _id                  = @{ Types = @{ ObjectId = 1 }; Count = 1; IsArray = $false; IsNested = $false; MaxLength = 24 }
                reviews              = @{ Types = @{ array = 1 }; Count = 1; IsArray = $true; IsNested = $true
                                          ArrayElementTypes = @{ object = 3 }; MaxElementLength = 0; MaxLength = 0 }
                'reviews[].reviewer' = @{ Types = @{ string = 3 }; Count = 3; IsArray = $false; IsNested = $false; MaxLength = 20 }
                'reviews[].rating'   = @{ Types = @{ integer = 3 }; Count = 3; IsArray = $false; IsNested = $false; MaxLength = 2 }
            }

            $result = New-SQLSchema -Schema $schema -TableName "films" -PrimaryKeyField "_id"
            $childSql = $result.Statements | Where-Object { $_ -match 'films_reviews' }

            $result.Tables | Should -Contain "films_reviews"
            $childSql | Should -Match '\[reviewer\]'
            $childSql | Should -Match '\[rating\]'
        }
    }

    It "gives an array of integers an INT value column" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{
                _id     = @{ Types = @{ ObjectId = 1 }; Count = 1; IsArray = $false; IsNested = $false; MaxLength = 24 }
                ratings = @{ Types = @{ array = 1 }; Count = 1; IsArray = $true; IsNested = $false
                             ArrayElementTypes = @{ integer = 30 }; MaxElementLength = 2; MaxLength = 0 }
            }

            $result = New-SQLSchema -Schema $schema -TableName "films" -PrimaryKeyField "_id"
            $childSql = $result.Statements | Where-Object { $_ -match 'films_ratings' }

            $childSql | Should -Match '\[value\] INT'
        }
    }

    It "falls back to text for an array with mixed element types" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{
                _id  = @{ Types = @{ ObjectId = 1 }; Count = 1; IsArray = $false; IsNested = $false; MaxLength = 24 }
                tags = @{ Types = @{ array = 1 }; Count = 1; IsArray = $true; IsNested = $false
                          ArrayElementTypes = @{ integer = 2; string = 3 }; MaxElementLength = 10; MaxLength = 0 }
            }

            $result = New-SQLSchema -Schema $schema -TableName "films" -PrimaryKeyField "_id"
            $childSql = $result.Statements | Where-Object { $_ -match 'films_tags' }

            $childSql | Should -Match '\[value\] VARCHAR\(MAX\)'
        }
    }
}

Describe "Convert-ToMySQLSyntax" {

    It "turns the T-SQL existence check into DROP TABLE IF EXISTS" {
        # Regression: the drop disappeared entirely, so a second run kept the old
        # table and never picked up a corrected column type.
        InModuleScope NoSqlToSqlMigration {
            $tsql = "IF OBJECT_ID('films', 'U') IS NOT NULL DROP TABLE [films];`n`nCREATE TABLE [films] (`n    [_id] VARCHAR(24) PRIMARY KEY NOT NULL`n);"
            $mysql = Convert-ToMySQLSyntax -SQLStatement $tsql

            $mysql | Should -Match 'DROP TABLE IF EXISTS `films`'
            $mysql | Should -Not -Match 'OBJECT_ID'
            $mysql | Should -Not -Match '\$tableName'
        }
    }

    It "translates VARCHAR(MAX), which MySQL does not have, to LONGTEXT" {
        InModuleScope NoSqlToSqlMigration {
            $mysql = Convert-ToMySQLSyntax -SQLStatement "CREATE TABLE [films] ([storyline] VARCHAR(MAX));"

            $mysql | Should -Match 'LONGTEXT'
            $mysql | Should -Not -Match 'VARCHAR\(MAX\)'
        }
    }

    It "replaces brackets with backticks and IDENTITY with AUTO_INCREMENT" {
        InModuleScope NoSqlToSqlMigration {
            $mysql = Convert-ToMySQLSyntax -SQLStatement "CREATE TABLE [films] ([id] INT IDENTITY(1,1) PRIMARY KEY);"

            $mysql | Should -Match '`films`'
            $mysql | Should -Match 'AUTO_INCREMENT'
            $mysql | Should -Not -Match '\['
        }
    }

    It "translates BIT and DATETIME2" {
        InModuleScope NoSqlToSqlMigration {
            $mysql = Convert-ToMySQLSyntax -SQLStatement "CREATE TABLE [t] ([a] BIT, [b] DATETIME2);"

            $mysql | Should -Match 'TINYINT\(1\)'
            $mysql | Should -Match 'DATETIME'
            $mysql | Should -Not -Match 'DATETIME2'
        }
    }
}

Describe "Split-SQLStatement" {

    It "splits a drop and a create into separate statements" {
        InModuleScope NoSqlToSqlMigration {
            $statements = @(Split-SQLStatement -SQLText "DROP TABLE IF EXISTS ``films``;`n`nCREATE TABLE ``films`` (`n  ``_id`` VARCHAR(24)`n);`n")

            $statements.Count | Should -Be 2
            $statements[0] | Should -Match 'DROP TABLE'
            $statements[1] | Should -Match 'CREATE TABLE'
        }
    }

    It "drops fragments that only contain comments or whitespace" {
        InModuleScope NoSqlToSqlMigration {
            $statements = @(Split-SQLStatement -SQLText "-- a comment`n`n")

            $statements.Count | Should -Be 0
        }
    }
}
