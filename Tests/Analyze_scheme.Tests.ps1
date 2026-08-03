<#
.SYNOPSIS
Tests for the schema analysis: type detection and document structure.

.DESCRIPTION
These tests run against the module that is actually loaded at runtime, not
against a copy of the source, and they need no database.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        # Keep the test output readable
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "Get-FieldType" {

    It "detects a string" {
        InModuleScope NoSqlToSqlMigration { Get-FieldType -Value "test" | Should -Be "string" }
    }

    It "detects an integer" {
        InModuleScope NoSqlToSqlMigration { Get-FieldType -Value 5 | Should -Be "integer" }
    }

    It "detects a decimal number" {
        InModuleScope NoSqlToSqlMigration { Get-FieldType -Value 8.6 | Should -Be "number" }
    }

    It "detects a boolean" {
        InModuleScope NoSqlToSqlMigration { Get-FieldType -Value $true | Should -Be "boolean" }
    }

    It "detects a datetime" {
        InModuleScope NoSqlToSqlMigration { Get-FieldType -Value ([datetime]"2020-01-02") | Should -Be "datetime" }
    }

    It "detects null" {
        InModuleScope NoSqlToSqlMigration { Get-FieldType -Value $null | Should -Be "null" }
    }

    It "detects an array" {
        InModuleScope NoSqlToSqlMigration { Get-FieldType -Value @(1, 2, 3) | Should -Be "array" }
    }

    It "detects an object built as PSCustomObject" {
        InModuleScope NoSqlToSqlMigration {
            Get-FieldType -Value ([PSCustomObject]@{ a = 1 }) | Should -Be "object"
        }
    }

    It "detects a dictionary as an object, not as an array" {
        # Regression: a MongoDB sub-document arrives as IDictionary, which is also
        # IEnumerable. Testing IEnumerable first made every sub-document look like
        # an array, so its fields never reached a table.
        InModuleScope NoSqlToSqlMigration {
            Get-FieldType -Value @{ city = "Gent" } | Should -Be "object"
        }
    }
}

Describe "Test-IsDocumentObject" {

    It "recognises a hashtable" {
        InModuleScope NoSqlToSqlMigration { Test-IsDocumentObject -Value @{ a = 1 } | Should -BeTrue }
    }

    It "does not treat an array as a document" {
        InModuleScope NoSqlToSqlMigration { Test-IsDocumentObject -Value @(1, 2) | Should -BeFalse }
    }

    It "does not treat a string as a document" {
        InModuleScope NoSqlToSqlMigration { Test-IsDocumentObject -Value "text" | Should -BeFalse }
    }

    It "handles null" {
        InModuleScope NoSqlToSqlMigration { Test-IsDocumentObject -Value $null | Should -BeFalse }
    }
}

Describe "Analyze-DocumentStructure" {

    It "records a scalar field with its type" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            Analyze-DocumentStructure -Document @{ title = "Heat" } -Schema $schema -TotalDocs 1

            $schema.ContainsKey('title') | Should -BeTrue
            $schema['title'].Types['string'] | Should -Be 1
            $schema['title'].IsArray | Should -BeFalse
        }
    }

    It "tracks the real length of a value, not the shortened sample" {
        # Regression: samples are cut to 50 characters for display. Sizing a
        # column on them made every string fit VARCHAR(255), so long text such
        # as a storyline was rejected on insert.
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $long = "x" * 400
            Analyze-DocumentStructure -Document @{ storyline = $long } -Schema $schema -TotalDocs 1

            $schema['storyline'].MaxLength | Should -Be 400
        }
    }

    It "marks an array field and counts its element types" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            Analyze-DocumentStructure -Document @{ genres = @("Drama", "Crime") } -Schema $schema -TotalDocs 1

            $schema['genres'].IsArray | Should -BeTrue
            $schema['genres'].ArrayElementTypes['string'] | Should -Be 2
        }
    }

    It "marks a sub-document as nested and analyses its fields" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            Analyze-DocumentStructure -Document @{ address = @{ city = "Gent" } } -Schema $schema -TotalDocs 1

            $schema['address'].IsNested | Should -BeTrue
            $schema['address'].IsArray | Should -BeFalse
            $schema.ContainsKey('address.city') | Should -BeTrue
        }
    }

    It "records fields of objects inside an array under a [] path" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $document = @{ reviews = @(@{ reviewer = "Ann"; rating = 9 }) }
            Analyze-DocumentStructure -Document $document -Schema $schema -TotalDocs 1

            $schema['reviews'].IsArray | Should -BeTrue
            $schema.ContainsKey('reviews[].reviewer') | Should -BeTrue
        }
    }

    It "counts both types when a field is a date in one document and text in another" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            Analyze-DocumentStructure -Document @{ created = [datetime]"2020-01-02" } -Schema $schema -TotalDocs 2
            Analyze-DocumentStructure -Document @{ created = "06/05/2022" } -Schema $schema -TotalDocs 2

            $schema['created'].Types['datetime'] | Should -Be 1
            $schema['created'].Types['string'] | Should -Be 1
        }
    }
}

Describe "Get-MongoDBSchema" {

    It "analyses the documents it receives from MongoDB" {
        InModuleScope NoSqlToSqlMigration {
            Mock Connect-Mdbc { }
            Mock Get-MdbcData {
                if ($Count) { return 2 }

                return @(
                    @{ _id = "1"; title = "Heat"; genres = @("Crime") },
                    @{ _id = "2"; title = "Alien"; genres = @("Horror", "Sci-Fi") }
                )
            }

            $schema = Get-MongoDBSchema -ConnectionString "mongodb://fake" -DatabaseName "db" -CollectionName "films"

            $schema.ContainsKey('title') | Should -BeTrue
            $schema['genres'].IsArray | Should -BeTrue
            $schema['genres'].ArrayElementTypes['string'] | Should -Be 3
        }
    }

    It "connects to MongoDB exactly once" {
        InModuleScope NoSqlToSqlMigration {
            Mock Connect-Mdbc { }
            Mock Get-MdbcData {
                if ($Count) { return 1 }
                return @(@{ _id = "1"; title = "Heat" })
            }

            Get-MongoDBSchema -ConnectionString "mongodb://fake" -DatabaseName "db" -CollectionName "films" | Out-Null

            Should -Invoke Connect-Mdbc -Times 1 -Exactly
        }
    }
}
