<#
.SYNOPSIS
    Unit tests for Analyze_scheme.ps1 functions.
.DESCRIPTION
    This script contains Pester tests for the functions defined in Analyze_scheme.ps1.
#>

BeforeAll {
    # Load the Analyze_scheme.ps1 script
    . "$PSScriptRoot/../private/Analyze_scheme.ps1"
}

Describe "Get-FieldType" {

    It "Detecteert string correct" {
        Get-FieldType -Value "test" | Should -Be "string"
    }

    It "Detecteert integer correct" {
        Get-FieldType -Value 5 | Should -Be "integer"
    }

    It "Detecteert boolean correct" {
        Get-FieldType -Value $true | Should -Be "boolean"
    }

    It "Detecteert null correct" {
        Get-FieldType -Value $null | Should -Be "null"
    }

    It "Detecteert array correct" {
        Get-FieldType -Value @(1,2,3) | Should -Be "array"
    }

    It "Detecteert object correct" {
        $obj = [PSCustomObject]@{ a = 1 }
        Get-FieldType -Value $obj | Should -Be "object"
    }
}

Describe "Analyze-DocumentStructure" {

    It "Voegt simpele velden toe aan schema" {
        $schema = @{}
        $doc = [PSCustomObject]@{
            name = "Jan"
            age  = 30
        }

        Analyze-DocumentStructure -Document $doc -Schema $schema -TotalDocs 1

        $schema.Keys | Should -Contain "name"
        $schema.Keys | Should -Contain "age"
        $schema["name"].Types.Keys | Should -Contain "string"
        $schema["age"].Types.Keys | Should -Contain "integer"
    }

    It "Detecteert nested objecten" {
        $schema = @{}
        $doc = [PSCustomObject]@{
            address = [PSCustomObject]@{
                city = "Utrecht"
            }
        }

        Analyze-DocumentStructure -Document $doc -Schema $schema -TotalDocs 1

        $schema.Keys | Should -Contain "address"
        $schema.Keys | Should -Contain "address.city"
        $schema["address"].IsNested | Should -BeTrue
    }

    It "Detecteert arrays en elementtypes" {
        $schema = @{}
        $doc = [PSCustomObject]@{
            tags = @("a","b","c")
        }

        Analyze-DocumentStructure -Document $doc -Schema $schema -TotalDocs 1

        $schema["tags"].IsArray | Should -BeTrue
        $schema["tags"].ArrayElementTypes.Keys | Should -Contain "string"
    }
}

Describe "Get-MongoDBSchema (gemockt)" {

    BeforeEach {
        # Mock MongoDB calls
        Mock Connect-Mdbc {}

        Mock Get-MdbcData {
            if ($Count) {
                return 2
            }

            return @(
                [PSCustomObject]@{
                    name = "Jan"
                    age  = 30
                },
                [PSCustomObject]@{
                    name = "Piet"
                    age  = 40
                }
            )
        }

        # Onderdruk output
        Mock Write-Host {}
        Mock Write-Progress {}
        Mock Display-SchemaResults {}
    }

    It "Geeft een schema hashtable terug" {
        $result = Get-MongoDBSchema `
            -ConnectionString "mongodb://fake" `
            -DatabaseName "testdb" `
            -CollectionName "users" `
            -SampleSize 10

        $result | Should -BeOfType Hashtable
    }

    It "Berekent veld-occurence correct" {
        $schema = Get-MongoDBSchema `
            -ConnectionString "mongodb://fake" `
            -DatabaseName "testdb" `
            -CollectionName "users"

        $schema["name"].Count | Should -Be 2
        $schema["age"].Count  | Should -Be 2
    }

    It "Roept Connect-Mdbc exact één keer aan" {
        Get-MongoDBSchema `
            -ConnectionString "mongodb://fake" `
            -DatabaseName "testdb" `
            -CollectionName "users"

        Assert-MockCalled Connect-Mdbc -Times 1
    }
}
