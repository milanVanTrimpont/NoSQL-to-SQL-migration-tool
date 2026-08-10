<#
.SYNOPSIS
Tests for a sub-document whose keys are ids instead of field names.

.DESCRIPTION
An export from Firebase or Firestore holds its records in a sub-document keyed by
id: { "users": { "user_001": {...}, "user_002": {...} } }. Read as a record that
gives a column per id, which produced a table of 2297 columns that MySQL refuses.
These tests cover the whole path: recognising it, the schema, the generated table,
the rows written, and the row count a sync expects.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "Test-IsDocumentMap" {

    It "recognises three documents with the same fields as a collection" {
        InModuleScope NoSqlToSqlMigration {
            $value = @{
                user_001 = @{ firstName = "Ann"; lastName = "Peeters" }
                user_002 = @{ firstName = "Bo";  lastName = "Janssens" }
                user_003 = @{ firstName = "Cas"; lastName = "Maes" }
            }

            Test-IsDocumentMap -Value $value | Should -BeTrue
        }
    }

    It "leaves a real record alone, because its parts describe different things" {
        InModuleScope NoSqlToSqlMigration {
            $value = @{
                address  = @{ city = "Gent"; street = "Korenmarkt" }
                contact  = @{ email = "a@b.c"; phone = "0900" }
                metadata = @{ status = "Active"; source = "import" }
            }

            Test-IsDocumentMap -Value $value | Should -BeFalse
        }
    }

    It "leaves two keys alone, even with the same fields" {
        # home and work with the same shape can be read both ways, and columns is
        # the reading that changes nothing about existing datasets
        InModuleScope NoSqlToSqlMigration {
            $value = @{
                home = @{ street = "Korenmarkt"; city = "Gent" }
                work = @{ street = "Veldstraat"; city = "Gent" }
            }

            Test-IsDocumentMap -Value $value | Should -BeFalse
        }
    }

    It "leaves a sub-document with a plain value alone" {
        # user_info holds a name next to a sub-document, so its keys are field names
        InModuleScope NoSqlToSqlMigration {
            $value = @{
                full_name = "Milan"
                address   = @{ city = "Gent" }
                contact   = @{ email = "a@b.c" }
            }

            Test-IsDocumentMap -Value $value | Should -BeFalse
        }
    }

    It "is not fooled by an array or a scalar" {
        InModuleScope NoSqlToSqlMigration {
            Test-IsDocumentMap -Value @(1, 2, 3) | Should -BeFalse
            Test-IsDocumentMap -Value "text" | Should -BeFalse
            Test-IsDocumentMap -Value $null | Should -BeFalse
        }
    }

    It "is not thrown off by optional fields" {
        # The old rule demanded a field in every record: 3 of 7 field names, 0.43,
        # and the whole table fell back to a column per id. Optional fields are
        # normal in MongoDB, so they may not decide this.
        InModuleScope NoSqlToSqlMigration {
            $value = @{
                a = @{ date = "1"; liters = 40; price = 70; station = "Total"; note = "full" }
                b = @{ date = "2"; liters = 38; price = 65; station = "Q8";    note = "half"; discount = 1 }
                c = @{ date = "3"; liters = 41; price = 72; station = "Shell" }
                d = @{ date = "4"; liters = 39; price = 68; tip = 2 }
            }

            Test-IsDocumentMap -Value $value | Should -BeTrue
        }
    }

    It "does demand a field in every record when asked to" {
        # The strict reading is still reachable, and then this is not a collection
        InModuleScope NoSqlToSqlMigration {
            $value = @{
                a = @{ date = "1"; liters = 40; price = 70; station = "Total"; note = "full" }
                b = @{ date = "2"; liters = 38; price = 65; station = "Q8";    note = "half"; discount = 1 }
                c = @{ date = "3"; liters = 41; price = 72; station = "Shell" }
                d = @{ date = "4"; liters = 39; price = 68; tip = 2 }
            }

            Test-IsDocumentMap -Value $value -MinimumFieldPresence 1.0 | Should -BeFalse
        }
    }

    It "still says no when every record brings its own fields" {
        # Counting per field may not turn a real record into a collection
        InModuleScope NoSqlToSqlMigration {
            $value = @{
                a = @{ x = 1; y = 2 }
                b = @{ x = 1; z = 3 }
                c = @{ x = 1; w = 4 }
            }

            Test-IsDocumentMap -Value $value | Should -BeFalse
        }
    }

    It "accepts a shape that differs a little" {
        # One record with an extra field is normal in MongoDB and may not turn the
        # whole collection back into columns
        InModuleScope NoSqlToSqlMigration {
            $value = @{
                a = @{ date = "2026-01-01"; liters = 40; price = 70 }
                b = @{ date = "2026-01-02"; liters = 38; price = 65 }
                c = @{ date = "2026-01-03"; liters = 41; price = 72; note = "full" }
            }

            Test-IsDocumentMap -Value $value | Should -BeTrue
        }
    }
}

Describe "Add-DocumentToSchema with a map" {

    It "records the map as a collection, not as fields" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $document = @{
                _id   = "1"
                users = @{
                    user_001 = @{ firstName = "Ann"; groupId = "g1" }
                    user_002 = @{ firstName = "Bo";  groupId = "g1" }
                    user_003 = @{ firstName = "Cas"; groupId = "g2" }
                }
            }

            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $schema['users'].IsMap | Should -BeTrue
            $schema['users'].IsArray | Should -BeTrue
            $schema['users'].ArrayElementTypes['object'] | Should -Be 3

            # The fields of the records, once, instead of once per key
            $schema.ContainsKey('users[].firstName') | Should -BeTrue
            $schema.ContainsKey('users[].groupId') | Should -BeTrue
            $schema.ContainsKey('users.user_001.firstName') | Should -BeFalse
        }
    }

    It "keeps the length of the longest key" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $document = @{ refuels = @{
                short                  = @{ liters = 1 }
                "9KYzu6AK2eDzs8yb0CvE" = @{ liters = 2 }
                other                  = @{ liters = 3 }
            } }

            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $schema['refuels'].MaxKeyLength | Should -Be 20
        }
    }

    It "still treats a real record as fields" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $document = @{ contact = @{ email = "a@b.c"; address = @{ city = "Gent" } } }

            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $schema['contact'].IsNested | Should -BeTrue
            $schema['contact'].IsMap | Should -BeFalse
            $schema.ContainsKey('contact.email') | Should -BeTrue
        }
    }
}

Describe "New-SQLSchema with a map" {

    It "gives the table a key column instead of a column per id" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $document = @{
                _id     = "1"
                refuels = @{
                    aaa = @{ date = "2026-01-01"; liters = 40 }
                    bbb = @{ date = "2026-01-02"; liters = 38 }
                    ccc = @{ date = "2026-01-03"; liters = 41 }
                }
            }
            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $result = New-SQLSchema -Schema $schema -TableName "trips"
            $create = ($result.Statements -join "`n")

            $result.Tables | Should -Contain 'trips_refuels'
            $create | Should -Match '\[map_key\] VARCHAR\(255\) NOT NULL'
            $create | Should -Match '\[date\]'
            $create | Should -Match '\[liters\]'

            # Not one column per id, and no array position that means nothing here
            $create | Should -Not -Match '\[aaa\]'
            ($result.Statements | Where-Object { $_ -match 'trips_refuels' }) -join '' |
                Should -Not -Match 'array_index'
        }
    }

    It "keeps array_index for a real array" {
        InModuleScope NoSqlToSqlMigration {
            $schema = @{}
            $document = @{ _id = "1"; history = @(@{ event = "created" }, @{ event = "changed" }) }
            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $result = New-SQLSchema -Schema $schema -TableName "trips"
            $create = ($result.Statements -join "`n")

            $create | Should -Match 'array_index'
            $create | Should -Not -Match 'map_key'
        }
    }
}

Describe "Invoke-ChildTableMigration with a map" {

    It "writes one row per key, with the key in the row" {
        InModuleScope NoSqlToSqlMigration {
            $script:TestRows = @()

            Mock Get-SQLTableColumns {
                @{ 'trips__id' = 'varchar(255)'; 'map_key' = 'varchar(255)'
                   'date' = 'varchar(255)'; 'liters' = 'decimal(18,2)' }
            }
            Mock Test-SQLRowBufferActive { $true }
            Mock Add-BufferedDelete { }
            Mock Add-SQLRow { $script:TestRows += $Row }

            $value = [ordered]@{
                aaa = @{ date = "2026-01-01"; liters = 40 }
                bbb = @{ date = "2026-01-02"; liters = 38 }
            }

            $written = Invoke-ChildTableMigration -Connection $null -ChildTable "trips_refuels" `
                                                  -ParentKeyColumn "trips__id" -ParentId "1" `
                                                  -Value $value -DatabaseType "MySQL"

            $written | Should -Be 2
            $script:TestRows.Count | Should -Be 2
            @($script:TestRows | ForEach-Object { $_['map_key'] }) | Should -Contain 'aaa'
            @($script:TestRows | ForEach-Object { $_['map_key'] }) | Should -Contain 'bbb'
            $script:TestRows[0]['trips__id'] | Should -Be "1"
            $script:TestRows[0]['date'] | Should -Be "2026-01-01"
        }
    }

    It "still writes a real record as a single row" {
        InModuleScope NoSqlToSqlMigration {
            $script:TestRows = @()

            Mock Get-SQLTableColumns {
                @{ 'trips__id' = 'varchar(255)'; 'city' = 'varchar(255)' }
            }
            Mock Test-SQLRowBufferActive { $true }
            Mock Add-BufferedDelete { }
            Mock Add-SQLRow { $script:TestRows += $Row }

            $written = Invoke-ChildTableMigration -Connection $null -ChildTable "trips_address" `
                                                  -ParentKeyColumn "trips__id" -ParentId "1" `
                                                  -Value @{ city = "Gent" } -DatabaseType "MySQL"

            $written | Should -Be 1
            $script:TestRows[0]['city'] | Should -Be "Gent"
        }
    }
}

Describe "Get-ExpectedChildRowCount with a map" {

    It "counts a row per key" {
        # Otherwise a sync sees drift on every document and rewrites all of it
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "1"; users = @{
                a = @{ name = "Ann" }; b = @{ name = "Bo" }; c = @{ name = "Cas" }
            } }

            Get-ExpectedChildRowCount -Document $document -FieldName "users" | Should -Be 3
        }
    }

    It "still counts a real record as one row" {
        InModuleScope NoSqlToSqlMigration {
            $document = @{ _id = "1"; address = @{ city = "Gent"; street = "Korenmarkt" } }

            Get-ExpectedChildRowCount -Document $document -FieldName "address" | Should -Be 1
        }
    }
}

Describe "Quoting of table names" {

    BeforeAll {
        # Records the statements it is asked to run, and answers nothing
        function New-RecordingConnection {
            $log = [System.Collections.ArrayList]::new()
            $connection = [PSCustomObject]@{ Log = $log }

            $connection | Add-Member -MemberType ScriptMethod -Name CreateCommand -Value {
                $command = [PSCustomObject]@{ CommandText = ''; Log = $this.Log }

                $command | Add-Member -MemberType ScriptMethod -Name ExecuteReader -Value {
                    $this.Log.Add($this.CommandText) | Out-Null

                    $reader = [PSCustomObject]@{}
                    $reader | Add-Member -MemberType ScriptMethod -Name Read -Value { return $false }
                    $reader | Add-Member -MemberType ScriptMethod -Name Close -Value { }
                    return $reader
                }

                return $command
            }

            return $connection
        }
    }

    It "quotes the table name when reading the existing rows" {
        # A collection named 20 gives "SELECT _id FROM 20", which MySQL refuses
        $connection = New-RecordingConnection

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Get-AllSQLRecords -Connection $Connection -TableName "20" -DatabaseType "MySQL" | Out-Null
        }

        $connection.Log[0] | Should -Be 'SELECT `_id` FROM `20`'
    }

    It "stops instead of pretending the table is empty" {
        # Regression: a failed read returned an empty list, so the sync saw every
        # document as new and ran into duplicate keys
        $connection = [PSCustomObject]@{}
        $connection | Add-Member -MemberType ScriptMethod -Name CreateCommand -Value {
            $command = [PSCustomObject]@{ CommandText = '' }
            $command | Add-Member -MemberType ScriptMethod -Name ExecuteReader -Value {
                throw "You have an error in your SQL syntax"
            }
            return $command
        }

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            { Get-AllSQLRecords -Connection $Connection -TableName "20" -DatabaseType "MySQL" } |
                Should -Throw "*Could not read the existing rows*"
        }
    }

    It "quotes the table name when checking for schema changes" {
        $connection = New-RecordingConnection

        InModuleScope NoSqlToSqlMigration -Parameters @{ Connection = $connection } {
            param($Connection)

            Update-SQLSchema -Connection $Connection -TableName "20" `
                             -MongoDocuments @(@{ _id = "1" }) -DatabaseType "MySQL" | Out-Null
        }

        $connection.Log[0] | Should -Be 'SHOW COLUMNS FROM `20`'
    }
}
