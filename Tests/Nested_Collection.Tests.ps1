<#
.SYNOPSIS
Tests for a collection that sits inside the records of another collection.

.DESCRIPTION
A Firebase export nests further than one level: every refuel holds an array of
repayments. Those used to become columns of the refuels table, with a backtick in
their name because of the [] in the schema path, and nothing ever wrote to them:
26 repayments disappeared while the migration reported success. They now get a
table of their own, tied to the record they belong to.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "New-SQLSchema with a collection inside a collection" {

    It "gives it a table of its own instead of columns in its parent" {
        InModuleScope NoSqlToSqlMigration {
            $WarningPreference = 'SilentlyContinue'
            $schema = @{}
            $document = @{
                _id     = "1"
                refuels = @{
                    aaa = @{ amount = 40; repayments = @(@{ userId = "u1"; paid = 10 }) }
                    bbb = @{ amount = 38; repayments = @(@{ userId = "u2"; paid = 5 }, @{ userId = "u3"; paid = 6 }) }
                    ccc = @{ amount = 41; repayments = @(@{ userId = "u4"; paid = 7 }) }
                }
            }
            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $result = New-SQLSchema -Schema $schema -TableName "trips"

            $result.Tables | Should -Contain 'trips_refuels'
            $result.Tables | Should -Contain 'trips_refuels_repayments'

            $nested = ($result.Statements | Where-Object { $_ -match 'trips_refuels_repayments' }) -join "`n"
            $nested | Should -Match '\[parent_key\] VARCHAR\(255\) NOT NULL'
            $nested | Should -Match '\[userId\]'
            $nested | Should -Match '\[paid\]'
        }
    }

    It "keeps neither the collection nor its fields in the parent table" {
        InModuleScope NoSqlToSqlMigration {
            $WarningPreference = 'SilentlyContinue'
            $schema = @{}
            $document = @{ _id = "1"; refuels = @{
                aaa = @{ amount = 40; repayments = @(@{ paid = 1 }) }
                bbb = @{ amount = 38; repayments = @(@{ paid = 2 }) }
                ccc = @{ amount = 41; repayments = @(@{ paid = 3 }) }
            } }
            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $result = New-SQLSchema -Schema $schema -TableName "trips"

            $parent = ($result.Statements | Where-Object {
                $_ -match 'trips_refuels' -and $_ -notmatch 'trips_refuels_repayments'
            }) -join "`n"

            $parent | Should -Match '\[amount\]'
            $parent | Should -Not -Match 'repayments'
        }
    }

    It "never puts a backtick in a column name" {
        # Convert-ToMySQLSyntax turns [ and ] into backticks, so a path holding []
        # used to come out as repayments`.amount
        InModuleScope NoSqlToSqlMigration {
            $WarningPreference = 'SilentlyContinue'
            $schema = @{}
            $document = @{ _id = "1"; refuels = @{
                aaa = @{ repayments = @(@{ paid = 1 }) }
                bbb = @{ repayments = @(@{ paid = 2 }) }
                ccc = @{ repayments = @(@{ paid = 3 }) }
            } }
            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $result = New-SQLSchema -Schema $schema -TableName "trips"

            foreach ($statement in $result.Statements) {
                $mysql = Convert-ToMySQLSyntax -SQLStatement $statement
                $mysql | Should -Not -Match '``'
            }
        }
    }

    It "gives a collection of plain values a value column" {
        InModuleScope NoSqlToSqlMigration {
            $WarningPreference = 'SilentlyContinue'
            $schema = @{}
            $document = @{ _id = "1"; refuels = @{
                aaa = @{ amount = 1; debtorIds = @("u1", "u2") }
                bbb = @{ amount = 2; debtorIds = @("u3") }
                ccc = @{ amount = 3; debtorIds = @("u4") }
            } }
            Add-DocumentToSchema -Document $document -Schema $schema -TotalDocs 1

            $result = New-SQLSchema -Schema $schema -TableName "trips"
            $nested = ($result.Statements | Where-Object { $_ -match 'trips_refuels_debtorIds' }) -join "`n"

            $result.Tables | Should -Contain 'trips_refuels_debtorIds'
            $nested | Should -Match '\[value\]'
            $nested | Should -Match '\[array_index\] INT NOT NULL'
        }
    }
}

Describe "Add-NestedCollectionRow" {

    BeforeEach {
        InModuleScope NoSqlToSqlMigration {
            $script:TestRows = @()
            $script:TestDeletes = @()

            Mock Get-SQLTableColumns {
                if ($TableName -eq 'trips_refuels') {
                    return @{ 'trips__id' = 'varchar(255)'; 'map_key' = 'varchar(255)'; 'amount' = 'int' }
                }
                if ($TableName -eq 'trips_refuels_repayments') {
                    return @{ 'trips__id' = 'varchar(255)'; 'parent_key' = 'varchar(255)'
                              'array_index' = 'int'; 'userId' = 'varchar(255)'; 'paid' = 'int' }
                }
                return @{}
            }
            Mock Test-SQLRowBufferActive { $true }
            Mock Add-BufferedDelete { $script:TestDeletes += $TableName }
            Mock Add-SQLRow { $script:TestRows += [PSCustomObject]@{ Table = $TableName; Row = $Row } }
        }
    }

    It "writes one row per entry, tied to the record it belongs to" {
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                aaa = @{ amount = 40; repayments = @(@{ userId = "u1"; paid = 10 }) }
                bbb = @{ amount = 38; repayments = @(@{ userId = "u2"; paid = 5 }, @{ userId = "u3"; paid = 6 }) }
            }

            $written = Invoke-ChildTableMigration -Connection $null -ChildTable "trips_refuels" `
                                                  -ParentKeyColumn "trips__id" -ParentId "1" `
                                                  -Value $value -DatabaseType "MySQL"

            # Two refuels plus three repayments
            $written | Should -Be 5

            $repayments = @($script:TestRows | Where-Object { $_.Table -eq 'trips_refuels_repayments' })
            $repayments.Count | Should -Be 3
            @($repayments | ForEach-Object { $_.Row['parent_key'] }) | Should -Contain 'aaa'
            @($repayments | ForEach-Object { $_.Row['parent_key'] }) | Should -Contain 'bbb'
            @($repayments | ForEach-Object { $_.Row['userId'] }) | Should -Contain 'u3'
            $repayments[0].Row['trips__id'] | Should -Be "1"
        }
    }

    It "starts counting positions again per record" {
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                aaa = @{ repayments = @(@{ paid = 1 }) }
                bbb = @{ repayments = @(@{ paid = 2 }, @{ paid = 3 }) }
            }

            Invoke-ChildTableMigration -Connection $null -ChildTable "trips_refuels" `
                                       -ParentKeyColumn "trips__id" -ParentId "1" `
                                       -Value $value -DatabaseType "MySQL" | Out-Null

            $ofBbb = @($script:TestRows |
                       Where-Object { $_.Table -eq 'trips_refuels_repayments' -and $_.Row['parent_key'] -eq 'bbb' })

            @($ofBbb | ForEach-Object { $_.Row['array_index'] }) | Should -Be @(0, 1)
        }
    }

    It "removes the old rows once per document, not once per record" {
        # Without a buffer a delete per record would wipe what the record before it
        # had just written
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                aaa = @{ repayments = @(@{ paid = 1 }) }
                bbb = @{ repayments = @(@{ paid = 2 }) }
                ccc = @{ repayments = @(@{ paid = 3 }) }
            }

            Invoke-ChildTableMigration -Connection $null -ChildTable "trips_refuels" `
                                       -ParentKeyColumn "trips__id" -ParentId "1" `
                                       -Value $value -DatabaseType "MySQL" | Out-Null

            @($script:TestDeletes | Where-Object { $_ -eq 'trips_refuels_repayments' }).Count | Should -Be 1
        }
    }

    It "leaves a record alone: that stays a set of columns" {
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                aaa = @{ amount = 40; date = @{ _seconds = 1767184524 } }
                bbb = @{ amount = 38; date = @{ _seconds = 1767135208 } }
            }

            $written = Invoke-ChildTableMigration -Connection $null -ChildTable "trips_refuels" `
                                                  -ParentKeyColumn "trips__id" -ParentId "1" `
                                                  -Value $value -DatabaseType "MySQL"

            $written | Should -Be 2
            @($script:TestRows | Where-Object { $_.Table -ne 'trips_refuels' }).Count | Should -Be 0
        }
    }
}

Describe "A collection inside a sub-document" {

    BeforeEach {
        InModuleScope NoSqlToSqlMigration {
            $script:TestRows = @()
            $script:TestDeletes = @()

            # geo is a record inside every event, and its coordinates are a
            # collection one level deeper
            Mock Get-SQLTableColumns {
                switch ($TableName) {
                    'trips_events' {
                        return @{ 'trips__id' = 'varchar(255)'; 'map_key' = 'varchar(255)'; 'type' = 'varchar(255)' }
                    }
                    'trips_events_geo.coordinates' {
                        return @{ 'trips__id' = 'varchar(255)'; 'parent_key' = 'varchar(255)'
                                  'array_index' = 'int'; 'value' = 'decimal(18,2)' }
                    }
                    'trips_events_a.b.kanalen' {
                        return @{ 'trips__id' = 'varchar(255)'; 'parent_key' = 'varchar(255)'
                                  'array_index' = 'int'; 'value' = 'varchar(255)' }
                    }
                    default { return @{} }
                }
            }
            Mock Test-SQLRowBufferActive { $true }
            Mock Add-BufferedDelete { $script:TestDeletes += $TableName }
            Mock Add-SQLRow { $script:TestRows += [PSCustomObject]@{ Table = $TableName; Row = $Row } }
        }
    }

    It "finds a collection one level down and writes its values" {
        # Regression: the table was created and stayed empty, so 266 coordinate
        # values of 133 events never arrived
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                ev1 = @{ type = "click"; geo = @{ type = "Point"; coordinates = @(4.35, 50.85) } }
                ev2 = @{ type = "view";  geo = @{ type = "Point"; coordinates = @(3.72, 51.05) } }
            }

            Invoke-ChildTableMigration -Connection $null -ChildTable "trips_events" `
                                       -ParentKeyColumn "trips__id" -ParentId "1" `
                                       -Value $value -DatabaseType "MySQL" | Out-Null

            $coordinates = @($script:TestRows | Where-Object { $_.Table -eq 'trips_events_geo.coordinates' })

            $coordinates.Count | Should -Be 4
            @($coordinates | ForEach-Object { $_.Row['value'] }) | Should -Contain 4.35
            @($coordinates | ForEach-Object { $_.Row['parent_key'] }) | Should -Contain 'ev1'
            @($coordinates | ForEach-Object { $_.Row['parent_key'] }) | Should -Contain 'ev2'
        }
    }

    It "keeps counting positions per record, not per document" {
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                ev1 = @{ geo = @{ coordinates = @(4.35, 50.85) } }
                ev2 = @{ geo = @{ coordinates = @(3.72, 51.05) } }
            }

            Invoke-ChildTableMigration -Connection $null -ChildTable "trips_events" `
                                       -ParentKeyColumn "trips__id" -ParentId "1" `
                                       -Value $value -DatabaseType "MySQL" | Out-Null

            $ofEv2 = @($script:TestRows |
                       Where-Object { $_.Table -eq 'trips_events_geo.coordinates' -and $_.Row['parent_key'] -eq 'ev2' })

            @($ofEv2 | ForEach-Object { $_.Row['array_index'] }) | Should -Be @(0, 1)
        }
    }

    It "goes as deep as the records go" {
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                ev1 = @{ a = @{ b = @{ kanalen = @("push", "mail") } } }
                ev2 = @{ a = @{ b = @{ kanalen = @("sms") } } }
                ev3 = @{ a = @{ b = @{ } } }
            }

            Invoke-ChildTableMigration -Connection $null -ChildTable "trips_events" `
                                       -ParentKeyColumn "trips__id" -ParentId "1" `
                                       -Value $value -DatabaseType "MySQL" | Out-Null

            @($script:TestRows | Where-Object { $_.Table -eq 'trips_events_a.b.kanalen' }).Count | Should -Be 3
        }
    }

    It "still removes the old rows only once per document" {
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                ev1 = @{ geo = @{ coordinates = @(1, 2) } }
                ev2 = @{ geo = @{ coordinates = @(3, 4) } }
                ev3 = @{ geo = @{ coordinates = @(5, 6) } }
            }

            Invoke-ChildTableMigration -Connection $null -ChildTable "trips_events" `
                                       -ParentKeyColumn "trips__id" -ParentId "1" `
                                       -Value $value -DatabaseType "MySQL" | Out-Null

            @($script:TestDeletes | Where-Object { $_ -eq 'trips_events_geo.coordinates' }).Count | Should -Be 1
        }
    }

    It "writes nothing extra for a record that only holds plain values" {
        InModuleScope NoSqlToSqlMigration {
            $value = [ordered]@{
                ev1 = @{ type = "click"; device = @{ os = "Android"; version = "14" } }
                ev2 = @{ type = "view";  device = @{ os = "iOS"; version = "17" } }
            }

            $written = Invoke-ChildTableMigration -Connection $null -ChildTable "trips_events" `
                                                  -ParentKeyColumn "trips__id" -ParentId "1" `
                                                  -Value $value -DatabaseType "MySQL"

            $written | Should -Be 2
            @($script:TestRows | Where-Object { $_.Table -ne 'trips_events' }).Count | Should -Be 0
        }
    }

    It "handles a field that is a collection in one record and a record in another" {
        # geo is polymorphic in the test data: both tables get the rows that apply
        InModuleScope NoSqlToSqlMigration {
            Mock Get-SQLTableColumns {
                switch ($TableName) {
                    'trips_events' {
                        return @{ 'trips__id' = 'varchar(255)'; 'map_key' = 'varchar(255)' }
                    }
                    'trips_events_geo' {
                        return @{ 'trips__id' = 'varchar(255)'; 'parent_key' = 'varchar(255)'
                                  'array_index' = 'int'; 'value' = 'decimal(18,2)' }
                    }
                    'trips_events_geo.coordinates' {
                        return @{ 'trips__id' = 'varchar(255)'; 'parent_key' = 'varchar(255)'
                                  'array_index' = 'int'; 'value' = 'decimal(18,2)' }
                    }
                    default { return @{} }
                }
            }

            $value = [ordered]@{
                ev1 = @{ geo = @(1.5, 2.5) }
                ev2 = @{ geo = @{ coordinates = @(3.5, 4.5) } }
            }

            Invoke-ChildTableMigration -Connection $null -ChildTable "trips_events" `
                                       -ParentKeyColumn "trips__id" -ParentId "1" `
                                       -Value $value -DatabaseType "MySQL" | Out-Null

            @($script:TestRows | Where-Object { $_.Table -eq 'trips_events_geo' }).Count | Should -Be 2
            @($script:TestRows | Where-Object { $_.Table -eq 'trips_events_geo.coordinates' }).Count | Should -Be 2
        }
    }
}

Describe "Get-ChildTableMap and nested collections" {

    It "does not read a nested collection table as a field of the document" {
        # Its field name would be refuels_repayments, which no document has, so a
        # sync would see drift every run and the cleanup would offer it as leftover
        $connection = [PSCustomObject]@{}
        $connection | Add-Member -MemberType ScriptMethod -Name CreateCommand -Value {
            $command = [PSCustomObject]@{ CommandText = '' }
            $command | Add-Member -MemberType ScriptMethod -Name ExecuteReader -Value {
                $reader = [PSCustomObject]@{ Rows = @("trips_refuels", "trips_refuels_repayments"); Index = -1 }
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
                if ($TableName -eq 'trips_refuels_repayments') {
                    return @{ trips__id = 'varchar(255)'; parent_key = 'varchar(255)'; paid = 'int' }
                }
                return @{ trips__id = 'varchar(255)'; map_key = 'varchar(255)' }
            }

            $map = Get-ChildTableMap -Connection $Connection -TableName "trips" -PrimaryKeyField "_id"

            $map.ContainsKey('refuels') | Should -BeTrue
            $map.ContainsKey('refuels_repayments') | Should -BeFalse
        }
    }
}
