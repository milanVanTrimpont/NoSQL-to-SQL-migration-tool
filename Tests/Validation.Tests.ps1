<#
.SYNOPSIS
Tests for the validation of a migrated collection.

.DESCRIPTION
Validation is what tells the user whether the migration can be trusted, so the
important thing is that it calls a problem a problem: a difference in record
counts, a document that never arrived, or a field that does not match. These
tests need no database, the counts and the records are handed to the function
through mocks.

The comparison of single values lives in Data_Migration.Tests.ps1; here the
whole validation is run.
#>

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot "..\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1") -Force

    InModuleScope NoSqlToSqlMigration {
        Set-N2SOutputMode -Mode Stream
    }
}

Describe "Test-MigrationValidation" {

    BeforeEach {
        InModuleScope NoSqlToSqlMigration {
            Set-N2SOutputMode -Mode Stream

            # A stand-in for the SQL connection. Validation runs one statement of
            # its own, the COUNT(*), so that is the only thing this has to answer.
            $connection = [PSCustomObject]@{ State = 'Closed' }
            $connection | Add-Member -MemberType ScriptMethod -Name Open -Value { $this.State = 'Open' }
            $connection | Add-Member -MemberType ScriptMethod -Name Close -Value { $this.State = 'Closed' }
            $connection | Add-Member -MemberType ScriptMethod -Name CreateCommand -Value {
                $command = [PSCustomObject]@{ CommandText = '' }
                $command | Add-Member -MemberType ScriptMethod -Name ExecuteScalar -Value {
                    if ($script:TestCountFails) { throw "Table 'db.films' doesn't exist" }
                    return $script:TestSQLCount
                }
                return $command
            }

            # Handed over through the module scope: a variable of the test file is
            # not in scope inside a mock that runs in the module
            $script:TestConnection = $connection
            $script:TestCountFails = $false
            $script:TestSQLCount = 2
            $script:TestMongoCount = 2
            $script:TestDocuments = @(
                @{ _id = '1'; title = 'Heat' }
                @{ _id = '2'; title = 'Alien' }
            )
            $script:TestRecords = @{
                '1' = @{ _id = '1'; title = 'Heat' }
                '2' = @{ _id = '2'; title = 'Alien' }
            }

            Mock Get-AppConfig {
                [PSCustomObject]@{ MongoDB = [PSCustomObject]@{ ConnectionString = 'mongodb://fake'; Database = 'db' } }
            }
            Mock Connect-Mdbc { }
            Mock Get-MdbcData {
                if ($Count) { return $script:TestMongoCount }

                # Honour -Last, so a test can see whether the sample size really
                # reaches MongoDB instead of being applied afterwards
                if ($Last -gt 0) { return @($script:TestDocuments | Select-Object -First $Last) }
                return $script:TestDocuments
            }
            Mock Get-SQLConnectionObject { $script:TestConnection }
            Mock Get-SQLRecord { $script:TestRecords[$Id] }
            Mock Test-DataIntegrity { @() }
        }
    }

    It "reports PASSED when the counts match and every sample is the same" {
        InModuleScope NoSqlToSqlMigration {
            $result = Test-MigrationValidation -TableName "films"

            $result.OverallStatus | Should -Be 'PASSED'
            $result.RecordCountMatch | Should -BeTrue
            $result.MongoCount | Should -Be 2
            $result.SQLCount | Should -Be 2
            $result.SamplesValidated | Should -Be 2
            $result.SamplesPassed | Should -Be 2
            $result.SamplesFailed | Should -Be 0
            $result.Issues.Count | Should -Be 0
        }
    }

    It "reports a difference in record counts as an issue" {
        InModuleScope NoSqlToSqlMigration {
            $ErrorActionPreference = 'SilentlyContinue'
            $script:TestSQLCount = 1

            $result = Test-MigrationValidation -TableName "films"

            $result.RecordCountMatch | Should -BeFalse
            $result.Issues -join ' ' | Should -Match 'Record count mismatch: MongoDB=2, SQL=1'
            $result.OverallStatus | Should -Not -Be 'PASSED'
        }
    }

    It "reports a document that never reached SQL" {
        InModuleScope NoSqlToSqlMigration {
            $ErrorActionPreference = 'SilentlyContinue'
            $script:TestRecords.Remove('2')

            $result = Test-MigrationValidation -TableName "films"

            $result.SamplesPassed | Should -Be 1
            $result.SamplesFailed | Should -Be 1
            $result.Issues -join ' ' | Should -Match 'Document 2 not found in SQL'
            # As many failed as passed is not a partial success
            $result.OverallStatus | Should -Be 'FAILED'
        }
    }

    It "reports a field that holds a different value in SQL" {
        # Most samples still match, so this is a partial success and not a
        # migration that went wrong from beginning to end
        InModuleScope NoSqlToSqlMigration {
            $ErrorActionPreference = 'SilentlyContinue'
            $script:TestDocuments += @{ _id = '3'; title = 'Se7en' }
            $script:TestRecords['3'] = @{ _id = '3'; title = 'Seven' }
            $script:TestMongoCount = 3
            $script:TestSQLCount = 3

            $result = Test-MigrationValidation -TableName "films"

            $result.SamplesPassed | Should -Be 2
            $result.SamplesFailed | Should -Be 1
            $result.Issues -join ' ' | Should -Match 'Document 3 has mismatches'
            $result.Issues -join ' ' | Should -Match 'title'
            $result.OverallStatus | Should -Be 'PARTIAL'
        }
    }

    It "never validates more samples than there are documents" {
        InModuleScope NoSqlToSqlMigration {
            $result = Test-MigrationValidation -TableName "films" -SampleSize 100

            $result.SamplesValidated | Should -Be 2
        }
    }

    It "validates no more documents than the sample size asks for" {
        # The sample size has to reach MongoDB: reading everything and counting
        # only part of it would make a large collection slow for nothing
        InModuleScope NoSqlToSqlMigration {
            $result = Test-MigrationValidation -TableName "films" -SampleSize 1

            $result.SamplesValidated | Should -Be 1
            $result.SamplesPassed | Should -Be 1
            Should -Invoke Get-SQLRecord -Times 1 -Exactly
        }
    }

    It "handles an empty collection without validating samples" {
        InModuleScope NoSqlToSqlMigration {
            $script:TestMongoCount = 0
            $script:TestSQLCount = 0

            $result = Test-MigrationValidation -TableName "films"

            $result.OverallStatus | Should -Be 'PASSED'
            $result.RecordCountMatch | Should -BeTrue
            $result.SamplesValidated | Should -Be 0
            Should -Not -Invoke Get-SQLRecord
        }
    }

    It "treats an integrity remark as a warning and not as a failure" {
        # A NULL in a column is worth mentioning, but it is not a reason to call
        # the migration failed: the document does say NULL there
        InModuleScope NoSqlToSqlMigration {
            Mock Test-DataIntegrity { @("Column 'released' holds 3 NULL value(s)") }

            $result = Test-MigrationValidation -TableName "films"

            $result.OverallStatus | Should -Be 'PASSED'
            $result.Issues.Count | Should -Be 0
            $result.Warnings.Count | Should -Be 1
            $result.Warnings -join ' ' | Should -Match 'NULL'
        }
    }

    It "reports a missing table instead of throwing" {
        # The workflow has to be able to keep going to the next collection
        InModuleScope NoSqlToSqlMigration {
            $ErrorActionPreference = 'SilentlyContinue'
            $script:TestCountFails = $true

            $result = Test-MigrationValidation -TableName "films"

            $result.OverallStatus | Should -Be 'ERROR'
            $result.Issues -join ' ' | Should -Match 'Validation error'
            $result.Issues -join ' ' | Should -Match "doesn't exist"
        }
    }

    It "closes the SQL connection even when validation fails" {
        InModuleScope NoSqlToSqlMigration {
            $ErrorActionPreference = 'SilentlyContinue'
            Mock Get-SQLRecord { throw "connection reset by peer" }

            Test-MigrationValidation -TableName "films" | Out-Null

            $script:TestConnection.State | Should -Be 'Closed'
        }
    }

    It "keeps the comparison of every sample in the result" {
        # Export-ValidationReport writes these out, so a user can see per document
        # what was compared
        InModuleScope NoSqlToSqlMigration {
            $result = Test-MigrationValidation -TableName "films"

            $result.Details.Count | Should -Be 2
            $result.Details[0].FieldsCompared | Should -BeGreaterThan 0
        }
    }
}
