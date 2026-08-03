@{
    # The module itself. All logic lives in this one file on purpose.
    RootModule = 'NoSqlToSqlMigration.psm1'

    ModuleVersion = '2.0.0'

    # Only PowerShell 7 and up: the module uses .NET drivers and modern operators
    CompatiblePSEditions = @('Core')
    PowerShellVersion = '7.0'

    GUID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'

    Author = 'Milan Van Trimpont'
    Copyright = '(c) 2026 Milan Van Trimpont. All rights reserved.'

    Description = @'
    Migrates documents from MongoDB to a relational MySQL database and keeps both
    sides synchronised. Analyses a collection, generates the tables including child
    tables for arrays and sub-documents, converts values per column so a difference
    in type or format does not cost a document, validates the result, and syncs only
    what changed. Usable from an interactive menu as well as unattended, with exit
    codes for a scheduled task.
'@

    # MongoDB access. Named here so importing fails with a clear message instead
    # of a confusing error at the first query.
    RequiredModules = @(
        @{ ModuleName = 'Mdbc'; ModuleVersion = '6.0.0' }
    )

    # MySQL Connector/NET is not a PowerShell module, so it cannot be required
    # here. Initialize-MySQLAssembly looks for MySql.Data.dll at runtime: first in
    # the lib folder next to this manifest, then in the installed connectors.

    # The Pester tests reach internal functions through
    # InModuleScope, so they do not need to be exported for that.
    FunctionsToExport = @(
        'Start-MigrationToolMenu'   # the interactive menu
        'Invoke-MigrationWorkflow'  # one run over one or more collections
        'Invoke-N2SMigration'       # unattended entry point, returns an ExitCode
        'Get-AppConfig'             # read the configuration file
    )

    CmdletsToExport = @()
    VariablesToExport = @()
    AliasesToExport = @()

    # The files that make up the project, for the reader of this manifest
    FileList = @(
        'NoSqlToSqlMigration.psd1'
        'NoSqlToSqlMigration.psm1'
    )

    PrivateData = @{
        PSData = @{
            Tags = @('MongoDB', 'SQL', 'MySQL', 'Migration', 'Database', 'NoSQL', 'ETL', 'Synchronisation')
            LicenseUri = ''
            ProjectUri = 'https://github.com/milanVanTrimpont/NoSQL-to-SQL-migration-tool'
            ReleaseNotes = @'
    2.0.0
    - Values are converted per column: dates in several notations, culture
      independent numbers, and a length check. A value that does not fit is reported
      according to Migration.OnConversionError (Warn, Skip or Fail) and written to a
      conversion report, instead of costing the document.
    - The column type is based on every observed type, so a field holding both dates
      and text becomes a text column.
    - Arrays and sub-documents get their own child tables, and a sync keeps those in
      step, including repairing rows that were removed straight from SQL.
    - Failed documents, a failed validation and a count mismatch are reported as
      failures, with exit codes 0, 1 and 2.
    - Unattended entry point (Invoke-N2SMigration, Start-Migration.ps1). The core
      reports through the PowerShell streams, so output can be silenced, logged or
      filtered by severity.
    - Rows are written as multi-row statements inside a transaction per batch.
    - Tables whose collection or field no longer exists are reported, and dropped
      only after confirmation.
    - Pester suite that runs without a database, plus tagged integration tests.

    1.0.0
    - First version: schema analysis, migration, validation and incremental sync.
'@
        }
    }
}
