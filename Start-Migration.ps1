<#
.SYNOPSIS
Runs a migration, sync or validation without any interaction.

.DESCRIPTION
Entry point for automated environments such as Task Scheduler, cron or a build
pipeline. It asks nothing, prints its progress to the information stream so the
caller can redirect or silence it, and ends with an exit code:

  0  everything finished correctly
  1  at least one collection failed
  2  the run could not start (configuration or connection problem)

For interactive use, run InteractiveMenu.ps1 instead.

.PARAMETER Collections
Collections to process. Leave empty to process every collection in the database.

.PARAMETER Operation
FullMigration, IncrementalSync, ValidationOnly or SchemaOnly.

.PARAMETER DatabaseType
MySQL or SQLServer.

.PARAMETER SampleSize
Number of documents to analyse for the schema.

.PARAMETER ConfigPath
Configuration file to use. Defaults to config.json next to this script.

.PARAMETER Quiet
Only report warnings and errors.

.EXAMPLE
# Daily sync of every collection
pwsh -File .\Start-Migration.ps1 -Operation IncrementalSync

.EXAMPLE
# Full migration of two collections, output appended to a log file
pwsh -File .\Start-Migration.ps1 -Collections test,users -Operation FullMigration 6>> .\migration.log
#>

[CmdletBinding()]
param (
    [string[]]$Collections = @(),

    [ValidateSet("FullMigration", "IncrementalSync", "ValidationOnly", "SchemaOnly")]
    [string]$Operation = "IncrementalSync",

    [ValidateSet("MySQL", "SQLServer")]
    [string]$DatabaseType = "MySQL",

    [int]$SampleSize = 100,

    [string]$ConfigPath,

    [switch]$Quiet
)

# Not 'Stop': an error written by the module would otherwise terminate this
# script before it can return its own exit code
$ErrorActionPreference = 'Continue'

try {
    $modulePath = Join-Path $PSScriptRoot "NoSqlToSqlMigration\NoSqlToSqlMigration.psd1"

    if (-not (Test-Path $modulePath)) {
        $host.UI.WriteErrorLine("Module not found: $modulePath")
        exit 2
    }

    Import-Module $modulePath -Force -ErrorAction Stop

    $arguments = @{
        Collections  = $Collections
        Operation    = $Operation
        DatabaseType = $DatabaseType
        SampleSize   = $SampleSize
        Quiet        = $Quiet
    }

    if ($ConfigPath) {
        $arguments['ConfigPath'] = $ConfigPath
    }

    $result = Invoke-N2SMigration @arguments

    if ($null -eq $result -or $null -eq $result.ExitCode) {
        exit 2
    }

    exit $result.ExitCode
}
catch {
    # Write to the error stream directly: Write-Error inside a catch can
    # terminate the script before it reaches its exit code
    $host.UI.WriteErrorLine("Could not start the migration: $($_.Exception.Message)")
    exit 2
}
