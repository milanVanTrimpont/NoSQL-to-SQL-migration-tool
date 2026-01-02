<#
.SYNOPSIS
Loads application configuration from a JSON file.

.DESCRIPTION
Reads and parses the configuration file containing database connection settings
for MongoDB, MySQL, and SQL Server. The configuration file should be in JSON format
and contain connection strings, credentials, and other required settings.

.PARAMETER Path
The path to the configuration file. Defaults to config.json in the script's directory.

#>
function Get-AppConfig {
    param(
        [string]$Path = "$PSScriptRoot\config.json"
    )

    if (-not (Test-Path $Path)) {
        throw "Config file not found: $Path"
    }

    return Get-Content $Path -Raw | ConvertFrom-Json
}
