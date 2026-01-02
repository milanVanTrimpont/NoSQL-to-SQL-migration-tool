function Get-AppConfig {
    param(
        [string]$Path = "$PSScriptRoot\config.json"
    )

    if (-not (Test-Path $Path)) {
        throw "Config file not found: $Path"
    }

    return Get-Content $Path -Raw | ConvertFrom-Json
}
