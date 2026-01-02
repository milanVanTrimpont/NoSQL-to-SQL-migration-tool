# NoSQL to SQL Migration Tool
# Main module file

#region Private Functions

# Load all private functions
$privateFunctions = @(
    'Config.ps1',
    'Connection_DB.ps1',
    'Analyze_scheme.ps1',
    'Sql_Schema_Generator.ps1',
    'Data_Migration.ps1',
    'Migration_Validation.ps1',
    'Sync.ps1'
)

$privateLoadErrors = @()

foreach ($function in $privateFunctions) {
    $functionPath = Join-Path -Path (Split-Path $PSScriptRoot -Parent) -ChildPath "private\$function"
    if (Test-Path $functionPath) {
        try {
            . $functionPath
            Write-Verbose "Loaded private function: $function"
        }
        catch {
            $privateLoadErrors += "Error loading $function : $_"
        }
    }
    else {
        $privateLoadErrors += "Could not find private function: $functionPath"
    }
}

if ($privateLoadErrors.Count -gt 0) {
    Write-Warning "Module loading errors:"
    $privateLoadErrors | ForEach-Object { Write-Warning $_ }
}

#endregion

#region Public Functions

# Load all public functions
$publicFunctions = @(
    'MasterWorkflow.ps1'
)

foreach ($function in $publicFunctions) {
    $functionPath = Join-Path -Path (Split-Path $PSScriptRoot -Parent) -ChildPath "public\$function"
    if (Test-Path $functionPath) {
        . $functionPath
    }
    else {
        Write-Warning "Could not find public function: $functionPath"
    }
}

#endregion

#region Module Exports

# Export all functions - use wildcard to export everything that was dot-sourced
Export-ModuleMember -Function * -Verbose

#endregion
