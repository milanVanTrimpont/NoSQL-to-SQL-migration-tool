<#
.SYNOPSIS
Runs the test suite with a known Pester version.

.DESCRIPTION
Both Pester 3.x (shipped with Windows) and Pester 5+ can be installed side by
side, and they are not compatible: a test file written for Pester 5 fails with
"BeforeAll may only be used inside a Describe block" when Pester 3 happens to
load first. This script loads Pester 5 or newer explicitly, so the result does
not depend on which version a machine picks.

Tests tagged Integration need a running MongoDB and MySQL and are skipped
unless -Integration is given.

.PARAMETER Integration
Also run the tests that require live databases.

.PARAMETER Detailed
Show every test instead of a summary.

.EXAMPLE
pwsh -File .\Tests\Invoke-Tests.ps1

.EXAMPLE
pwsh -File .\Tests\Invoke-Tests.ps1 -Integration -Detailed
#>

[CmdletBinding()]
param (
    [switch]$Integration,
    [switch]$Detailed
)

# Deliberately not 'Stop': tests check that the code writes a non-terminating
# error, and Stop would turn that into an aborted test
$ErrorActionPreference = 'Continue'

Get-Module Pester | Remove-Module -Force

$pester = Get-Module -ListAvailable Pester |
          Where-Object { $_.Version -ge [version]'5.0.0' } |
          Sort-Object Version -Descending |
          Select-Object -First 1

if (-not $pester) {
    $host.UI.WriteErrorLine("Pester 5 or newer is required. Install it with: Install-Module Pester -MinimumVersion 5.0 -Scope CurrentUser")
    exit 2
}

Import-Module $pester.Path -Force
Write-Host "Using Pester $($pester.Version)" -ForegroundColor Cyan

$configuration = New-PesterConfiguration
$configuration.Run.Path = $PSScriptRoot
$configuration.Run.PassThru = $true
$configuration.Output.Verbosity = if ($Detailed) { 'Detailed' } else { 'Normal' }

if (-not $Integration) {
    $configuration.Filter.ExcludeTag = 'Integration'
}

$result = Invoke-Pester -Configuration $configuration

Write-Host ""
Write-Host "Passed : $($result.PassedCount)" -ForegroundColor Green
Write-Host "Failed : $($result.FailedCount)" -ForegroundColor $(if ($result.FailedCount -gt 0) { 'Red' } else { 'Gray' })
Write-Host "Skipped: $($result.SkippedCount)" -ForegroundColor Gray

exit $(if ($result.FailedCount -gt 0) { 1 } else { 0 })
