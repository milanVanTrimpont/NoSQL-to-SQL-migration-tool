@{
    # Script module or binary module file associated with this manifest
    RootModule = 'NoSqlToSqlMigration.psm1'
    
    # Version number of this module
    ModuleVersion = '1.0.0'
    
    # ID used to uniquely identify this module
    GUID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
    
    # Author of this module
    Author = 'Milan Van Trimpont'
    
    # Copyright statement for this module
    Copyright = '(c) 2026 Milan Van Trimpont. All rights reserved.'
    
    # Description of the functionality provided by this module
    Description = 'PowerShell module for migrating data from NoSQL (MongoDB) databases to SQL databases (MySQL/SQL Server). Supports schema analysis, data migration, validation, and incremental synchronization.'
    
    # Minimum version of the PowerShell engine required by this module
    PowerShellVersion = '7.0'
    
    # Functions to export from this module (use * to export all)
    FunctionsToExport = '*'
    
    # Cmdlets to export from this module
    CmdletsToExport = @()
    
    # Variables to export from this module
    VariablesToExport = @()
    
    # Aliases to export from this module
    AliasesToExport = @()
    
    # Private data to pass to the module specified in RootModule/ModuleToProcess
    PrivateData = @{
        PSData = @{
            # Tags applied to this module for module discovery
            Tags = @('MongoDB', 'SQL', 'MySQL', 'SQLServer', 'Migration', 'Database', 'NoSQL')
            
            # A URL to the license for this module
            LicenseUri = ''
            
            # A URL to the main website for this project
            ProjectUri = ''
            
            # ReleaseNotes of this module
            ReleaseNotes = 'Initial release of NoSQL to SQL Migration Tool'
        }
    }
}
