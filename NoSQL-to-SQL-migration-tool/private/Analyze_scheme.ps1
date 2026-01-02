function Get-MongoDBSchema {
    <#
    .SYNOPSIS
    Analyzes MongoDB collection structure and generates a schema overview
    
    .DESCRIPTION
    This function examines documents in a MongoDB collection to identify:
    - Field names and their data types
    - Nested structures (objects and arrays)
    - Field occurrence frequency
    - Sample values for each field
    
    .PARAMETER ConnectionString
    MongoDB connection string
    
    .PARAMETER DatabaseName
    Name of the MongoDB database
    
    .PARAMETER CollectionName
    Name of the collection to analyze
    
    .PARAMETER SampleSize
    Number of documents to sample for analysis (default: 100)
    
    .EXAMPLE
    $config = Get-AppConfig
    Get-MongoDBSchema -ConnectionString $config.MongoDB.ConnectionString -DatabaseName $config.MongoDB.Database -CollectionName "users" -SampleSize 50
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [string]$ConnectionString,
        
        [Parameter(Mandatory=$true)]
        [string]$DatabaseName,
        
        [Parameter(Mandatory=$true)]
        [string]$CollectionName,
        
        [Parameter(Mandatory=$false)]
        [int]$SampleSize = 100
    )
    
    try {
        Write-Host "`n═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "    MongoDB Schema Analysis - $CollectionName" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
        
        # Connect to MongoDB
        Write-Host "Connecting to MongoDB..." -ForegroundColor Yellow
        Connect-Mdbc -ConnectionString $ConnectionString -DatabaseName $DatabaseName -CollectionName $CollectionName
        
        # Get total document count
        $totalDocs = Get-MdbcData -Count
        Write-Host "Total documents in collection: $totalDocs" -ForegroundColor Gray
        
        # Determine actual sample size
        $actualSampleSize = [Math]::Min($SampleSize, $totalDocs)
        Write-Host "Analyzing $actualSampleSize documents...`n" -ForegroundColor Gray
        
        # Get sample documents
        $documents = Get-MdbcData -Last $actualSampleSize
        
        # Initialize schema structure
        $schema = @{}
        
        # Analyze each document
        $docCount = 0
        foreach ($doc in $documents) {
            $docCount++
            Write-Progress -Activity "Analyzing documents" -Status "Document $docCount of $actualSampleSize" -PercentComplete (($docCount / $actualSampleSize) * 100)
            
            # Debug: Check document type
            Write-Host "DEBUG: Document type: $($doc.GetType().FullName)" -ForegroundColor DarkGray
            
            # Convert to proper object if needed
            if ($doc -is [MongoDB.Bson.BsonDocument]) {
                $doc = [MongoDB.Bson.BsonTypeMapper]::MapToDotNetValue($doc)
            }
            
            Analyze-DocumentStructure -Document $doc -Schema $schema -Path "" -TotalDocs $actualSampleSize
        }
        
        Write-Progress -Activity "Analyzing documents" -Completed
        
        # Generate and display results
        Write-Host "Schema Analysis Results:" -ForegroundColor Green
        Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
        
        Display-SchemaResults -Schema $schema -TotalDocs $actualSampleSize
        
        # Return schema object for further processing
        return $schema
    }
    catch {
        Write-Host "Error during schema analysis: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

function Analyze-DocumentStructure {
    <#
    .SYNOPSIS
    Recursively analyzes document structure and updates schema
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        $Document,
        
        [Parameter(Mandatory=$true)]
        [hashtable]$Schema,
        
        [Parameter(Mandatory=$false)]
        [string]$Path = "",
        
        [Parameter(Mandatory=$true)]
        [int]$TotalDocs
    )
    
    # Safety check
    if ($null -eq $Document) {
        Write-Host "WARNING: Null document encountered at path: $Path" -ForegroundColor Yellow
        return
    }
    
    # Get properties based on document type
    $properties = @()
    
    if ($Document -is [System.Collections.IDictionary]) {
        # This handles Mdbc.Dictionary, Hashtable, and other dictionary types
        $properties = $Document.GetEnumerator() | ForEach-Object {
            [PSCustomObject]@{
                Name = $_.Key
                Value = $_.Value
            }
        }
    }
    elseif ($Document -is [PSCustomObject]) {
        $properties = $Document.PSObject.Properties | ForEach-Object {
            [PSCustomObject]@{
                Name = $_.Name
                Value = $_.Value
            }
        }
    }
    else {
        Write-Host "WARNING: Unexpected document type: $($Document.GetType().FullName)" -ForegroundColor Yellow
        return
    }
    
    foreach ($property in $properties) {
        $fieldName = $property.Name
        $fieldValue = $property.Value
        
        # Skip MongoDB internal fields if desired (optional)
        # if ($fieldName -eq "_id") { continue }
        
        # Create full path for nested fields
        $fullPath = if ($Path) { "$Path.$fieldName" } else { $fieldName }
        
        # Initialize field in schema if not exists
        if (-not $Schema.ContainsKey($fullPath)) {
            $Schema[$fullPath] = @{
                Types = @{}
                Count = 0
                IsNested = $false
                IsArray = $false
                SampleValues = @()
                ArrayElementTypes = @{}
            }
        }
        
        # Increment occurrence count
        $Schema[$fullPath].Count++
        
        # Determine and record type
        $fieldType = Get-FieldType -Value $fieldValue
        
        if ($Schema[$fullPath].Types.ContainsKey($fieldType)) {
            $Schema[$fullPath].Types[$fieldType]++
        } else {
            $Schema[$fullPath].Types[$fieldType] = 1
        }
        
        # Handle different data types
        if ($null -eq $fieldValue) {
            # Null value - already counted in types
        }
        elseif ($fieldValue -is [System.Collections.IEnumerable] -and $fieldValue -isnot [string]) {
            # Array or collection
            $Schema[$fullPath].IsArray = $true
            
            foreach ($item in $fieldValue) {
                $itemType = Get-FieldType -Value $item
                
                if ($Schema[$fullPath].ArrayElementTypes.ContainsKey($itemType)) {
                    $Schema[$fullPath].ArrayElementTypes[$itemType]++
                } else {
                    $Schema[$fullPath].ArrayElementTypes[$itemType] = 1
                }
                
                # Recursively analyze nested objects in arrays
                if ($item -is [PSCustomObject] -or $item -is [System.Collections.Hashtable]) {
                    $Schema[$fullPath].IsNested = $true
                    Analyze-DocumentStructure -Document $item -Schema $Schema -Path "$fullPath[]" -TotalDocs $TotalDocs
                }
            }
        }
        elseif ($fieldValue -is [PSCustomObject] -or $fieldValue -is [System.Collections.Hashtable]) {
            # Nested object
            $Schema[$fullPath].IsNested = $true
            Analyze-DocumentStructure -Document $fieldValue -Schema $Schema -Path $fullPath -TotalDocs $TotalDocs
        }
        else {
            # Store sample values (limit to 3 unique samples)
            if ($Schema[$fullPath].SampleValues.Count -lt 3) {
                $valueStr = $fieldValue.ToString()
                if ($valueStr.Length -gt 50) {
                    $valueStr = $valueStr.Substring(0, 47) + "..."
                }
                if ($valueStr -notin $Schema[$fullPath].SampleValues) {
                    $Schema[$fullPath].SampleValues += $valueStr
                }
            }
        }
    }
}

function Get-FieldType {
    <#
    .SYNOPSIS
    Determines the data type of a field value
    #>
    
    param (
        $Value
    )
    
    if ($null -eq $Value) {
        return "null"
    }
    elseif ($Value -is [string]) {
        return "string"
    }
    elseif ($Value -is [int] -or $Value -is [int32] -or $Value -is [int64]) {
        return "integer"
    }
    elseif ($Value -is [double] -or $Value -is [float] -or $Value -is [decimal]) {
        return "number"
    }
    elseif ($Value -is [bool]) {
        return "boolean"
    }
    elseif ($Value -is [datetime]) {
        return "datetime"
    }
    elseif ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        return "array"
    }
    elseif ($Value -is [PSCustomObject] -or $Value -is [System.Collections.Hashtable]) {
        return "object"
    }
    else {
        return $Value.GetType().Name
    }
}

function Display-SchemaResults {
    <#
    .SYNOPSIS
    Displays the schema analysis results in a readable format
    #>
    
    param (
        [Parameter(Mandatory=$true)]
        [hashtable]$Schema,
        
        [Parameter(Mandatory=$true)]
        [int]$TotalDocs
    )
    
    # Check if schema is empty
    if ($Schema.Keys.Count -eq 0) {
        Write-Host "No fields found in the analyzed documents." -ForegroundColor Yellow
        return
    }
    
    # Sort fields by path
    $sortedFields = $Schema.Keys | Sort-Object
    
    foreach ($fieldPath in $sortedFields) {
        $fieldInfo = $Schema[$fieldPath]
        
        # Safety check
        if ($null -eq $fieldInfo -or $null -eq $fieldInfo.Types) {
            Write-Host "WARNING: Invalid field info for $fieldPath" -ForegroundColor Yellow
            continue
        }
        
        $percentage = [math]::Round(($fieldInfo.Count / $TotalDocs) * 100, 1)
        
        # Field name and occurrence
        Write-Host "Field: " -NoNewline -ForegroundColor Cyan
        Write-Host $fieldPath -ForegroundColor White
        Write-Host "  Occurrence: $($fieldInfo.Count)/$TotalDocs ($percentage%)" -ForegroundColor Gray
        
        # Types
        Write-Host "  Types: " -NoNewline -ForegroundColor Gray
        $typeStrings = $fieldInfo.Types.GetEnumerator() | ForEach-Object {
            "$($_.Key) ($($_.Value))"
        }
        Write-Host ($typeStrings -join ", ") -ForegroundColor Yellow
        
        # Array information
        if ($fieldInfo.IsArray) {
            Write-Host "  Array Element Types: " -NoNewline -ForegroundColor Gray
            $arrayTypeStrings = $fieldInfo.ArrayElementTypes.GetEnumerator() | ForEach-Object {
                "$($_.Key) ($($_.Value))"
            }
            Write-Host ($arrayTypeStrings -join ", ") -ForegroundColor Magenta
        }
        
        # Nested indicator
        if ($fieldInfo.IsNested) {
            Write-Host "  [NESTED STRUCTURE]" -ForegroundColor Red
        }
        
        # Sample values
        if ($fieldInfo.SampleValues.Count -gt 0) {
            Write-Host "  Samples: " -NoNewline -ForegroundColor Gray
            Write-Host ($fieldInfo.SampleValues -join " | ") -ForegroundColor DarkGray
        }
        
        Write-Host ""
    }
    
    # Summary statistics
    Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "Summary:" -ForegroundColor Green
    Write-Host "  Total unique fields: $($Schema.Keys.Count)" -ForegroundColor Gray
    
    $nestedFields = ($Schema.Values | Where-Object { $_.IsNested }).Count
    $arrayFields = ($Schema.Values | Where-Object { $_.IsArray }).Count
    
    Write-Host "  Nested structures: $nestedFields" -ForegroundColor Gray
    Write-Host "  Array fields: $arrayFields" -ForegroundColor Gray
    Write-Host "═══════════════════════════════════════════════════════`n" -ForegroundColor Cyan
}

# Example usage function
function Test-SchemaAnalysis {
    <#
    .SYNOPSIS
    Test function to run schema analysis with config.json
    #>
    
    # Load configuration
    $config = Get-AppConfig
    
    # Run analysis
    $schema = Get-MongoDBSchema -ConnectionString $config.MongoDB.ConnectionString `
                                -DatabaseName $config.MongoDB.Database `
                                -CollectionName $config.MongoDB.Collection `
                                -SampleSize 100
    
    return $schema
}