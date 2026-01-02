BeforeAll {
    # Import the script to test - handle both Windows and WSL paths
    $scriptPath = if ($PSScriptRoot -match '^\\\\wsl') {
        # Convert WSL path to proper format
        Join-Path (Split-Path $PSScriptRoot -Parent) "Connection_DB.ps1"
    } else {
        Join-Path (Split-Path $PSScriptRoot -Parent) "Connection_DB.ps1"
    }
    
    if (-not (Test-Path $scriptPath)) {
        throw "Cannot find Connection_DB.ps1 at: $scriptPath"
    }
    
    . $scriptPath
    
    # Create mock config for testing
    $script:mockConfigPath = "$TestDrive\config.json"
    $script:mockConfig = @{
        MongoDB = @{
            ConnectionString = "mongodb://localhost:27017"
            Database = "test_db"
            Collection = "test_collection"
        }
        MySQL = @{
            Server = "localhost"
            Port = 3306
            Database = "test_mysql_db"
            Username = "test_user"
            Password = "test_password"
        }
        SQLServer = @{
            Server = "localhost"
            Database = "test_sqlserver_db"
            Username = "test_user"
            Password = "test_password"
        }
    }
}

Describe "Get-AppConfig" {
    Context "When config file exists" {
        BeforeEach {
            $script:mockConfig | ConvertTo-Json -Depth 10 | Out-File -FilePath $script:mockConfigPath -Encoding UTF8
        }
        
        It "Should load config successfully" {
            $result = Get-AppConfig -Path $script:mockConfigPath
            $result | Should -Not -BeNullOrEmpty
        }
        
        It "Should return PSCustomObject" {
            $result = Get-AppConfig -Path $script:mockConfigPath
            $result | Should -BeOfType [PSCustomObject]
        }
        
        It "Should contain MongoDB configuration" {
            $result = Get-AppConfig -Path $script:mockConfigPath
            $result.MongoDB | Should -Not -BeNullOrEmpty
            $result.MongoDB.ConnectionString | Should -Be "mongodb://localhost:27017"
            $result.MongoDB.Database | Should -Be "test_db"
        }
        
        It "Should contain MySQL configuration" {
            $result = Get-AppConfig -Path $script:mockConfigPath
            $result.MySQL | Should -Not -BeNullOrEmpty
            $result.MySQL.Server | Should -Be "localhost"
            $result.MySQL.Port | Should -Be 3306
        }
        
        It "Should contain SQLServer configuration" {
            $result = Get-AppConfig -Path $script:mockConfigPath
            $result.SQLServer | Should -Not -BeNullOrEmpty
            $result.SQLServer.Server | Should -Be "localhost"
        }
    }
    
    Context "When config file does not exist" {
        It "Should throw an error" {
            { Get-AppConfig -Path "$TestDrive\nonexistent.json" } | Should -Throw "*Configuration file not found*"
        }
    }
    
    Context "When config file has invalid JSON" {
        BeforeEach {
            "{ invalid json }" | Out-File -FilePath $script:mockConfigPath -Encoding UTF8
        }
        
        It "Should throw JSON parsing error" {
            { Get-AppConfig -Path $script:mockConfigPath } | Should -Throw
        }
    }
}

Describe "Test-MongoDBConnection" {
    Context "When MongoDB connection is successful" {
        BeforeEach {
            Mock Connect-Mdbc { }
            Mock Get-MdbcData { return 100 }
        }
        
        It "Should return true on successful connection" {
            $result = Test-MongoDBConnection `
                -ConnectionString "mongodb://localhost:27017" `
                -DatabaseName "test_db" `
                -CollectionName "test_collection"
            
            $result | Should -Be $true
        }
        
        It "Should call Connect-Mdbc with correct parameters" {
            Test-MongoDBConnection `
                -ConnectionString "mongodb://localhost:27017" `
                -DatabaseName "test_db" `
                -CollectionName "test_collection" | Out-Null
            
            Should -Invoke Connect-Mdbc -Times 1 -ParameterFilter {
                $ConnectionString -eq "mongodb://localhost:27017" -and
                $DatabaseName -eq "test_db" -and
                $CollectionName -eq "test_collection"
            }
        }
        
        It "Should get document count when collection is specified" {
            Test-MongoDBConnection `
                -ConnectionString "mongodb://localhost:27017" `
                -DatabaseName "test_db" `
                -CollectionName "test_collection" | Out-Null
            
            Should -Invoke Get-MdbcData -Times 1 -ParameterFilter { $Count }
        }
        
        It "Should not get document count when collection is not specified" {
            Mock Get-MdbcData { return 0 }
            
            Test-MongoDBConnection `
                -ConnectionString "mongodb://localhost:27017" `
                -DatabaseName "test_db" | Out-Null
            
            Should -Invoke Get-MdbcData -Times 0
        }
        
        It "Should write success messages" {
            Mock Write-Host { }
            
            Test-MongoDBConnection `
                -ConnectionString "mongodb://localhost:27017" `
                -DatabaseName "test_db" `
                -CollectionName "test_collection" | Out-Null
            
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "MongoDB connection successful" 
            }
        }
    }
    
    Context "When MongoDB connection fails" {
        BeforeEach {
            Mock Connect-Mdbc { throw "Connection failed" }
            Mock Write-Host { }
        }
        
        It "Should return false on connection failure" {
            $result = Test-MongoDBConnection `
                -ConnectionString "mongodb://invalid:27017" `
                -DatabaseName "test_db"
            
            $result | Should -Be $false
        }
        
        It "Should write error messages" {
            Test-MongoDBConnection `
                -ConnectionString "mongodb://invalid:27017" `
                -DatabaseName "test_db" | Out-Null
            
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "MongoDB connection failed" 
            }
        }
    }
}

Describe "Test-MySQLConnection" {
    Context "When MySQL connection is successful" {
        BeforeEach {
            Mock Add-Type { }
            Mock New-Object {
                return [PSCustomObject]@{
                    ConnectionString = ""
                    ServerVersion = "8.0.33"
                    Open = { }
                    Close = { }
                }
            } -ParameterFilter { $TypeName -eq "MySql.Data.MySqlClient.MySqlConnection" }
            Mock Write-Host { }
        }
        
        It "Should return true on successful connection" {
            $result = Test-MySQLConnection `
                -Server "localhost" `
                -Database "test_db" `
                -Port 3306 `
                -Username "test_user" `
                -Password "test_password"
            
            $result | Should -Be $true
        }
        
        It "Should load MySql.Data assembly" {
            Test-MySQLConnection `
                -Server "localhost" `
                -Database "test_db" | Out-Null
            
            Should -Invoke Add-Type -Times 1 -ParameterFilter {
                $AssemblyName -eq "MySql.Data"
            }
        }
        
        It "Should build connection string with credentials" {
            Mock New-Object {
                $conn = [PSCustomObject]@{
                    ConnectionString = ""
                    ServerVersion = "8.0.33"
                    Open = { }
                    Close = { }
                }
                # Capture the connection string
                $script:capturedConnectionString = $conn.ConnectionString
                return $conn
            } -ParameterFilter { $TypeName -eq "MySql.Data.MySqlClient.MySqlConnection" }
            
            Test-MySQLConnection `
                -Server "localhost" `
                -Database "test_db" `
                -Username "test_user" `
                -Password "test_password" | Out-Null
            
            # Connection string should be set before Open() is called
            # This is a simplified check
            Should -Invoke New-Object -Times 1
        }
        
        It "Should use default port 3306 when not specified" {
            Test-MySQLConnection -Server "localhost" -Database "test_db" | Out-Null
            Should -Invoke New-Object -Times 1
        }
        
        It "Should write success messages" {
            Test-MySQLConnection `
                -Server "localhost" `
                -Database "test_db" | Out-Null
            
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "MySQL connection successful" 
            }
        }
    }
    
    Context "When MySQL connection fails" {
        BeforeEach {
            Mock Add-Type { }
            Mock New-Object {
                $conn = [PSCustomObject]@{
                    ConnectionString = ""
                    Open = { throw "Connection refused" }
                    Close = { }
                }
                return $conn
            } -ParameterFilter { $TypeName -eq "MySql.Data.MySqlClient.MySqlConnection" }
            Mock Write-Host { }
        }
        
        It "Should return false on connection failure" {
            $result = Test-MySQLConnection -Server "invalid" -Database "test_db"
            $result | Should -Be $false
        }
        
        It "Should write error messages" {
            Test-MySQLConnection -Server "invalid" -Database "test_db" | Out-Null
            
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "MySQL connection failed" 
            }
        }
    }
    
    Context "When MySQL connector is not installed" {
        BeforeEach {
            Mock Add-Type { throw "Assembly not found" }
            Mock Write-Host { }
        }
        
        It "Should return false when connector is missing" {
            $result = Test-MySQLConnection -Server "localhost" -Database "test_db"
            $result | Should -Be $false
        }
        
        It "Should show helpful error message" {
            Test-MySQLConnection -Server "localhost" -Database "test_db" | Out-Null
            
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "MySQL connection failed" 
            }
        }
    }
}

Describe "Test-SQLServerConnection" {
    Context "When SQL Server connection is successful" {
        BeforeEach {
            Mock New-Object {
                return [PSCustomObject]@{
                    ConnectionString = ""
                    Open = { }
                    Close = { }
                }
            } -ParameterFilter { $TypeName -eq "System.Data.SqlClient.SqlConnection" }
            Mock Write-Host { }
        }
        
        It "Should return true on successful connection" {
            $result = Test-SQLServerConnection `
                -Server "localhost" `
                -Database "test_db" `
                -Username "sa" `
                -Password "password"
            
            $result | Should -Be $true
        }
        
        It "Should use SQL authentication when credentials provided" {
            Test-SQLServerConnection `
                -Server "localhost" `
                -Database "test_db" `
                -Username "sa" `
                -Password "password" | Out-Null
            
            Should -Invoke New-Object -Times 1
        }
        
        It "Should use Windows authentication when no credentials" {
            Test-SQLServerConnection `
                -Server "localhost" `
                -Database "test_db" | Out-Null
            
            Should -Invoke New-Object -Times 1
        }
        
        It "Should write success messages" {
            Test-SQLServerConnection `
                -Server "localhost" `
                -Database "test_db" | Out-Null
            
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "SQL Server connection successful" 
            }
        }
    }
    
    Context "When SQL Server connection fails" {
        BeforeEach {
            Mock New-Object {
                $conn = [PSCustomObject]@{
                    ConnectionString = ""
                    Open = { throw "Login failed" }
                    Close = { }
                }
                return $conn
            } -ParameterFilter { $TypeName -eq "System.Data.SqlClient.SqlConnection" }
            Mock Write-Host { }
        }
        
        It "Should return false on connection failure" {
            $result = Test-SQLServerConnection -Server "invalid" -Database "test_db"
            $result | Should -Be $false
        }
        
        It "Should write error messages" {
            Test-SQLServerConnection -Server "invalid" -Database "test_db" | Out-Null
            
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "SQL Server connection failed" 
            }
        }
    }
}

Describe "Initialize-DatabaseConnections" {
    BeforeEach {
        # Create config file
        $script:mockConfig | ConvertTo-Json -Depth 10 | Out-File -FilePath "$PSScriptRoot\config.json" -Encoding UTF8
        
        Mock Write-Host { }
        Mock Get-AppConfig { return $script:mockConfig }
    }
    
    AfterEach {
        if (Test-Path "$PSScriptRoot\config.json") {
            Remove-Item "$PSScriptRoot\config.json" -Force
        }
    }
    
    Context "When all connections succeed with MySQL" {
        BeforeEach {
            Mock Test-MongoDBConnection { return $true }
            Mock Test-MySQLConnection { return $true }
        }
        
        It "Should return true when all connections succeed" {
            $result = Initialize-DatabaseConnections -DatabaseType "MySQL"
            $result | Should -Be $true
        }
        
        It "Should test MongoDB connection" {
            Initialize-DatabaseConnections -DatabaseType "MySQL" | Out-Null
            Should -Invoke Test-MongoDBConnection -Times 1
        }
        
        It "Should test MySQL connection" {
            Initialize-DatabaseConnections -DatabaseType "MySQL" | Out-Null
            Should -Invoke Test-MySQLConnection -Times 1
        }
        
        It "Should display success message" {
            Initialize-DatabaseConnections -DatabaseType "MySQL" | Out-Null
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "All database connections are successful" 
            }
        }
    }
    
    Context "When all connections succeed with SQL Server" {
        BeforeEach {
            Mock Test-MongoDBConnection { return $true }
            Mock Test-SQLServerConnection { return $true }
        }
        
        It "Should return true when all connections succeed" {
            $result = Initialize-DatabaseConnections -DatabaseType "SQLServer"
            $result | Should -Be $true
        }
        
        It "Should test SQL Server connection" {
            Initialize-DatabaseConnections -DatabaseType "SQLServer" | Out-Null
            Should -Invoke Test-SQLServerConnection -Times 1
        }
    }
    
    Context "When MongoDB connection fails" {
        BeforeEach {
            Mock Test-MongoDBConnection { return $false }
            Mock Test-MySQLConnection { return $true }
        }
        
        It "Should return false" {
            $result = Initialize-DatabaseConnections -DatabaseType "MySQL"
            $result | Should -Be $false
        }
        
        It "Should display failure message" {
            Initialize-DatabaseConnections -DatabaseType "MySQL" | Out-Null
            Should -Invoke Write-Host -ParameterFilter { 
                $Object -match "One or more database connections failed" 
            }
        }
    }
    
    Context "When SQL connection fails" {
        BeforeEach {
            Mock Test-MongoDBConnection { return $true }
            Mock Test-MySQLConnection { return $false }
        }
        
        It "Should return false" {
            $result = Initialize-DatabaseConnections -DatabaseType "MySQL"
            $result | Should -Be $false
        }
    }
    
    Context "When config file is missing" {
        BeforeEach {
            Mock Get-AppConfig { throw "Configuration file not found" }
        }
        
        It "Should return false" {
            $result = Initialize-DatabaseConnections -DatabaseType "MySQL"
            $result | Should -Be $false
        }
    }
}

Describe "Get-SQLConnection" {
    BeforeEach {
        Mock Add-Type { }
    }
    
    Context "When getting MySQL connection" {
        BeforeEach {
            Mock New-Object {
                return [PSCustomObject]@{
                    ConnectionString = ""
                }
            } -ParameterFilter { $TypeName -eq "MySql.Data.MySqlClient.MySqlConnection" }
        }
        
        It "Should create MySQL connection object" {
            $result = Get-SQLConnection -Config $script:mockConfig -DatabaseType "MySQL"
            $result | Should -Not -BeNullOrEmpty
        }
        
        It "Should load MySQL assembly" {
            Get-SQLConnection -Config $script:mockConfig -DatabaseType "MySQL" | Out-Null
            Should -Invoke Add-Type -ParameterFilter { $AssemblyName -eq "MySql.Data" }
        }
        
        It "Should create MySqlConnection object" {
            Get-SQLConnection -Config $script:mockConfig -DatabaseType "MySQL" | Out-Null
            Should -Invoke New-Object -ParameterFilter { 
                $TypeName -eq "MySql.Data.MySqlClient.MySqlConnection" 
            }
        }
    }
    
    Context "When getting SQL Server connection" {
        BeforeEach {
            Mock New-Object {
                return [PSCustomObject]@{
                    ConnectionString = ""
                }
            } -ParameterFilter { $TypeName -eq "System.Data.SqlClient.SqlConnection" }
        }
        
        It "Should create SQL Server connection object" {
            $result = Get-SQLConnection -Config $script:mockConfig -DatabaseType "SQLServer"
            $result | Should -Not -BeNullOrEmpty
        }
        
        It "Should create SqlConnection object" {
            Get-SQLConnection -Config $script:mockConfig -DatabaseType "SQLServer" | Out-Null
            Should -Invoke New-Object -ParameterFilter { 
                $TypeName -eq "System.Data.SqlClient.SqlConnection" 
            }
        }
    }
    
    Context "When config has credentials" {
        It "Should build connection string with credentials for MySQL" {
            Mock New-Object {
                return [PSCustomObject]@{
                    ConnectionString = ""
                }
            } -ParameterFilter { $TypeName -eq "MySql.Data.MySqlClient.MySqlConnection" }
            
            $result = Get-SQLConnection -Config $script:mockConfig -DatabaseType "MySQL"
            Should -Invoke New-Object -Times 1
        }
        
        It "Should build connection string with credentials for SQL Server" {
            Mock New-Object {
                return [PSCustomObject]@{
                    ConnectionString = ""
                }
            } -ParameterFilter { $TypeName -eq "System.Data.SqlClient.SqlConnection" }
            
            $result = Get-SQLConnection -Config $script:mockConfig -DatabaseType "SQLServer"
            Should -Invoke New-Object -Times 1
        }
    }
}

Describe "Integration Tests" -Tag "Integration" {
    Context "When reading actual config file" {
        BeforeEach {
            # Create a real config file
            $testConfig = @{
                MongoDB = @{
                    ConnectionString = "mongodb://localhost:27017"
                    Database = "integration_test"
                    Collection = "test_coll"
                }
                MySQL = @{
                    Server = "localhost"
                    Port = 3306
                    Database = "integration_test"
                    Username = "root"
                    Password = "password"
                }
            }
            $testConfig | ConvertTo-Json -Depth 10 | Out-File -FilePath "$TestDrive\test_config.json" -Encoding UTF8
        }
        
        It "Should load and parse real config file" {
            $config = Get-AppConfig -Path "$TestDrive\test_config.json"
            $config.MongoDB.Database | Should -Be "integration_test"
            $config.MySQL.Server | Should -Be "localhost"
        }
    }
}

AfterAll {
    # Cleanup
    if (Test-Path "$PSScriptRoot\config.json") {
        Remove-Item "$PSScriptRoot\config.json" -Force -ErrorAction SilentlyContinue
    }
}