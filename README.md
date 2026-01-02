# NoSQL-to-SQL Migration Tool

An advanced PowerShell module designed to automate ETL (Extract, Transform, Load) processes from unstructured data (MongoDB) to relational databases (MySQL & SQL Server).

This project was developed as part of the Applied Computer Science program (3rd year Scripting).

## Project Goal

The primary goal is to bridge the gap between NoSQL and SQL data structures. Manual migrations are prone to errors, especially when dealing with nested JSON data. This tool fully automates this process.

**Key Features:**
* **Schema Discovery:** Analyzes MongoDB collections and automatically generates SQL `CREATE TABLE` scripts.
* **Data Flattening:** Converts nested objects and arrays into flat relational columns.
* **Incremental Sync:** Detects changes (New, Updated, Deleted) using hashing and synchronizes only the differences.
* **Validation:** Verifies record counts after migration and validates data integrity via sampling.
* **Interactive Menu:** A user-friendly text-based interface to manage migrations.

## Requirements

Ensure your environment meets the following requirements:

* **Operating System:** Windows 10/11 or Linux (via WSL).
* **PowerShell:** Version 7.0 or higher (PowerShell Core).
* **Docker Desktop:** Required to run the database containers.
* **Dependencies (Modules & Drivers):**
    * PowerShell Module: `Mdbc` (for MongoDB connection).
      ```powershell
      Install-Module -Name Mdbc -Force
      ```
    * MySQL Connector/NET (DLL): The module attempts to load this automatically, but installing the MySQL Connector on the host is recommended.

## Installation & Setup

Follow these steps to set up the project correctly.

### 1. Clone Repository
```powershell
git clone [https://github.com/milanVanTrimpont/NoSQL-to-SQL-migration-tool.git](https://github.com/milanVanTrimpont/NoSQL-to-SQL-migration-tool.git)
cd NoSQL-to-SQL-migration-tool
```
### 3 Create Configuration (config.json)
The tool will not work without a configuration file. <br />
1.Create a new file named config.json in the root directory of the project (next to this README.md).

2.Paste the content below and adjust where necessary: (make sure to change the data to your databases) 
```json
{
  "MongoDB": {
    "ConnectionString": "mongodb://localhost:27017",
    "Database": "ScriptingPS"
  },
  "MySQL": {
    "Server": "127.0.0.1",
    "Port": 3307,
    "Database": "mijn_database",
    "Username": "root",
    "Password": "YourStrong@NewPassword"
  },
  "SQLServer": {
    "Server": ".\\SQLEXPRESS",
    "Database": "TargetDB",
    "Username": "sa",
    "Password": "Password123"
  },
  "Migration": {
    "BatchSize": 100
  }
}
```

### 4. Import Module
Import the module from the project root:
```powershell
Import-Module ".\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1" -Force
```
## usage
There are two ways to use the tool.<br />

__Option A: Interactive Menu (Recommended)__
Start the visual interface to test connections, discover collections, and run migrations.<br />
```powershell
Start-MigrationToolMenu
```
__Option B: Automated (CLI)__
For use in scripts or pipelines, you can call the workflow directly.<br />
__Example 1: Full Migration__
```powershell
Invoke-MigrationWorkflow -Operation FullMigration -DatabaseType MySQL -Verbose
```
__Example 2: Incremental Synchronization (Changes Only) __
Example 1: Full Migration<br />
```powershell
Invoke-MigrationWorkflow -Operation IncrementalSync -DatabaseType MySQL
```
## Architecture
__NoSqlToSqlMigration/:__ The actual module folder.
- NoSqlToSqlMigration.psd1: The module manifest.
- NoSqlToSqlMigration.psm1: Contains all logic (Connection, Analysis, Transformation, Validation, and Menu).
The code is modularized within a single file:<br />
1. Extract: Get-MdbcData retrieves data from MongoDB.<br />
2. Analyze: Get-MongoDBSchema determines data types and nested structures.<br />
3. Transform: Convert-ToSQLValue flattens objects and converts types (e.g., ObjectId -> String).<br />
4. Load: Invoke-InsertDocument writes data to SQL via bulk operations or transactions.<br />

## Sources
The following sources were used during development: <br />
Claude and Chatgpt for the datamigration, validation and sync <br />
__github copilot__ for errorhandeling<br />

__DB connection:__ https://medium.com/@kavindra.mpez/database-automation-powershell-connectivity-with-mysql-ado-net-provider-powershell-cmdlets-b1c4f528eeab
__Pester test:__ <br />
https://pester.dev/docs/quick-start <br />
https://www.youtube.com/watch?v=iWbemnUpGx4<br />

