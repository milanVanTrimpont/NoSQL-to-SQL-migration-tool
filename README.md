# NoSQL-to-SQL Migration Tool

A PowerShell module that migrates documents from MongoDB to a relational MySQL
database and then keeps both sides synchronised.

Developed for the Applied Computer Science program (3rd year, Scripting).

## What it does

* **Schema discovery** — analyses a collection and generates `CREATE TABLE`
  statements, including child tables for arrays and sub-documents.
* **Tolerant conversion** — a field that holds a date in one document and text in
  the next does not cost you a document. Values are converted per column, and a
  value that truly does not fit is reported instead of silently dropped.
* **Incremental sync** — detects new, changed and deleted documents through a hash
  of the whole document, and repairs child rows that were removed in SQL.
* **Validation** — compares record counts and validates sampled documents field by
  field.
* **Two ways in** — an interactive menu, and a non-interactive entry point with
  exit codes for Task Scheduler or a pipeline.

**Target database:** MySQL. The configuration also has a `SQLServer` section and
connecting to SQL Server works, but the read and write statements use MySQL
syntax (`SHOW TABLES`, backtick quoting), so migrating to SQL Server is not
supported yet.

---

## Requirements

| What | Version | Why |
|---|---|---|
| PowerShell | 7.0 or higher | the module targets PowerShell Core |
| MongoDB | any recent | source database |
| MySQL | 8.x | target database |
| `Mdbc` module | 6 or higher | MongoDB access from PowerShell |
| MySQL Connector/NET | 8.x or 9.x | the `MySql.Data.dll` driver |
| `Pester` module | 5.0 or higher | only needed to run the tests |

Docker is the easiest way to get the two databases, but a local installation
works just as well — only the values in `config.json` change.

---

## Setup on a new machine

### 1. Clone and enter the project

```powershell
git clone https://github.com/milanVanTrimpont/NoSQL-to-SQL-migration-tool.git
cd NoSQL-to-SQL-migration-tool
```

### 2. Install the PowerShell dependencies

```powershell
Install-Module Mdbc -Scope CurrentUser -Force
Install-Module Pester -MinimumVersion 5.0 -Scope CurrentUser -Force
```

### 3. Install MySQL Connector/NET

Download it from <https://dev.mysql.com/downloads/connector/net/> and install it.
The module searches both Program Files folders for `MySql.Data.dll` and takes the
newest version it finds, so any version works.

No installer available? Put the DLL in a `lib` folder inside the module folder
instead: `NoSqlToSqlMigration\lib\MySql.Data.dll`. That path is checked first.

### 4. Start the databases

With Docker:

```powershell
docker run -d --name mongo-container -p 27017:27017 mongo:7
```

```powershell
docker run -d --name mysql-container -p 3307:3306 -e MYSQL_ROOT_PASSWORD=YourPassword -e MYSQL_DATABASE=mijn_database mysql:8
```

Note the port: MySQL is published on **3307** here so it does not clash with a
local MySQL on 3306. Any port works, as long as `config.json` matches.

### 5. Create the configuration

```powershell
Copy-Item config.example.json config.json
```

Open `config.json` and fill in your own server, port, database and password.

| Setting | Meaning |
|---|---|
| `MongoDB.ConnectionString` | for example `mongodb://localhost:27017` |
| `MongoDB.Database` | the database holding the collections |
| `MySQL.Server` / `Port` | `127.0.0.1` and `3307` for the container above |
| `MySQL.Database` | must already exist; the tool creates tables, not databases |
| `Migration.BatchSize` | documents per batch during a migration |
| `Migration.OnConversionError` | `Warn` (default), `Skip` or `Fail`, see below |

### 6. Put something in MongoDB to migrate

Skip this if you already have data. This creates a collection with a long text,
two arrays, a sub-document and a date written in two different ways — the cases
worth testing:

```powershell
Import-Module Mdbc; Connect-Mdbc -ConnectionString "mongodb://localhost:27017" -DatabaseName "ScriptingPS" -CollectionName "films" -NewCollection; @( @{ title = "Heat"; storyline = ("plot " * 120); genres = @("Crime","Drama"); ratings = @(8,9,10); director = @{ name = "Michael Mann"; born = 1943 }; released = [datetime]"1995-12-15" }, @{ title = "Alien"; storyline = "short"; genres = @("Horror"); ratings = @(9); director = @{ name = "Ridley Scott"; born = 1937 }; released = "25/05/1979" } ) | ForEach-Object { Add-MdbcData $_ }
```

### 7. Check the connections

Import the module and open the menu:

```powershell
Import-Module .\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1 -Force
```

```powershell
Start-MigrationToolMenu
```

Choose **[1] Test Database Connections**. Both lines must report success before
anything else will work.

### 8. Migrate

Choose **[3] Migrate Single Collection**, pick the collection, and use a sample
size at least as large as the number of documents so no field is missed. Check
the result afterwards with **[8] Validate Single Collection**.

---

## Usage

### Interactive menu

Import the module once per session, then call the menu:

```powershell
Import-Module .\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1 -Force
```

```powershell
Start-MigrationToolMenu
```

Or let the launcher do both steps, handy for a shortcut or a fresh window:

```powershell
pwsh -File .\InteractiveMenu.ps1
```

| Option | What it does |
|---|---|
| 1 | test the MongoDB and MySQL connections |
| 2 | list the collections in MongoDB |
| 3, 4, 5 | full migration of one, several or all collections |
| 6, 7 | incremental sync of one or all collections |
| 8 | validate a collection against MongoDB |
| 9 | analyse the schema only, write nothing |
| 10 | drop tables that have nothing behind them in MongoDB anymore |

Option 10 asks twice: you type `YES`, and then confirm each table separately. The
number of rows is shown, because dropping cannot be undone.

### Without a menu, for a scheduled task

```powershell
pwsh -File .\Start-Migration.ps1 -Operation IncrementalSync
```

It asks nothing and ends with an exit code:

| Code | Meaning |
|---|---|
| 0 | everything finished correctly |
| 1 | at least one collection failed |
| 2 | the run could not start, for example a bad configuration |

Useful variations:

```powershell
pwsh -File .\Start-Migration.ps1 -Collections films,users -Operation FullMigration -SampleSize 500
```

```powershell
pwsh -File .\Start-Migration.ps1 -Operation IncrementalSync -Quiet
```

```powershell
pwsh -File .\Start-Migration.ps1 -Operation IncrementalSync *> .\migration.log
```

Because the module reports through the regular PowerShell streams, you can keep
exactly what you need: `-Quiet` for silence, `6>>` for a progress log, `3>` for
warnings, `2>` for errors, `-Verbose` for detail.

### From your own script

```powershell
Import-Module .\NoSqlToSqlMigration\NoSqlToSqlMigration.psd1 -Force
$result = Invoke-N2SMigration -Collections films -Operation FullMigration -SampleSize 500
exit $result.ExitCode
```

The result object also holds `TotalSuccess`, `TotalFailed`, `TotalWarnings`,
`OrphanTables` and the details per collection. Every run writes the same
information to `workflow_report_<timestamp>.json`.

---

## How conversion errors are handled

MongoDB has no schema, so the same field can hold different types. The column
type is therefore based on **all** observed types: a field with dates as well as
text becomes a text column, so no value is excluded up front.

While writing, every value is converted to the type of its column. Dates are read
in twelve notations (day-first before month-first, so `06/05/2022` is 6 May), and
numbers are read culture independent, so `8,6` also arrives as `8.6`.

A value that still does not fit is handled according to
`Migration.OnConversionError`:

| Setting | What happens |
|---|---|
| `Warn` (default) | the field is stored as NULL, the document is kept, the problem is recorded |
| `Skip` | the document is not migrated and is recorded |
| `Fail` | the migration stops at the first value that does not fit |

Whatever the setting, every problem ends up in
`conversion_report_<collection>_<timestamp>.csv` with the table, document, field,
reason and the action taken. Nothing disappears without a trace.

---

## Tests

```powershell
pwsh -File .\Tests\Invoke-Tests.ps1
```

116 tests, a few seconds, **no database needed** — the database calls are mocked.
The runner loads Pester 5 or newer itself, because a Pester 3 installation next to
it would otherwise make the test files fail.

Tests that do need a live MongoDB and MySQL are tagged and skipped by default:

```powershell
pwsh -File .\Tests\Invoke-Tests.ps1 -Integration -Detailed
```

They create their own `n2s_test_*` collection and tables and remove them
afterwards.

---

## Structure

```
NoSqlToSqlMigration/NoSqlToSqlMigration.psm1   all logic, in one file
NoSqlToSqlMigration/NoSqlToSqlMigration.psd1   module manifest
InteractiveMenu.ps1                            launcher for the menu
Start-Migration.ps1                            entry point without a menu
config.example.json                            template for config.json
Tests/                                         Pester tests and runner
```

The module is deliberately a single file, but there are three layers inside it:

1. **Core** — analysis, schema, migration, sync, validation. Takes parameters,
   returns objects, reports through `Write-N2SMessage`. No `Write-Host`, and it
   never asks a question.
2. **Presentation** — `Show-*` functions that turn results into console output.
3. **Interaction** — the menu, the only place that asks questions.

That separation is what makes the same code usable from a menu and from a
scheduled task. `Set-N2SOutputMode` decides how the core reports: `Console` for
the menu, `Stream` for automated runs.

---

## Troubleshooting

**`Cannot find type [MySql.Data.MySqlClient.MySqlConnection]`**
MySQL Connector/NET is not installed, or the DLL is somewhere the module does not
look. Install the connector, or copy `MySql.Data.dll` into
`NoSqlToSqlMigration\lib\`.

**`BeforeAll may only be used inside a Describe block`**
Pester 3 was loaded instead of Pester 5. Use `Tests\Invoke-Tests.ps1`, which picks
the right version.

**`PowerShell is in NonInteractive mode`** while dropping tables
A confirmation cannot be answered in an automated run. Use
`Invoke-N2SMigration -RemoveOrphanTables`, where the switch itself counts as the
confirmation, or add `-Confirm:$false`.

**`Cannot delete or update a parent row: a foreign key constraint fails`**
You are deleting a row from a main table while a child table still refers to it.
Delete the child rows first, or let a sync do it.

**A field is missing from the SQL table**
The schema comes from a sample. Run again with a `-SampleSize` at least as large
as the collection, so every field is seen.

**The sync says FULL SYNC every time**
The `sync_state_<collection>.json` file is missing, so there is nothing to compare
against. Not an error, only slower.

---

## Known limitations

* A **changed value inside an existing child row** is not detected by a sync when
  the number of rows still matches. Changes in MongoDB are caught by the document
  hash; the row count check exists to catch changes made directly in SQL.
* A **new array field** has no child table yet. The sync reports it; a full
  migration creates the table.
* **SQL Server** is not supported as a migration target, see above.
* The **schema is based on a sample**. With a sample smaller than the collection a
  field can be missed, and then its column will not exist.

---

## Sources

The following sources were used during development:

**Claude and ChatGPT** for the data migration, validation and sync
**GitHub Copilot** for error handling

**DB connection:**
<https://medium.com/@kavindra.mpez/database-automation-powershell-connectivity-with-mysql-ado-net-provider-powershell-cmdlets-b1c4f528eeab>

**Pester tests:** <https://pester.dev/docs/quick-start> and
<https://www.youtube.com/watch?v=iWbemnUpGx4>

**Output streams:**
<https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_output_streams>

**ShouldProcess, the confirmation before dropping a table:**
<https://learn.microsoft.com/en-us/powershell/scripting/learn/deep-dives/everything-about-shouldprocess>

**Extra:** <https://learn.microsoft.com/en-us/powershell/> and
<https://learn.microsoft.com/en-us/powershell/scripting/developer/module/how-to-write-a-powershell-module-manifest>
