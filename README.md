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
* **Housekeeping** — reports tables whose collection is gone, and child tables
  whose field disappeared from every document, and drops them only when asked.
* **Built for volume** — rows go in as multi-row statements inside a transaction
  per batch, so a migration costs a handful of round trips instead of one per row.
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

Choose **[3] Migrate Single Collection** and pick the collection. It then asks how
many documents to analyse for the schema and suggests the whole collection —
press enter to accept. A field that appears only outside the sample gets no
column, so analysing everything is the safe answer.

Check the result afterwards with **[8] Validate Single Collection**.

---

## Usage

### Interactive menu

Started as in step 7 of the setup: import the module, then `Start-MigrationToolMenu`.
Or let `pwsh -File .\InteractiveMenu.ps1` do both in one go.

| Option | What it does |
|---|---|
| 1 | test the MongoDB and MySQL connections |
| 2 | list the collections in MongoDB |
| 3, 4, 5 | full migration of one, several or all collections; asks how many documents to analyse |
| 6, 7 | incremental sync of one or all collections |
| 8 | validate a collection against MongoDB |
| 9 | analyse the schema only, write nothing; asks how many documents to analyse |
| 10 | drop tables that have nothing behind them in MongoDB anymore |

Option 10 finds two kinds of leftover table, and shows which kind each one is:

* the **collection is gone** from MongoDB, so its table and child tables are
  never visited again by a sync;
* the collection still exists but a **field disappeared from every document**, so
  the child table that held that field is left behind with old rows.

It asks twice: you type `YES`, and then confirm each table separately. The number
of rows is shown, because dropping cannot be undone. Child tables are dropped
before their parent, so a foreign key cannot block it.

A sync reports both kinds as a warning without touching anything. The
whole-database check runs when a run covers every collection, a run on one
collection says nothing about the other tables.

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

The result object holds, in this order:

| Field | Meaning |
|---|---|
| `Operation` | what was run |
| `StartTime`, `EndTime` | when it ran |
| `Duration`, `DurationSeconds` | how long it took, readable and as a number |
| `Collections` | the result per collection, including its details |
| `TotalSuccess`, `TotalWarnings`, `TotalFailed` | counts over the collections |
| `OrphanTables`, `OrphanTablesRemoved` | tables without a collection behind them |
| `ExitCode` | 0, 1 or 2 as above |

The migration and sync results carry the same `Duration` and `DurationSeconds`,
which makes it easy to compare a slow run with a fast one. Every run writes all
of it to `workflow_report_<timestamp>.json`.

To clean up leftover tables in the same run, add `-RemoveOrphanTables`. In an
automated run there is nobody to answer a confirmation, so passing the switch
counts as the confirmation itself and a warning says so in the log:

```powershell
Invoke-N2SMigration -Operation IncrementalSync -RemoveOrphanTables
```

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

## Speed, and why it is built this way

Rows are not written one at a time. Per batch of documents the tool opens one
transaction, collects the rows per table and per set of columns, and sends them as
multi-row statements. Measured on a local MySQL: row by row does about 200 rows
per second, the same rows in multi-row statements inside a transaction more than
11,000. A batch of 100 documents with 3,300 rows goes in 10 statements instead of
3,300 round trips.

Change detection hashes the document's own BSON bytes rather than walking every
value from PowerShell. On one document with a few hundred sub-documents that is
the difference between 16 seconds and a third of a second.

Measured on a collection of 1,000 documents with 10,000 child rows:

| Operation | Time |
|---|---|
| Full migration | ~8 s |
| Full sync, every document rewritten | ~11 s |
| Incremental sync, nothing changed | ~1 s |
| Repair after rows were deleted in SQL | ~2 s |

A full sync only happens when the state file is missing or `-ForceFullSync` is
given; day to day you are in the one-second case.

**Why batching and not parallel processing:** the bottleneck was the number of
round trips to the database, not a shortage of threads. Ten threads that each
still write row by row only make that ten times less bad; one statement with 500
rows makes it a hundred times less bad. After this change the database is no
longer the slowest part — the per-row work in PowerShell is, and that is where
parallel runspaces would be the next step.

If a statement fails, that chunk is retried row by row, so one bad row costs its
own row instead of the whole batch, and the report names the document it came
from. If a commit fails, the whole batch is rolled back and that is reported.

Not everything is batched: during a sync the main row of a changed document is a
single `UPDATE`, one round trip per document. Its child rows are batched, and
those are the bulk of the work.

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

**Every document looks changed after updating the tool**
The way a document is hashed changed, so the stored hashes no longer match. The
first sync rewrites everything once and is back to normal after that.

**A sync of one collection takes much longer than expected**
Check whether you are syncing all collections instead of the one you changed:
option 7 in the menu, or `Start-Migration.ps1` without `-Collections`, walks the
whole database.

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
* **Field order counts** for change detection. The hash comes from the document's
  BSON, which keeps the order the fields are stored in. If MongoDB writes them in
  a different order after an update, the document is seen as changed and rewritten.
  That costs a rewrite, not data.

---

## Sources

What each source was actually used for, rather than a list of links.

### Libraries and drivers

| Source | Used for |
|---|---|
| [Mdbc](https://github.com/nightroman/Mdbc) | MongoDB from PowerShell: `Connect-Mdbc`, `Get-MdbcData`, `Add-MdbcData`, `Remove-MdbcCollection` |
| [MongoDB C#/.NET driver](https://www.mongodb.com/docs/drivers/csharp/current/) | `BsonDocument` and `ToBsonDocument()`, used to hash a document from its own BSON |
| [MySQL Connector/NET](https://dev.mysql.com/doc/connector-net/en/) | `MySqlConnection`, parameterised commands, transactions |
| [Connecting PowerShell to MySQL](https://medium.com/@kavindra.mpez/database-automation-powershell-connectivity-with-mysql-ado-net-provider-powershell-cmdlets-b1c4f528eeab) | the first working connection, before the driver was loaded dynamically |

### PowerShell

| Source | Used for |
|---|---|
| [about_Output_Streams](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_output_streams) | the reason the core reports with levels: `Write-Host` also writes to the information stream, but always shows on the console unless that stream is redirected. That is why it is unsuitable for the core and fine in the menu. |
| [Everything about ShouldProcess](https://learn.microsoft.com/en-us/powershell/scripting/learn/deep-dives/everything-about-shouldprocess) | the confirmation before a table is dropped, plus `-WhatIf` and `-Confirm:$false` |
| [How to write a module manifest](https://learn.microsoft.com/en-us/powershell/scripting/developer/module/how-to-write-a-powershell-module-manifest) | the `.psd1` |
| [PowerShell documentation](https://learn.microsoft.com/en-us/powershell/) | general reference |

### MySQL

| Source | Used for |
|---|---|
| [INSERT](https://dev.mysql.com/doc/refman/8.0/en/insert.html) | multi-row statements, the basis of the batching |
[FOREIGN KEY constraints](https://dev.mysql.com/doc/refman/8.0/en/create-table-foreign-keys.html) | the link between a main table and its child tables, and the drop order |
|  [Data type storage requirements](https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html) | why long text becomes `LONGTEXT` instead of a longer `VARCHAR` |

### .NET

| Source | Used for |
|---|---|
| [DateTime.TryParseExact](https://learn.microsoft.com/en-us/dotnet/api/system.datetime.tryparseexact) | reading dates in several notations, one format at a time so day-first wins over month-first |
| [CultureInfo.InvariantCulture](https://learn.microsoft.com/en-us/dotnet/api/system.globalization.cultureinfo.invariantculture) | numbers and dates that mean the same on every machine |

### Testing

| Source | Used for |
|---|---|
| [Pester quick start](https://pester.dev/docs/quick-start) | the structure of the test files |
| [Pester: InModuleScope](https://pester.dev/docs/commands/InModuleScope) | testing functions the module does not export |
| [Pester: Mock](https://pester.dev/docs/usage/mocking) | running the tests without a database |
| [Pester tutorial (video)](https://www.youtube.com/watch?v=iWbemnUpGx4) | first steps with Pester |

### AI assistance

AI was used as a tool during development.

* **Claude Code (Opus 5)** — the data migration, validation and synchronisation,
  the conversion layer for type and format differences, batched writing, the
  Pester suite and this README.
* **ChatGPT** — sparring on the schema analysis and the sync design.
* **GitHub Copilot** — error handling and repetitive code.
