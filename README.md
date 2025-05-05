# TVM Multi Scan Exporter

Tenable Vulnerability Management offers
the [Scan Export API](https://pytenable.readthedocs.io/en/stable/api/io/scans.html#tenable.io.scans.ScansAPI.export) to
export vulnerabilities from individual scans,
and [Vulnerability Exports API](https://pytenable.readthedocs.io/en/stable/api/io/exports.html#tenable.io.exports.api.ExportsAPI.vulns)
to export vulnerabilities from all the scans, after they have been collapsed into an asset entity. This library allows
the users of Tenable VM to gather vulnerabilities from their scan data, before the vulnerabilities are collapsed into
an Asset entity by using existing public APIs. We call this TVM Multi Scan Exporter.

> ⚠️ **Disclaimer**  
> Export aggregation is performed entirely on your machine.  
> Please make sure your system has enough memory and disk space to complete the process successfully.

Users can create the following types of aggregated scan exports.

1. `CSV` - A single CSV file will be created in the same directory of the script you write.
2. `JSON` - A single JSON file will be created in the same directory of the script you write.
3. `Parquet` - A single Apache Parquet file will be created in the same directory of the script you write.
4. `Write to DB` - The result of the export will be written to an external database
    - Users need to provide database credentials to a real DB, and the results of the exports are dumped into it. The
      database needs to be created by the user before using this library. A table will be automatically created.
    - > ⚠️ **Disclaimer**
      Make sure the columns stay the same each time you use this export option. If the schema changes between exports,
      the library will raise an error.

## How to use this library.

### Optional Prerequisites

If you're writing to an external database, you need to install the driver for the database you are using.

For running MS SQL Server on the Mac, for example, you would want to do the following. You can pick the version
compatible with the version of the DB you're running.

```
brew install msodbcsql17
```

For Linux, you may
follow [this](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16&tabs=alpine18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline)
guide to install the driver.

This library can be configured in two ways

1. As a Method Argument.
2. Using a `TOML` Configuration file. (This is the recommended approach)

### 1. Configuring TvmMultiScanExporter Programmatically

This library provides a single class, `TvmMultiScanExporter`, with a single method, `export`, which accepts a `Config`
object containing all the necessary export configuration.

Below is a detailed overview of the supported configuration options available in this library.

#### Config

1. `scan_name` (str) Scan Name to export. A case-insensitive contains check will be run on this value. This is a *
   *mandatory** field.
2. `allowed_memory_gb` (int) Allowed memory for the Duck DB process on the client. This is a **mandatory** field.
3. `workers` (int) Parallelism for running exports. Defaults to 1. This value should be between 1 and 10.
4. `window` (`Window`) Window to export scans.
5. `columns` (`List[str]`) Columns to include in the export. These are header values from CSV Scan Exports. It already
   defaults to a common list of fields.
6. `output_location` (str) The folder where the export files will be downloaded. This is an **optional** field.
   The default value is the current working directory.
7. `export_type`: (`ExportType`) Type of export. The supported values are: `Csv()`, `Json()`, `Parquet()`,
   and `WriteToDatabase()`. The `WriteToDatabase` object takes in the config to connect to an external database. When
   using `WriteToDatabase()`, users need to configure the `EXTERNAL_DB_PASSWORD` environment variable.

#### Window

1. `since` (int) Unix Timestamp of the start time of the window to export scans. Defaults to 24 hours in the past.
2. `upto` (int) Unix timestamp of the end time of the window. Defaults to current time.

#### WriteToDatabase

1. `user` (str): The username for connecting to the external database.
2. `host_address` (str): The server name or IP address of the external database.
3. `port` (int): The port number used to connect to the database.
4. `database_name` (str): The name of the database. This should already exist within the database.
5. `table_name` (str): The name of the table where exported data will be written. This table may or may not already
   exist,
   but the schema of any existing table should match the schema of the export (i.e., the columns being exported).
6. `engine` (DatabaseEngine): The type of database engine. Currently, we support MS SQL Server by default.
7. `write_batch_size` (int): The number of rows included in each batch. The default is set to 100.

Here's how you would use it.

1. Create an instance of `TenableScanAggregator` after setting these environmental variables: `TVM_ACCESS_KEY`
   and `TVM_SECRET_KEY`

```python
exporter = TvmMultiScanExporter()
```

From here, all you need to do is to call the `export` method with the required `Config`. Here are some examples:

##### How to create a CSV / JSON / Parquet

```python
exporter.export(
    Config(
        scan_name="nessus-agent-1-asset-hour-9",
        allowed_memory_gb=12,
        export_type=Csv(),  # or Json() or Parquet()
        workers=1
    )
)
```

##### How to create a "Write-to-External-DB Export"

When using the `WriteToDatabase` export type, you must provide the database password as an environment variable. The
library will automatically look for the following environment variable when running the script:

- `EXTERNAL_DB_PASSWORD`: The password for connecting to the external database.

Make sure to set this environment variable before executing the script to ensure a successful connection.

```python
exporter.export(
    Config(
        scan_name="nessus-agent-1-asset-hour-9",
        allowed_memory_gb=12,
        export_type=WriteToDatabase(
            user="your-db-username",
            host_address="db-server-name",
            port=1433,
            database_name="your-db-name",
            table_name="your-table-name",
        ),
        workers=1
    )
)
```

### 2. Configuring TenableScanAggregator with a Configuration file (Recommended)

In this case, the Python file will be the smallest. All necessary information can be provided to the library using a
file named `config.toml` placed in the same directory as the script. It follows a similar structure as the `Config`
class. Here's an example `config.toml` file.

Your Tenable Vulnerability Management access and secret keys can be configured as environment variables. The library
will look for the following variables when you run the script.

1. `TVM_ACCESS_KEY` TVM Access Key
2. `TVM_SECRET_KEY` TVM Secret Key

The name and location of this configuration file can also be customized, as detailed in the following sections of this
document.

When using the `write_to_database` export type, you need to provide the database password as an environment variable.
The library will look for the additional environmental variable: `EXTERNAL_DB_PASSWORD` when you run the script.

> ⚠️ **Note**  
> The configuration in the `toml` file can be overridden by providing a `Config` object to the `export` method.

```toml
# Required fields
scan_name = "" # scan name to filter by
allowed_memory_gb = 12 # tweak as necessary

# Optional fields: uncomment as needed

# workers = 1
# columns = ["Asset UUID", "IP Address", "Plugin ID", "Severity"]

# output_location = "path/to/download/folder"

# Uncomment for CSV / JSON / Parquet export types
#export_type = "csv"
#export_type = "json"
#export_type = "parquet"

# Keep the following object to write to database. Comment out if not needed
[export_type]
type = "write_to_database"
user = "your-db-username"
host_address = "db-server-name"
port = 1433
database_name = "your-db-name"
table_name = "your-table-name"
engine = "MsSqlServer"           # Optional; defaults to MsSqlServer
write_batch_size = 500           # Optional; defaults to 100

# uncomment the following object if you want to override the window
#[window]
#since = 1742898712
#upto = 1710086402

```

If the config file is placed in a different directory / under a different name, the path to the same can be provided as
a property to the `TvmMultiScanExporter` object.

```
project/
├── run.py   ← your script file
├── config/
│   └── settings.toml

```

```python
TvmMultiScanExporter(config_path="config/settings.toml").export()
```

Examples for both the approaches can be found inside the `examples` directory in this repo.

## Configuring `tvm_url` for Different TVM Environments

The `TvmMultiScanExporter` class accepts an optional `tvm_url` property, allowing users to configure the library for
different TVM environments. By default, the library uses the standard TVM URL (cloud.tenable.com),
but this can be overridden as needed.

Example: Configuring `tvm_url`
You can specify the tvm_url when creating an instance of `TvmMultiScanExporter`:

```python
from tvm_multi_scan_exporter.aggregated_scan_exporter import TvmMultiScanExporter

exporter = TvmMultiScanExporter(tvm_url="https://custom-tvm-environment.example.com")
```