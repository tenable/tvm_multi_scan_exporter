# How to Set up MS SQL Server Locally

1.

Install the driver for MS SQL Server for your
platform. [This document](https://learn.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server?view=sql-server-ver16)
from Microsoft has the required information.

For macOS, you can use Homebrew to install the driver:

```
brew install msodbcsql17
```

2. Run the docker compose in this folder
   ```
    docker compose up -d
    ```

3. Go to the exec in the docker desktop, and run this command to open up the MSSQL query thingy.
   ```
   /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'YourStrong!Passw0rd' -C -N
   ```

4. Create a DB
   ```
   CREATE DATABASE tenable;
   GO
   ```

5. Verify if the DB was created
   ```
   SELECT name FROM sys.databases;
   GO
   ```

6. Use `tenable` database
   ```
   USE tenable;
   GO
   ```

7. List Tables
   ```
   SELECT name FROM sys.tables;
   GO
   ```

8. Inspect the table
   ```
   SELECT * FROM scan_results;
   GO
   
   SELECT TOP (5) * FROM scan_results;
   GO 
   
   SELECT COUNT(*) FROM scan_results;
   GO
   
   SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'scan_results';
   GO
   ```