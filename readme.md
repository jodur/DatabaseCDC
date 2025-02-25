# SQL CDC CRUD

This project demonstrates how to use SQL Server Change Data Capture (CDC) to track changes in a database table and perform CRUD operations. The script connects to a SQL Server database, fetches changes from a specified table, and simulates API calls based on the changes.

## Prerequisites

- Python 3.x
- `pyodbc` library
- SQL Server with CDC enabled on the target table

## Setup

1. Install the required Python library:
    ```sh
    pip install pyodbc
    ```

2. Ensure that Change Data Capture (CDC) is enabled on your SQL Server database and the target table.

## Enabling CDC in SQL Server

To enable CDC on your SQL Server database and table, follow these steps:

1. Enable CDC on the database:
    ```sql
    USE [YourDatabaseName]
    GO
    EXEC sys.sp_cdc_enable_db
    GO
    ```

2. Enable CDC on the target table:
    ```sql
    USE [YourDatabaseName]
    GO
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'YourTableName',
        @role_name = NULL
    GO
    ```

## Configuration

Update the database connection details in the script:

```python
myDatabase = 'Test_DB'
myTable = 'prospects'
server = 'localhost'
username = 'sa'
password = 'your_password'