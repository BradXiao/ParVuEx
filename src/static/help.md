# ParVuEx Help

## `v1.3.0`

### Overview

This application allows you to execute SQL queries on Parquet files.

### Extended Version Major Changes

-   Redesign UI
    -   Concise main UI
    -   Simplify pagination
    -   Add simple status info
    -   Improve dialogs
    -   Support SQL statement history
    -   Auto remember columns width
    -   Enhanced SQL statement highlighting
    -   Zebra striping for the table
-   Add new hotkeys
-   Support launcher
    -   Speedup startup time
    -   Support single/multi windows
-   SQL Edit
    -   Auto complete
    -   Support inserting specific columns
-   Quick statistics
    -   Value counts
    -   Quick row values
-   Support _.csv, _.parquet
-   Memory efficient (can open a large file quickly)

### Hotkeys

-   Browse SQL statement history
    -   `Ctrl`+`↑`/`↓`
-   Pagination
    -   `Ctrl`+`←`/`→`
-   Excute
    -   `Enter`
    -   `Shift`+`Enter` for new lines
-   Mouse scroll the table horizontally
    -   Hold `Shift`

### How to Use

1. Click `File` → `Open` to select a Parquet/CSV file.
2. Write your SQL query in the provided text area.
3. Click `Execute` or press `Enter` to run the query and see results.

### FAQ:

1. Which SQL Queries are supported?
    - Basically it supports data transformation, non join queries. Take a look https://duckdb.org/docs/sql/query_syntax/select

### Contact

#### Project:

-   ParVuEx: https://github.com/BradXiao/ParVuEx/;
-   ParVu GitHub: https://github.com/AzizNadirov/ParVu;
