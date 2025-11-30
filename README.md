# ParVuEx - **Par**quet **Vie**wer **Ex**tended

![parquet logo](https://datos.gob.es/sites/default/files/styles/blog_image/public/blog/image/logo_formato_parquet.jpg?itok=CT-UucXj)

## Overview

This application allows you to execute SQL queries on Parquet files. Uses [DuckDB](https://duckdb.org/) Python implementation

## Extended Version Major Changes

-   Redesign UI
    -   Concise main UI
    -   Simplify pagination
    -   Add simple status info
    -   Improve dialogs
    -   Support SQL statement history
    -   Auto remember columns width

![main](https://raw.githubusercontent.com/BradXiao/ParVuEx/02d17fb60499d81aaf61a2889f108e0216f526d2/docs/images/main.png)

-   Add new hotkeys
-   Support launcher
    -   Speedup startup time
    -   Support single/multi windows
-   Add quick value counts

![value counts](https://raw.githubusercontent.com/BradXiao/ParVuEx/02d17fb60499d81aaf61a2889f108e0216f526d2/docs/images/value_counts.png)

## Features

-   Load Parquet files
-   Execute SQL queries on the file
-   Filter results
-   Export results to CSV or Excel
-   Syntax highlighting for SQL
-   Minimalistic design

## Hotkeys

-   Browse SQL statement history
    -   `Ctrl`+`↑`/`↓`
-   Pagination
    -   `Ctrl`+`←`/`→`
-   Excute
    -   `Enter`
    -   `Shift`+`Enter` for new lines
-   Mouse scroll the table horizontally
    -   Hold `Shift`

## How to Use

1. Click 'Browse' to select a Parquet file.
2. Write your SQL query in the provided text area.
3. Click 'Execute' to run the query and see results.
4. Use the 'Filter' button to apply filters to the results.
5. Export results using the 'Export' option in the 'File' menu.
6. Adjust the SQL editor size by dragging the splitter.

## Build

-   Windows
    -   `build.bat`

## FAQ:

1. Which SQL Queries are supported?
    - Basically it supports data transformation, non join queries. Look [here](https://duckdb.org/docs/sql/query_syntax/select)
