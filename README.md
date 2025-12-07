# ParVuEx - **Par**quet **Vie**wer **Ex**tended

<img src="https://raw.githubusercontent.com/BradXiao/ParVuEx/refs/heads/main/docs/images/main.png" width="350">

## Overview

This application allows you to execute SQL queries on Parquet/CSV files. Uses [DuckDB](https://duckdb.org/) Python implementation.

## Extended Version Major Changes

#### Redesign UI

<img src="https://raw.githubusercontent.com/BradXiao/ParVuEx/refs/heads/main/docs/images/highlighting.png" width="700">

-   Enhanced SQL statement highlighting
    -   Support column names highlighting
    -   Support common highlighting such as comments, values, strings
    -   Customable highlighting keywords
-   Add simple status info
    -   Showing total/current/selected rows
-   Support SQL statement history
    -   Use hotkeys to switch previous statements
-   Concise main UI
-   Simplify pagination
-   Improve dialogs
-   Auto remember columns width

#### SQL Edit

<img src="https://raw.githubusercontent.com/BradXiao/ParVuEx/refs/heads/main/docs/images/insert_selected_columns.png" width="700">

-   Support inserting specific columns

#### Statistics

<img src="https://raw.githubusercontent.com/BradXiao/ParVuEx/refs/heads/main/docs/images/value_counts.png" width="700">
<img src="https://raw.githubusercontent.com/BradXiao/ParVuEx/refs/heads/main/docs/images/table_info.png" width="700">
<img src="https://raw.githubusercontent.com/BradXiao/ParVuEx/refs/heads/main/docs/images/value_counts_highlight.png" width="700">

-   Value counts
    -   Show statistics on selected columns
-   Quick row values
    -   Show selected row values in a table
-   Table info
-   Quick highlight characters as you type

#### Support launcher

-   Speedup startup time
    -   Use only one core
    -   Minimize to tray
-   Support single/multi windows

<img src="https://raw.githubusercontent.com/BradXiao/ParVuEx/refs/heads/main/docs/images/multi_windows.png" width="700">

#### Others

-   Add new hotkeys
-   Memory efficient (can open a large file quickly)
-   Support _.csv, _.parquet

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

1. Click `File` → `Open` to select a Parquet/CSV file.
2. Write your SQL query in the provided text area.
3. Click `Execute` or press `Enter` to run the query and see results.

## Build

-   Windows
    -   `build.bat`

## FAQ:

1. Which SQL Queries are supported?
    - Basically it supports data transformation, non join queries. Look [here](https://duckdb.org/docs/sql/query_syntax/select)

## Icon

<a href="https://www.flaticon.com/free-icons/table-of-content" title="table of content icons">Table of content icons created by iconsax - Flaticon</a>
