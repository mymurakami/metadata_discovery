# Metadata Discovery

## Overview
The **Metadata Discovery** tool extracts metadata from SQL queries, identifying elements such as columns, WHERE clauses, JOIN operations, GROUP BY statements, subqueries, functions, partitions, and variables using regular expressions.

## Features
- **SQL Query Parsing**: Analyzes SQL queries to extract essential metadata.
- **Regex-based Extraction**: Uses regular expressions to identify and parse specific SQL components.
- **Support for Various SQL Elements**: Recognizes columns, WHERE clauses, JOINs, subqueries, and other SQL operations.

## Requirements
- Python 3.7+
- Dependencies:
  - `mo_sql_parsing`
  - `sql_metadata`
  
Install the required packages using the following commands:
```bash
pip install mo_sql_parsing
pip install sql_metadata
```

## Usage
1. **Import Libraries**: The tool relies on libraries such as `mo_sql_parsing` and `sql_metadata` for SQL parsing and metadata extraction. Make sure to import these in your script.
2. **Metadata Extraction**: This project contains classes and functions designed to parse SQL queries. The main logic is found in the notebook cells, which include parsing logic, metadata extraction, and SQL component identification.

## Contents of the Notebook
1. **Library Installation**: Commands to install necessary libraries (`mo_sql_parsing` and `sql_metadata`).
2. **Import Statements**: The notebook includes imports for parsing and formatting SQL queries.
3. **Operator Enum**: Contains a list of comparison operators used in SQL.

## How to Run
1. Install the necessary libraries using the commands mentioned in the **Requirements** section.
2. Load the notebook and execute the cells in order to parse and extract metadata from SQL queries.
