# SQL File Executor

A utility script for reliably importing SQL dump data into PostgreSQL databases with proper table ordering based on Prisma schema relationships.

## Overview

This script solves common issues with importing large SQL dumps by:
- Breaking down large SQL files into manageable chunks
- Analyzing table dependencies using Prisma schema
- Executing INSERT statements in the correct order to avoid foreign key conflicts
- Providing detailed logging and error tracking

## Prerequisites

- Node.js 16+ (Check the tested version in `.nvmrc`)
- PostgreSQL database
- Prisma schema file (if using dependency ordering)
- SQL dump file(s) containing INSERT statements

## Installation

```sh
pnpm install
```

## Configuration

- Create a `.env` file based on `example.env`:

```env
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=your_db_host
DB_DATABASE=your_database_name
DB_PORT=5432
DB_SCHEMA=public
```

To update the configuration:

1. Create or edit the .env file in the root directory
2. Replace the placeholder values with your actual database credentials:
  - `your_username`: Your PostgreSQL username
  - `your_password`: Your PostgreSQL password
  - `your_db_host`: Hostname where PostgreSQL is running
  - `your_database_name`: Name of your target database
  - The default port `5432` and schema `public` can be left as-is for typical PostgreSQL installations

## Quick Start

```bash
# Basic usage with SQL dump file
node lib/sql-file-executer.js path/to/dump.sql

# With Prisma schema for table dependency ordering
node lib/sql-file-executer.js path/to/dump.sql -s path/to/schema.prisma

# Skip execution until specific line number
node lib/sql-file-executer.js path/to/dump.sql --skip-line-until 1000

# Only generate execution order without running statements
node lib/sql-file-executer.js path/to/dump.sql -s path/to/schema.prisma --only-execution-order

## Configuration Settings

The script uses the following configuration settings defined in `config.js`:

```javascript
const CONFIG = {
  // Lines to ignore when processing SQL files
  IGNORED_LINE_STARTS: [],

  // Cache directory for temporary files
  CACHE_DIR: "",

  // Error log file name
  ERROR_FILE_NAME: "",

  // File name prefixes for different types of statements
  CHUNK_FILE_NAME_PREFIX: "",
  INSERT_STATEMENT_FILE_NAME_PREFIX: "",
  ORDERED_INSERT_STATEMENT_FILE_NAME_PREFIX: "",
  OTHER_STATEMENT_FILE_NAME_PREFIX: "",

  // Maximum number of SQL statements per file chunk
  MAX_LINES_PER_FILE: 2000,
};
```

### Settings Description

| Setting | Description |
|---------|-------------|
|IGNORED_LINE_STARTS | Array of SQL line prefixes to ignore during processing |
| CACHE_DIR | Directory where temporary processing files are stored |
| CHUNK_FILE_NAME_PREFIX | Prefix for chunked SQL file segments |
| ERROR_FILE_NAME | Name of the error log file |
| INSERT_STATEMENT_FILE_NAME_PREFIX | Prefix for files containing INSERT statements |
| ORDERED_INSERT_STATEMENT_FILE_NAME_PREFIX | Prefix for ordered INSERT statement files |
| OTHER_STATEMENT_FILE_NAME_PREFIX | Prefix for non-INSERT SQL statements |
| MAX_LINES_PER_FILE | Maximum number of SQL statements per chunk file |

These settings control how the script processes and organizes SQL statements during execution. The chunking mechanism helps manage memory usage for large SQL dumps by breaking them into smaller, manageable files during processing.



## Usage

Run the script by providing the SQL file path and optionally the Prisma schema:

```sh
node lib/sql-file-executer.js path/to/dump.sql -s [path/to/schema.prisma]
```

### Process Flow

1. Reads and parses the SQL dump file
2. If Prisma schema provided:
   - Analyzes table relationships
   - Creates dependency graph
   - Determines optimal insertion order
3. Splits statements into chunks
4. Executes statements in ordered batches
5. Logs progress and errors to `error.log`



### Error Handling

- Failed statements are logged to `error.log` with timestamps
- Script continues execution even if individual statements fail
- Summary of successful/failed operations is provided

## Caching

The script uses the `.cache` directory for temporary files:
- Chunked SQL statements
- Ordered insert statements
- Other SQL statements

Cache files are automatically cleaned up after execution.

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| DB_USER | Database username | Yes |
| DB_PASSWORD | Database password | Yes |
| DB_HOST | Database host | Yes |
| DB_DATABASE | Target database name | Yes |
| DB_PORT | Database port | Yes |
| DB_SCHEMA | Database schema | Yes |

## Limitations

- Currently tested only with PostgreSQL
- Optimized for INSERT statements
- Requires valid Prisma schema for dependency ordering