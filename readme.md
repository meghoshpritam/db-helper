# DB Helper Tool

A robust Node.js utility for reliably importing large SQL dumps into PostgreSQL databases with intelligent table ordering and error handling.

[![GitHub stars](https://img.shields.io/github/stars/meghoshpritam/db-helper?style=social)](https://img.shields.io/github/stars/meghoshpritam/db-helper)

## 🎯 Motivation

Importing large SQL dumps often fails due to:
- Foreign key constraint violations
- Memory limitations with large files
- Lack of proper error handling and recovery
- No visibility into progress or errors

This tool solves these problems by providing intelligent chunking, dependency-based ordering, and detailed execution tracking.

## 🚀 Installation

Clone the repository:
```sh
git clone git@github.com:meghoshpritam/db-helper.git
cd db-helper
pnpm install
```

## ⚡ Quick Start

1. Configure database connection in `.env`:

```env
DB_USER=user
DB_PASSWORD=password
DB_HOST=localhost
DB_DATABASE=db
DB_PORT=5432
DB_SCHEMA=public
```

2. Run the executor:
```sh
# With Prisma schema for table ordering
node lib/sql-file-executer.js path/to/dump.sql path/to/schema.prisma

# Without schema
node lib/sql-file-executer.js path/to/dump.sql
```

## 📚 Full Documentation

- [SQL File Executor](docs/sql-file-executer.md)

## ✨ Features

- Breaks down large SQL files into manageable chunks
- Analyzes table dependencies using Prisma schema
- Executes INSERT statements in correct order
- Detailed progress logging and error tracking
- Automatic cleanup of temporary files
- Continues execution even if individual statements fail

## ⭐ Support

If you find this tool useful, please consider giving it a star on GitHub! Your support helps make the project more visible to others who might benefit from it.

[⭐ Star on GitHub](https://github.com/meghoshpritam/db-helper)

## 📝 License

Apache License 2.0