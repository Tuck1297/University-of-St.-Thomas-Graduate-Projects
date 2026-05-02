# GIS Data Migration: DuckDB to PostgreSQL

This project contains scripts to migrate data from a DuckDB database (specifically the `normalized` schema) to a PostgreSQL database.

## Prerequisites

- Python 3.10+
- PostgreSQL instance (local or remote)
- The DuckDB database file (`project_data.duckdb`) should be located in `../duckdb-ingestion/`.

## Setup

1.  **Initialize the virtual environment:**
    ```bash
    cd migration-scripts
    python -m venv venv
    ```

2.  **Activate the virtual environment:**
    - **Windows:**
      ```powershell
      .\venv\Scripts\activate
      ```
    - **macOS/Linux:**
      ```bash
      source venv/bin/activate
      ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    Edit the `.env` file with your PostgreSQL credentials and the path to your DuckDB file.

    ```env
    DUCKDB_PATH=../duckdb-ingestion/project_data.duckdb
    POSTGRES_DB=gis_migration
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=password
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    POSTGRES_SCHEMA=normalized
    ```

## Running the Migration

To execute the migration, run:

```bash
python migrate.py
```

The script will:
1. Connect to the DuckDB database.
2. Identify all tables in the `normalized` schema.
3. Create the `normalized` schema in PostgreSQL if it doesn't exist.
4. Migrate each table from DuckDB to PostgreSQL (using `if_exists='replace'` by default).
