import os
import sys
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DUCKDB_PATH = os.getenv('DUCKDB_PATH', '../duckdb-ingestion/project_data.duckdb')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'gis_migration')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'normalized')

def check_existing_tables(engine, schema):
    """Checks if any tables exist in the specified PostgreSQL schema."""
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema=schema)
    return existing_tables

def migrate():
    # PostgreSQL connection string
    pg_conn_str = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

    print(f"Connecting to DuckDB at: {DUCKDB_PATH}")
    try:
        duck_conn = duckdb.connect(DUCKDB_PATH)
    except Exception as e:
        print(f"Error connecting to DuckDB: {e}")
        return

    print(f"Connecting to PostgreSQL at: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    try:
        pg_engine = create_engine(pg_conn_str)
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        duck_conn.close()
        return

    # 1. Check for existing tables in PostgreSQL
    existing_pg_tables = check_existing_tables(pg_engine, POSTGRES_SCHEMA)

    if existing_pg_tables:
        print(f"\nWARNING: The following tables already exist in the '{POSTGRES_SCHEMA}' schema on PostgreSQL:")
        for table in existing_pg_tables:
            print(f" - {table}")

        user_input = input(f"\nDo you want to drop and rebuild these tables? (yes/no): ").strip().lower()
        if user_input != 'yes':
            print("Migration cancelled by user. Exiting.")
            duck_conn.close()
            sys.exit(0)
        print("User approved. Proceeding with drop and rebuild...")

    # 2. Create schema if it doesn't exist
    with pg_engine.connect() as conn:
        print(f"Ensuring schema '{POSTGRES_SCHEMA}' exists in PostgreSQL...")
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA}"))
        conn.commit()

    # 3. Get list of tables in the normalized schema from DuckDB
    tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'normalized'"
    tables = duck_conn.execute(tables_query).fetchall()

    if not tables:
        print(f"No tables found in DuckDB schema 'normalized'.")
        duck_conn.close()
        return

    print(f"Found {len(tables)} tables to migrate: {[t[0] for t in tables]}")

    # 4. Perform migration
    for (table_name,) in tables:
        print(f"Migrating table: {table_name}...")

        # Read ALL data from DuckDB for this table
        df = duck_conn.execute(f"SELECT * FROM normalized.{table_name}").df()

        # Write ALL data to PostgreSQL
        # if_exists='replace' handles the drop/rebuild if user approved above
        df.to_sql(
            table_name,
            pg_engine,
            schema=POSTGRES_SCHEMA,
            if_exists='replace',
            index=False
        )
        print(f"Successfully migrated {len(df)} rows to {POSTGRES_SCHEMA}.{table_name}")

    duck_conn.close()
    print("\nMigration completed successfully!")

if __name__ == "__main__":
    migrate()
