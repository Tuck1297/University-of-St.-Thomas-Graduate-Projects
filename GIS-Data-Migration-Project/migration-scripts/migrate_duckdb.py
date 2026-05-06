import os
import sys
import duckdb
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Configuration
DUCKDB_PATH = os.getenv('DUCKDB_PATH', '../duckdb-ingestion/project_data.duckdb')
POSTGRES_DB = os.getenv('POSTGRES_DB_DUCKDB', 'explore_more_v2')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'normalized')

def ensure_database_exists():
    """Ensures the target PostgreSQL database exists before migration."""
    print(f"Checking if database '{POSTGRES_DB}' exists on {POSTGRES_HOST}...")
    try:
        # Connect to 'postgres' default database to check for existence of target DB
        conn = psycopg2.connect(
            dbname='postgres',
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{POSTGRES_DB}'")
        exists = cur.fetchone()
        
        if not exists:
            print(f"Database '{POSTGRES_DB}' does not exist. Creating it...")
            cur.execute(f"CREATE DATABASE {POSTGRES_DB}")
            print(f"Database '{POSTGRES_DB}' created successfully.")
        else:
            print(f"Database '{POSTGRES_DB}' already exists.")
            
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Warning: Could not verify/create database '{POSTGRES_DB}': {e}")
        print("Continuing, but the ATTACH command may fail if the database doesn't exist.")
        return False

def migrate():
    # Ensure database exists
    ensure_database_exists()

    print(f"\nConnecting to DuckDB at: {DUCKDB_PATH}")
    try:
        con = duckdb.connect(DUCKDB_PATH)
    except Exception as e:
        print(f"Error connecting to DuckDB: {e}")
        return

    try:
        print("Installing and loading PostgreSQL extension...")
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")
        
        # PostgreSQL connection string for DuckDB extension
        # Note: The extension uses a specific format for the connection string or multiple parameters
        pg_conn_info = f"dbname={POSTGRES_DB} user={POSTGRES_USER} host={POSTGRES_HOST} password={POSTGRES_PASSWORD} port={POSTGRES_PORT}"
        
        print(f"Attaching PostgreSQL database '{POSTGRES_DB}' on {POSTGRES_HOST}...")
        con.execute(f"ATTACH '{pg_conn_info}' AS pg (TYPE POSTGRES);")
    except Exception as e:
        print(f"Error connecting to PostgreSQL via DuckDB extension: {e}")
        print("Make sure the database exists on the PostgreSQL server.")
        con.close()
        return

    try:
        # 1. Ensure schema exists in PG
        print(f"Ensuring schema '{POSTGRES_SCHEMA}' exists in PostgreSQL...")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{POSTGRES_SCHEMA};")

        # 2. Get list of tables and views from DuckDB normalized schema
        objects_query = """
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'normalized'
        """
        objects = con.execute(objects_query).fetchall()

        if not objects:
            print(f"No tables or views found in DuckDB schema 'normalized'.")
            return

        print(f"\nFound {len(objects)} objects to migrate.")

        # 3. Perform migration using DuckDB's native cross-database engine
        for obj_name, obj_type in objects:
            label = "Table" if obj_type == 'BASE TABLE' else "View"
            print(f"Migrating {label}: {obj_name}...")
            
            # Drop if exists in PG (to match 'replace' behavior of migrate.py)
            con.execute(f"DROP TABLE IF EXISTS pg.{POSTGRES_SCHEMA}.{obj_name};")
            
            # Create table in PG from DuckDB object
            # This handles both tables and views by creating a table in PG
            con.execute(f"CREATE TABLE pg.{POSTGRES_SCHEMA}.{obj_name} AS SELECT * FROM normalized.{obj_name};")
            
            count = con.execute(f"SELECT COUNT(*) FROM pg.{POSTGRES_SCHEMA}.{obj_name}").fetchone()[0]
            print(f"  Successfully migrated {count} rows.")

        print("\nMigration completed successfully via DuckDB PostgreSQL extension!")

    except Exception as e:
        print(f"An error occurred during migration: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    migrate()
