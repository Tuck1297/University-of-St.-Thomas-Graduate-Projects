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
    """Checks if any tables or views exist in the specified PostgreSQL schema."""
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema=schema)
    existing_views = inspector.get_view_names(schema=schema)
    return existing_tables + existing_views

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

    # 1. Check for existing tables/views in PostgreSQL
    existing_pg_objects = check_existing_tables(pg_engine, POSTGRES_SCHEMA)

    if existing_pg_objects:
        print(f"\nWARNING: The following objects already exist in the '{POSTGRES_SCHEMA}' schema on PostgreSQL:")
        for obj in existing_pg_objects:
            print(f" - {obj}")

        user_input = input(f"\nDo you want to drop and rebuild these objects? (yes/no): ").strip().lower()
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

    # 3. Get list of tables and views in the normalized schema from DuckDB
    objects_query = f"""
        SELECT table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema = 'normalized'
    """
    objects = duck_conn.execute(objects_query).fetchall()

    if not objects:
        print(f"No tables or views found in DuckDB schema 'normalized'.")
        duck_conn.close()
        return

    tables = [obj[0] for obj in objects if obj[1] == 'BASE TABLE']
    views = [obj[0] for obj in objects if obj[1] == 'VIEW']

    print(f"\nFound {len(tables)} tables and {len(views)} views to migrate.")
    if tables: print(f"Tables: {tables}")
    if views: print(f"Views: {views}")

    # 4. Perform migration
    # We migrate both tables and views as tables in PostgreSQL.
    # This ensures that DuckDB-specific view logic (like STRUCT_PACK) is preserved as data.
    for obj_name, obj_type in objects:
        label = "Table" if obj_type == 'BASE TABLE' else "View"
        print(f"\nMigrating {label}: {obj_name}...")

        try:
            # Read ALL data from DuckDB
            df = duck_conn.execute(f"SELECT * FROM normalized.{obj_name}").df()

            # Write to PostgreSQL
            # If it's a view in DuckDB, it becomes a table in PostgreSQL to preserve the calculated data.
            df.to_sql(
                obj_name, 
                pg_engine, 
                schema=POSTGRES_SCHEMA, 
                if_exists='replace', 
                index=False
            )
            print(f"Successfully migrated {len(df)} rows to {POSTGRES_SCHEMA}.{obj_name}")
            if obj_type == 'VIEW':
                print(f"Note: View '{obj_name}' was migrated as a static table to ensure data compatibility.")
        except Exception as e:
            print(f"Error migrating {obj_name}: {e}")

    duck_conn.close()
    print("\nMigration completed successfully!")

if __name__ == "__main__":
    migrate()

