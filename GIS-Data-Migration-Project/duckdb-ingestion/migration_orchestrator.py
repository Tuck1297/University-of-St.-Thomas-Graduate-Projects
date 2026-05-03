import typer
import duckdb
import os
import sys
from datetime import datetime

"""
Orchestrator module for running various DuckDB SQL transformation and migration scripts.
This module provides a command-line interface (CLI) using Typer to execute
different SQL-based data migration steps independently or all together.

Available Transformations:
    - NPS: National Park Service SQL transformation
    - RIDB Recreation: RIDB SQL transformation
    - Google: Google Places SQL transformation
    - MN DNR: Minnesota DNR SQL transformation
    - MN GIS: Minnesota GIS SQL transformation
    - Schema: Create/Reset normalized table schema
    - Sanity: Run data sanity checks

Usage:
    python migration_orchestrator.py --restart           # Reset schema and start fresh
    python migration_orchestrator.py --run-nps          # Run only NPS SQL migration
    python migration_orchestrator.py --run-ridb         # Run only RIDB SQL migration
    python migration_orchestrator.py --all              # Run all migration scripts
    python migration_orchestrator.py --all --sanity     # Run all and perform sanity checks
"""

# Configuration
DUCKDB_NAME = 'project_data.duckdb'
SQL_DIR = 'SQL Scripts'

SCRIPTS = {
    'schema': 'DuckDB SQL Queries - Create Normalized Tables.sql',
    'nps': 'DuckDB SQL Queries - Migrate NPS Data.sql',
    'ridb': 'DuckDB SQL Queries - Migrate RIDB Rec Data.sql',
    'google': 'DuckDB SQL Queries - Migrate Google Places Data.sql',
    'mn_dnr': 'DuckDB SQL Queries - Migrate MN DNR Data.sql',
    'mn_gis': 'DuckDB SQL Queries - Migrate MN GIS Data.sql',
    'sanity': 'DuckDB SQL Queries - Data Sanity Check.sql'
}

def get_connect_duckdb():
    if os.path.exists(DUCKDB_NAME):
        print(f"Connecting to existing DuckDB database: {DUCKDB_NAME}")
    else:
        print(f"Creating new DuckDB database: {DUCKDB_NAME}")

    conn = duckdb.connect(DUCKDB_NAME)
    return conn

def run_sql_script(conn, script_name):
    script_path = os.path.join(SQL_DIR, script_name)
    if not os.path.exists(script_path):
        print(f"Error: Script not found: {script_path}")
        return False
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Executing {script_name}...")
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        conn.execute(sql)
        print(f"Successfully completed {script_name}")
        return True
    except Exception as e:
        print(f"Error executing {script_name}: {e}")
        return False

app = typer.Typer()

@app.command()
def run(
    restart: bool = typer.Option(False, "--restart", help="Reset the database and recreate normalized tables"),
    run_nps_flag: bool = typer.Option(False, "--run-nps", help="Migrate NPS data"),
    run_ridb_flag: bool = typer.Option(False, "--run-ridb", help="Migrate RIDB data"),
    run_google_flag: bool = typer.Option(False, "--run-google", help="Migrate Google Places data"),
    run_mn_dnr_flag: bool = typer.Option(False, "--run-mn-dnr", help="Migrate MN DNR data"),
    run_mn_gis_flag: bool = typer.Option(False, "--run-mn-gis", help="Migrate MN GIS data"),
    all_flag: bool = typer.Option(False, "--all", help="Run all migration steps"),
    sanity: bool = typer.Option(False, "--sanity", help="Run data sanity checks")
):
    # Ensure we are in the correct directory if run from project root
    if not os.path.exists(SQL_DIR) and os.path.exists(os.path.join('duckdb-ingestion', SQL_DIR)):
        os.chdir('duckdb-ingestion')

    conn = get_connect_duckdb()

    success = True

    if restart:
        if not run_sql_script(conn, SCRIPTS['schema']):
            success = False

    if success and (all_flag or run_nps_flag):
        if not run_sql_script(conn, SCRIPTS['nps']):
            success = False

    if success and (all_flag or run_ridb_flag):
        if not run_sql_script(conn, SCRIPTS['ridb']):
            success = False

    if success and (all_flag or run_google_flag):
        if not run_sql_script(conn, SCRIPTS['google']):
            success = False

    if success and (all_flag or run_mn_dnr_flag):
        if not run_sql_script(conn, SCRIPTS['mn_dnr']):
            success = False

    if success and (all_flag or run_mn_gis_flag):
        if not run_sql_script(conn, SCRIPTS['mn_gis']):
            success = False

    if success and sanity:
        if not run_sql_script(conn, SCRIPTS['sanity']):
            success = False

    conn.close()

    if success:
        if any([restart, run_nps_flag, run_ridb_flag, run_google_flag, run_mn_dnr_flag, run_mn_gis_flag, all_flag, sanity]):
            print("\nMigration process completed successfully.")
        else:
            print("No steps selected. Use --help for options.")
    else:
        print("\nMigration process failed.")
        sys.exit(1)

if __name__ == "__main__":
    app()
