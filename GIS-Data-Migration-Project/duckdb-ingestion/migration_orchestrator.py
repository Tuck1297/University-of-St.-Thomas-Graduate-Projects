import typer
import duckdb
import os
import sys
from datetime import datetime

"""
Orchestrator module for running various DuckDB SQL transformation and migration scripts.
This module provides a command-line interface (CLI) using Typer to execute
different SQL-based data migration steps independently or all together.

Note: This script assumes that the raw data has already been ingested into
the database (e.g., via orchestrator.py).

Available Transformations:
    - NPS: National Park Service SQL transformation
    - RIDB Recreation: RIDB SQL transformation
    - Google: Google Places SQL transformation
    - MN DNR: Minnesota DNR SQL transformation
    - MN GIS: Minnesota GIS SQL transformation
    - MN GIS Boundaries: Minnesota GIS Boundary SQL transformation
    - MN GIS Campsites: Minnesota GIS Campsite SQL transformation
    - Schema: Create/Reset normalized table schema
    - Sanity: Run data sanity checks

Usage:
    python migration_orchestrator.py --restart           # Reset schema and start fresh
    python migration_orchestrator.py --run-nps          # Run only NPS SQL transformation
    python migration_orchestrator.py --run-ridb         # Run only RIDB SQL transformation
    python migration_orchestrator.py --run-google       # Run only Google Places SQL transformation
    python migration_orchestrator.py --run-mn-dnr       # Run only MN DNR SQL transformation
    python migration_orchestrator.py --run-mn-gis       # Run only MN GIS SQL transformation
    python migration_orchestrator.py --run-mn-gis-boundary  # Run only MN GIS Boundary SQL transformation
    python migration_orchestrator.py --run-mn-gis-campsites  # Run only MN GIS Campsite SQL transformation
    python migration_orchestrator.py --all              # Run all transformation scripts
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
    'mn_gis_boundary': 'DuckDB SQL Queries - Migrate MN GIS Boundary Data.sql',
    'mn_gis_campsites': 'DuckDB SQL Queries - Migrate MN GIS Campsite Data.sql',
    'mn_merge': 'DuckDB SQL Queries - Merge MN DNR and GIS Data.sql',
    'campsites_associate': 'DuckDB SQL Queries - Associate MN Campgrounds to Merged Locations.sql',
    'sanity': 'DuckDB SQL Queries - Data Sanity Check.sql'
}

def get_connect_duckdb():
    if os.path.exists(DUCKDB_NAME):
        print(f"Connecting to existing DuckDB database: {DUCKDB_NAME}")
    else:
        print(f"Error: DuckDB database not found: {DUCKDB_NAME}. Run orchestrator.py first.")
        sys.exit(1)

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
    run_nps_flag: bool = typer.Option(False, "--run-nps", help="Transform NPS data"),
    run_ridb_flag: bool = typer.Option(False, "--run-ridb", help="Transform RIDB data"),
    run_google_flag: bool = typer.Option(False, "--run-google", help="Transform Google Places data"),
    run_mn_dnr_flag: bool = typer.Option(False, "--run-mn-dnr", help="Transform MN DNR data"),
    run_mn_gis_flag: bool = typer.Option(False, "--run-mn-gis", help="Transform MN GIS data"),
    run_mn_gis_boundary_flag: bool = typer.Option(False, "--run-mn-gis-boundary", help="Transform MN GIS Boundary data"),
    run_mn_gis_campsites_flag: bool = typer.Option(False, "--run-mn-gis-campsites", help="Transform MN GIS Campsite data"),
    all_flag: bool = typer.Option(False, "--all", help="Run all transformation steps"),
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

    if success and (all_flag or run_mn_gis_boundary_flag):
        if not run_sql_script(conn, SCRIPTS['mn_gis_boundary']):
            success = False

    # Run merge if any MN source was updated or all flag is set
    # Moved BEFORE campsites to avoid FK issues with deactivated parents
    if success and (all_flag or run_mn_dnr_flag or run_mn_gis_flag or run_mn_gis_boundary_flag):
        if not run_sql_script(conn, SCRIPTS['mn_merge']):
            success = True # Continue if merge fails? No, keep False.
            success = False

    # Elevate master records to ID 9 BEFORE they become parents
    if success and (all_flag or (run_mn_gis_campsites_flag and (run_mn_gis_boundary_flag or run_mn_dnr_flag))):
        if not run_sql_script(conn, SCRIPTS['campsites_associate']):
            success = False

    if success and (all_flag or run_mn_gis_campsites_flag):
        if not run_sql_script(conn, SCRIPTS['mn_gis_campsites']):
            success = False
            
    if success and sanity:
        if not run_sql_script(conn, SCRIPTS['sanity']):
            success = False

    conn.close()

    if success:
        if any([restart, run_nps_flag, run_ridb_flag, run_google_flag, run_mn_dnr_flag, run_mn_gis_flag, run_mn_gis_boundary_flag, run_mn_gis_campsites_flag, all_flag, sanity]):
            print("\nTransformation process completed successfully.")
        else:
            print("No steps selected. Use --help for options.")
    else:
        print("\nTransformation process failed.")
        sys.exit(1)

if __name__ == "__main__":
    app()