import typer
import duckdb
import os
import sys
"""
Orchestrator module for running various GIS data ingestion jobs.
This module provides a command-line interface (CLI) using Typer to execute
different data migration jobs independently or all together. The data
is ingested and imported into a single DuckDB database for further
analysis, querying and ingestion.

Available Jobs:
    - NPS: National Park Service data ingestion
    - Google: Google Places data ingestion
    - RIDB Recreation: Recreation Information Database ingestion
    - Minnesota GIS Units: Minnesota Geographic Information System data ingestion (Parks, Forests)
    - Minnesota GIS Campsites: Minnesota GIS Campsite GeoPackage ingestion
    - Minnesota DNR: Minnesota Department of Natural Resources data ingestion (PDF Extraction)

Usage:
    python orchestrator.py --run-nps              # Run only NPS job
    python orchestrator.py --run-google           # Run only Google Places job
    python orchestrator.py --run-ridb-rec         # Run only RIDB Recreation job
    python orchestrator.py --run-mn-gis           # Run only Minnesota GIS Units job
    python orchestrator.py --run-mn-gis-campsite  # Run only Minnesota GIS Campsite job
    python orchestrator.py --run-mn-dnr           # Run only Minnesota DNR job
    python orchestrator.py --all                  # Run all jobs
    python orchestrator.py                        # No jobs run (displays message)

Options:
    --run-nps:             (bool) Execute NPS data ingestion job
    --run-google:          (bool) Execute Google Places data ingestion job
    --run-ridb-rec:        (bool) Execute RIDB Recreation data ingestion job
    --run-mn-gis:          (bool) Execute Minnesota GIS Units data ingestion job
    --run-mn-gis-campsite: (bool) Execute Minnesota GIS Campsite data ingestion job
    --run-mn-dnr:          (bool) Execute Minnesota DNR data ingestion job
    --all:                 (bool) Execute all available jobs
    --help:                Display help message with all available options

Examples:
    Run multiple jobs: python orchestrator.py --run-nps --run-google
    Run all jobs: python orchestrator.py --all
"""
import nps
import google
import ridb_rec
import mn_gis
import mn_gis_campsites
import mn_dnr

DUCKDB_NAME = 'project_data.duckdb'

def get_connect_duckdb():
    db_exists = os.path.exists(DUCKDB_NAME)

    if db_exists:
        print(f"Connecting to existing DuckDB database: {DUCKDB_NAME}")
    else:
        print(f"Creating new DuckDB database: {DUCKDB_NAME}")

    conn = duckdb.connect(DUCKDB_NAME)
    return conn, db_exists

app = typer.Typer()

@app.command()
def run(
    run_nps_flag: bool = typer.Option(False, "--run-nps", help="Run NPS job"),
    run_google_flag: bool = typer.Option(False, "--run-google", help="Run Google Places job"),
    run_ridb_rec_flag: bool = typer.Option(False, "--run-ridb-rec", help="Run RIDB Recreation job"),
    run_mn_gis_flag: bool = typer.Option(False, "--run-mn-gis", help="Run Minnesota GIS Units job"),
    run_mn_gis_campsite_flag: bool = typer.Option(False, "--run-mn-gis-campsite", help="Run Minnesota GIS Campsite job"),
    run_mn_dnr_flag: bool = typer.Option(False, "--run-mn-dnr", help="Run Minnesota DNR job"),
    all_flag: bool = typer.Option(False, "--all", help="Run all jobs")
):
    # Ensure we are in the correct directory if run from project root
    if not os.path.exists('downloads') and os.path.exists(os.path.join('duckdb-ingestion', 'downloads')):
        os.chdir('duckdb-ingestion')

    conn, existed = get_connect_duckdb()

    if run_nps_flag:
        nps.run(conn, existed)

    if run_google_flag:
        google.run(conn, existed)

    if run_ridb_rec_flag:
        ridb_rec.run(conn, existed)

    if run_mn_gis_flag:
        mn_gis.run(conn, existed)

    if run_mn_gis_campsite_flag:
        mn_gis_campsites.run(conn, existed)

    if run_mn_dnr_flag:
        mn_dnr.run(conn, existed)

    if all_flag:
        nps.run(conn, existed)
        google.run(conn, existed)
        ridb_rec.run(conn, existed)
        mn_gis.run(conn, existed)
        mn_gis_campsites.run(conn, existed)
        mn_dnr.run(conn, existed)

    if not any([run_nps_flag, run_google_flag, run_ridb_rec_flag, run_mn_gis_flag, run_mn_gis_campsite_flag, run_mn_dnr_flag, all_flag]):
        print("No scripts selected")

    conn.close()

if __name__ == "__main__":
    app()