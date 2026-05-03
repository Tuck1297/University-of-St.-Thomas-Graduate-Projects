# DuckDB Data Ingestion Notes

## Install Dependencies

- To run this script you first will need to start up a python virtual environment and install script dependencies. The steps are outlined below:

1. Create and Start Virtual Environment

### For Windows

```
python -m venv venv # Create Virtual Environment -- You will only need to do this once.

./venv/Scripts/activate # Activate the Virtual Environment
```

### For macOS / Linux

```
python3 -m venv venv # Create Virtual Environment -- You will only need to do this once.

source venv/bin/activate # Activate the Virtual Environment
```

2. Install Script dependencies

```
pip install -r requirements.txt
```

3. Run script -- Review the **Invoke Script** and **Migration Orchestrator** sections below to learn how to run the ingestion and migration workflows.

## Invoke Script (Data Ingestion)

The `orchestrator.py` module is responsible for fetching raw data from external sources (NPS, Google, RIDB, MN GIS, MN DNR) and ingesting it into DuckDB. It provides a command-line interface (CLI) using Typer.

### Available Ingestion Jobs:

- NPS: National Park Service data ingestion
- Google: Google Places data ingestion
- RIDB Recreation: Recreation Information Database ingestion
- Minnesota GIS: Minnesota Geographic Information System data ingestion
- Minnesota DNR: Minnesota Department of Natural Resources data ingestion

### Usage:

- `python orchestrator.py --run-nps`       # Run only NPS ingestion
- `python orchestrator.py --run-google`    # Run only Google Places ingestion
- `python orchestrator.py --run-ridb-rec` # Run only RIDB Recreation ingestion
- `python orchestrator.py --run-mn-gis`    # Run only Minnesota GIS ingestion
- `python orchestrator.py --run-mn-gis-campsite` # Run only Minnesota GIS Campsites ingestion
- `python orchestrator.py --run-mn-dnr`    # Run only Minnesota DNR ingestion
- `python orchestrator.py --all`           # Run all ingestion jobs

## Migration Orchestrator (Data Transformation)

The `migration_orchestrator.py` module orchestrates the execution of SQL scripts to transform raw ingested data into a normalized schema. This script is used after the initial ingestion is complete.

### Available Transformation Steps:

- **Schema Setup:** Creates or resets the normalized table structure.
- **NPS Migration:** Transforms raw NPS data into normalized tables.
- **RIDB Migration:** Transforms raw RIDB data (including Tours and Links).
- **Google Migration:** Transforms raw Google Places data.
- **MN DNR Migration:** Transforms raw MN DNR PDF-extracted data.
- **MN GIS Migration:** Transforms raw MN GIS spatial data.
- **Sanity Checks:** Runs verification queries to ensure data integrity.

### Usage:

- `python migration_orchestrator.py --restart`               # Reset schema and start fresh
- `python migration_orchestrator.py --run-nps`               # Run NPS SQL migration
- `python migration_orchestrator.py --run-ridb`              # Run RIDB SQL migration
- `python migration_orchestrator.py --run-google`            # Run Google SQL migration
- `python migration_orchestrator.py --run-mn-dnr`            # Run only MN DNR SQL transformation
- `python migration_orchestrator.py --run-mn-gis`            # Run only MN GIS SQL transformation
- `python migration_orchestrator.py --run-mn-gis-boundary`   # Run only MN GIS Boundary SQL transformation
- `python migration_orchestrator.py --run-mn-gis-campsites`  # Run only MN GIS Campsite SQL transformation
- `python migration_orchestrator.py --all`                   # Run all migration scripts
- `python migration_orchestrator.py --all --sanity`          # Run all and perform sanity checks

### Options:

- `--restart`: (bool) Reset the database and recreate normalized tables
- `--run-nps`: (bool) Migrate NPS data
- `--run-ridb`: (bool) Migrate RIDB data
- `--run-google`: (bool) Migrate Google Places data
- `--run-mn-dnr`: (bool) Migrate MN DNR data
- `--run-mn-gis`: (bool) Migrate MN GIS data
- `--run-mn-gis-boundary`: (bool) Migrate MN GIS Boundary data
- `--run-mn-gis-campsites`: (bool) Migrate MN GIS Campsites data
- `--all`: (bool) Execute all available transformation scripts
- `--sanity`: (bool) Run data sanity checks
- `--help`: Display help message with all available options

## Export Database

To export the database into CSV files that are easier to commit to a GitHub repository run the following script:

```
python db_export.py
```

## Import Database backup

To import the database from CSV files that exist in `project_data_export` run the following python script:

```
python db_import.py
```

## Freeze requirements.txt after dependencies updates

```
pip freeze > requirements.txt
```

## Common DuckDB CMD Commands

- Show all Tables

```
pragma show_tables;
```

- Connect to DuckDB Database

```
duckdb [DATABASE FILE].duckdb
```

# DuckDB Ingestion Guide: Zip & CSV Workflow

This guide explains how to use Python to download a zipped archive from a URL, extract its contents, and ingest the CSV data into DuckDB.

## 1. Prerequisites

Ensure you have the required libraries installed:

```bash
pip install duckdb requests python-dotenv
```

_(Note: `zipfile` and `os` are part of the Python Standard Library and do not need installation.)_

## 2. Environment Variables (.env)

To keep your script configurable without hardcoding values, use a `.env` file.

**Create a `.env` file in this directory:**

```env
DATA_SOURCE_URL=https://example.com/data.zip
DATABASE_NAME=my_project.db
```

**Accessing them in Python:**

```python
import os
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env into os.environ

URL = os.getenv("DATA_SOURCE_URL")
DB_PATH = os.getenv("DATABASE_NAME")
```

## 3. Implementation Strategy

### A. Download the Zip File

Use the `requests` library to fetch the file. It's best to stream the download for larger files to save memory.

### B. Extract the Archive

Use the `zipfile` module to extract the files to a temporary directory.

### C. Ingest into DuckDB

Use DuckDB's `read_csv_auto()` function. DuckDB is highly efficient at reading local CSVs and can often infer types automatically.

## 3. Complete Python Script Example

```python
import requests
import zipfile
import os
import duckdb

# Configuration
URL = "https://example.com/data.zip"  # Replace with your link
ZIP_PATH = "downloaded_data.zip"
EXTRACT_DIR = "extracted_data"
DB_PATH = "my_project.db"

def download_and_ingest():
    # 1. Download the zip file
    print(f"Downloading from {URL}...")
    response = requests.get(URL, stream=True)
    with open(ZIP_PATH, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # 2. Unzip the file
    print("Extracting files...")
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)

    # 3. Initialize DuckDB and Ingest CSVs
    con = duckdb.connect(DB_PATH)

    # Iterate through extracted files and upload CSVs
    for filename in os.listdir(EXTRACT_DIR):
        if filename.endswith(".csv"):
            table_name = os.path.splitext(filename)[0].replace("-", "_")
            file_path = os.path.join(EXTRACT_DIR, filename)

            print(f"Ingesting {filename} into table '{table_name}'...")
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")

    print("Ingestion complete!")
    con.close()

if __name__ == "__main__":
    download_and_ingest()
```

## 4. Pro Tip: Performance

If you have a very large CSV, you don't actually need to "upload" it line-by-line. DuckDB's `CREATE TABLE ... AS SELECT` syntax performs a bulk import that is significantly faster than standard SQL `INSERT` statements.

## 5. NPS Data to introduce later if I have time

- visitorcenters
- webcams
