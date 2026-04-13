# DuckDB Ingestion Guide: Zip & CSV Workflow

This guide explains how to use Python to download a zipped archive from a URL, extract its contents, and ingest the CSV data into DuckDB.

## 1. Prerequisites
Ensure you have the required libraries installed:
```bash
pip install duckdb requests python-dotenv
```
*(Note: `zipfile` and `os` are part of the Python Standard Library and do not need installation.)*

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