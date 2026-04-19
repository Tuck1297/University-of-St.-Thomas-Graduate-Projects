import camelot
import pandas as pd
import os
from db_utils import handle_existing_data

def run(conn, existed):
    schema_name = "mn_dnr"
    
    # Check if we should skip based on existing data
    if existed:
        try:
            # Check if schema exists first
            conn.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
            schema_exists = conn.fetchone()

            if schema_exists:
                # Check for one of the tables we expect to create
                if not handle_existing_data(conn, f"{schema_name}.extracted_table_0", "Minnesota DNR", schema_name=schema_name):
                    return
        except Exception:
            # If table doesn't exist, proceed
            pass

    # Ensure schema exists
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    print("Running Minnesota DNR job - Extracting tables from PDF...")

    pdf_path = os.path.join("downloads", "2025_MN_State_Parks_data.pdf")
    
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at {pdf_path}")
        return

    # Attempt extraction with Camelot
    try:
        print("Extracting tables using Camelot (Lattice flavor)...")
        tables = camelot.read_pdf(pdf_path, pages='2', flavor='lattice')
        
        if len(tables) == 0:
            print("No tables found with Lattice flavor. Trying Stream flavor...")
            tables = camelot.read_pdf(pdf_path, pages='2', flavor='stream')

        if len(tables) == 0:
            print("Warning: No tables could be extracted from the PDF.")
            return

        print(f"Found {len(tables)} tables.")

        for i, table in enumerate(tables):
            table_name = f"{schema_name}.extracted_table_{i}"
            df = table.df
            
            # Correct reversed headers if detected
            if i == 0 and not df.empty:
                first_row_str = " ".join(df.iloc[0].astype(str))
                if "RETNIW" in first_row_str:
                    print(f"Detected reversed headers in table {i}, correcting...")
                    df.iloc[0] = df.iloc[0].apply(lambda x: "".join(reversed(str(x))) if x else x)

            # Import into DuckDB
            temp_df_name = f"temp_df_{i}"
            conn.register(temp_df_name, df)
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {temp_df_name}")
            conn.unregister(temp_df_name)
            
            print(f"Successfully imported table {i} into {table_name} ({len(df)} rows)")

        print("Minnesota DNR job completed successfully.")

    except Exception as e:
        print(f"Error during PDF table extraction/import: {e}")

if __name__ == "__main__":
    # This allows running the script standalone for testing
    import duckdb
    DUCKDB_NAME = 'project_data.duckdb'
    conn = duckdb.connect(DUCKDB_NAME)
    run(conn, os.path.exists(DUCKDB_NAME))
