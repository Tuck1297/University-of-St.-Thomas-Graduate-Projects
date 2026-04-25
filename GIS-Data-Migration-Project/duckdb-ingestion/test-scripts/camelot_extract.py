import camelot
import pandas as pd
import os

pdf_path = "downloads/2025_MN_State_Parks_data.pdf"
output_csv_prefix = "project_data_export/mn_state_parks_camelot"

def camelot_extract_table():
    if not os.path.exists("project_data_export"):
        os.makedirs("project_data_export")

    print(f"Attempting extraction with Camelot (Lattice flavor)...")
    try:
        # Lattice: for tables with lines
        # Note: This might require Ghostscript on Windows
        tables = camelot.read_pdf(pdf_path, pages='2', flavor='lattice')
        print(f"Lattice found {len(tables)} tables.")
        if len(tables) > 0:
            for i, table in enumerate(tables):
                output_path = f"{output_csv_prefix}_lattice_{i}.csv"
                table.to_csv(output_path)
                print(f"Saved Lattice table {i} to {output_path}")
    except Exception as e:
        print(f"Lattice extraction failed or requires Ghostscript: {e}")

    print(f"\nAttempting extraction with Camelot (Stream flavor)...")
    try:
        # Stream: for tables with whitespace
        tables = camelot.read_pdf(pdf_path, pages='2', flavor='stream')
        print(f"Stream found {len(tables)} tables.")
        if len(tables) > 0:
            for i, table in enumerate(tables):
                output_path = f"{output_csv_prefix}_stream_{i}.csv"
                table.to_csv(output_path)
                print(f"Saved Stream table {i} to {output_path}")
                
                # Show first few rows
                print(f"Preview of table {i}:")
                print(table.df.head())
    except Exception as e:
        print(f"Stream extraction failed: {e}")

if __name__ == "__main__":
    camelot_extract_table()
