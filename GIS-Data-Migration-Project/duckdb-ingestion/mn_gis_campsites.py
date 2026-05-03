import os
from db_utils import handle_existing_data

def run(conn, existed):
    schema_name = "mn_gis_campsites"
    gpkg_dir = os.path.join("downloads", "gpkg_struc_parks_and_trails_campsites")
    gpkg_file = "struc_parks_and_trails_campsites.gpkg"
    file_path = os.path.join(gpkg_dir, gpkg_file)

    if not os.path.exists(file_path):
        print(f"Error: MN GIS GeoPackage not found at {file_path}")
        return

    # Check if we should skip based on existing data
    if existed:
        try:
            # Check if schema exists first
            conn.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
            schema_exists = conn.fetchone()

            if schema_exists:
                # Check for the table
                if not handle_existing_data(conn, f"{schema_name}.struc_parks_and_trails_campsites", "Minnesota GIS Campsites"):
                    return
        except Exception:
            # If table doesn't exist, proceed
            pass

    print("Running Minnesota GIS Campsites job")

    # Install and load spatial extension
    try:
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
    except Exception as e:
        print(f"Error loading spatial extension: {e}")
        return

    # Create schema if it doesn't exist
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # Import GeoPackage layer into table
    table_name = gpkg_file.replace(".gpkg", "")
    print(f"Importing {gpkg_file} into {schema_name}.{table_name}...")
    
    # Use ST_Read from spatial extension
    # We replace backslashes with forward slashes for DuckDB compatibility
    normalized_path = file_path.replace('\\', '/')
    conn.execute(f"""
        CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
        SELECT * FROM ST_Read('{normalized_path}');
    """)

    print("Minnesota GIS Campsites job completed successfully.")
