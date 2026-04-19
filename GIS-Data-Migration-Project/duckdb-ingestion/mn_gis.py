import os
from db_utils import handle_existing_data

def run(conn, existed):
    schema_name = "mn_gis"
    gis_dir = os.path.join("downloads", "shp_bdry_dnr_lrs_prk")

    if not os.path.exists(gis_dir):
        print(f"Error: MN GIS directory not found at {gis_dir}")
        return

    # Check if we should skip based on existing data
    if existed:
        try:
            # Check if schema exists first
            conn.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
            schema_exists = conn.fetchone()

            if schema_exists:
                # Check for one of the tables
                if not handle_existing_data(conn, f"{schema_name}.dnr_management_units_prk", "Minnesota GIS"):
                    return
        except Exception:
            # If table doesn't exist, proceed
            pass

    print("Running Minnesota GIS job")

    # Install and load spatial extension
    try:
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
    except Exception as e:
        print(f"Error loading spatial extension: {e}")
        return

    # Create schema
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # List of shapefiles to import
    shapefiles = [
        "dnr_management_units_prk.shp",
        "dnr_management_units_prk_ref_pts.shp",
        "dnr_stat_plan_areas_prk.shp"
    ]

    for shp_file in shapefiles:
        table_name = shp_file.replace(".shp", "")
        file_path = os.path.join(gis_dir, shp_file)

        if os.path.exists(file_path):
            print(f"Importing {shp_file} into {schema_name}.{table_name}...")
            # Use ST_Read from spatial extension
            conn.execute(f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
                SELECT * FROM ST_Read('{file_path.replace('\\', '/')}');
            """)
        else:
            print(f"Warning: File {shp_file} not found in {gis_dir}")

    gis_dir = os.path.join("downloads", "shp_struc_state_forest_campgrounds")

    if not os.path.exists(gis_dir):
        print(f"Error: MN GIS directory not found at {gis_dir}")
        return

    # Check if we should skip based on existing data
    if existed:
        try:
            # Check if schema exists first
            conn.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
            schema_exists = conn.fetchone()

            if schema_exists:
                # Check for one of the tables
                if not handle_existing_data(conn, f"{schema_name}.state_forest_campgrounds_and_day_use_areas_orig", "Minnesota GIS"):
                    return
        except Exception:
            # If table doesn't exist, proceed
            pass

    print("Running Minnesota GIS job")

    # List of shapefiles to import
    shapefiles = [
        "state_forest_campgrounds_and_day_use_areas_orig.shp",
        "state_forest_campgrounds_and_day_use_areas.shp"
    ]

    for shp_file in shapefiles:
        table_name = shp_file.replace(".shp", "")
        file_path = os.path.join(gis_dir, shp_file)

        if os.path.exists(file_path):
            print(f"Importing {shp_file} into {schema_name}.{table_name}...")
            # Use ST_Read from spatial extension
            conn.execute(f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
                SELECT * FROM ST_Read('{file_path.replace('\\', '/')}');
            """)
        else:
            print(f"Warning: File {shp_file} not found in {gis_dir}")

    print("Minnesota GIS job completed successfully.")