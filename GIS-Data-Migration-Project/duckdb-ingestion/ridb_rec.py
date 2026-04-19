import os
from db_utils import handle_existing_data

def run(conn, existed):
    schema_name = "ridb"
    ridb_dir = os.path.join("downloads", "RIDBFullExport_V1_CSV")

    if not os.path.exists(ridb_dir):
        print(f"Error: RIDB directory not found at {ridb_dir}")
        return

    # Check if we should skip based on existing data
    # We'll check for the 'RecAreas' table as a proxy for the entire RIDB schema
    if existed:
        try:
            # Check if schema exists first
            conn.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
            schema_exists = conn.fetchone()

            if schema_exists:
                if not handle_existing_data(conn, f"{schema_name}.RecAreas_API_v1", "RIDB Recreation"):
                    return
        except Exception:
            # If table doesn't exist, proceed
            pass

    print("Running RIDB Recreation job")

    # Create schema
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # List of CSV files to import
    csv_files = [
        "Activities_API_v1.csv",
        "CampsiteAttributes_API_v1.csv",
        "Campsites_API_v1.csv",
        "EntityActivities_API_v1.csv",
        "Events_API_v1.csv",
        "Facilities_API_v1.csv",
        "FacilityAddresses_API_v1.csv",
        "Links_API_v1.csv",
        "Media_API_v1.csv",
        "MemberTours_API_v1.csv",
        "Organizations_API_v1.csv",
        "OrgEntities_API_v1.csv",
        "PermitEntranceAttributes_API_v1.csv",
        "PermitEntrances_API_v1.csv",
        "PermitEntranceZones_API_v1.csv",
        "PermittedEquipment_API_v1.csv",
        "RecAreaAddresses_API_v1.csv",
        "RecAreaFacilities_API_v1.csv",
        "RecAreas_API_v1.csv",
        "TourAttributes_API_v1.csv",
        "Tours_API_v1.csv"
    ]

    for csv_file in csv_files:
        table_name = csv_file.replace(".csv", "")
        file_path = os.path.join(ridb_dir, csv_file)

        if os.path.exists(file_path):
            print(f"Importing {csv_file} into {schema_name}.{table_name}...")

            # Use read_csv_auto with robust options for handling commas in quotes
            # quote='"' and escape='"' ensure quoted fields are parsed correctly
            # sample_size=-1 forces DuckDB to sample the entire file for better type detection
            conn.execute(f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS 
                SELECT * FROM read_csv_auto('{file_path.replace('\\', '/')}', quote='"', escape='"', sample_size=-1);
            """)
        else:
            print(f"Warning: File {csv_file} not found in {ridb_dir}")
    print("RIDB Recreation job completed successfully.")
