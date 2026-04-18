from db_utils import handle_existing_data

def run(conn, existed):
    if existed:
        if not handle_existing_data(conn, "mn_gis.data", "Minnesota GIS", schema_name="mn_gis"):
            return

    print("Running Minnesota GIS job")
    # TODO: Implement Minnesota GIS ingestion logic
