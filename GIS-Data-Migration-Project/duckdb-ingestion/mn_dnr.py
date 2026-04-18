from db_utils import handle_existing_data

def run(conn, existed):
    if existed:
        if not handle_existing_data(conn, "mn_dnr.data", "Minnesota DNR", schema_name="mn_dnr"):
            return

    print("Running Minnesota DNR job")
    # TODO: Implement Minnesota DNR ingestion logic
