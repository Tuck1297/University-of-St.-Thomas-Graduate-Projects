from db_utils import handle_existing_data

def run(conn, existed):
    if existed:
        if not handle_existing_data(conn, "ridb.facilities", "RIDB Recreation", schema_name="ridb"):
            return

    print("Running RIDB Recreation job")
    # TODO: Implement RIDB Recreation ingestion logic
