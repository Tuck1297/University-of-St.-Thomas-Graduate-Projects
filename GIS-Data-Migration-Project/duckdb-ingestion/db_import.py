import duckdb

# Connect to the database
con = duckdb.connect("project_data.duckdb")

# Install and load the spatial extension required for GIS functions
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

# Import the database
try:
    con.execute("IMPORT DATABASE 'project_data_export'")
    print("Database reimport complete.")
except Exception as e:
    print(f"Error during import: {e}")
finally:
    con.close()
