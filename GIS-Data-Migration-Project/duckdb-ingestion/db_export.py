import duckdb

con = duckdb.connect("project_data.duckdb")
# Using PARQUET format to preserve NULL vs empty string distinction and handle complex types
con.execute("EXPORT DATABASE 'project_data_export' (FORMAT PARQUET)")
con.close()

