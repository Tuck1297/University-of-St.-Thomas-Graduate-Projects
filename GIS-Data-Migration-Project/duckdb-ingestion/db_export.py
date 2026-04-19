import duckdb

con = duckdb.connect("project_data.duckdb")
con.execute("EXPORT DATABASE 'project_data_export'")
con.close()

