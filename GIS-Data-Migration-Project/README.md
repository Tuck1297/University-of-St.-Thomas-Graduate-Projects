# GIS Data Migration & Visualization Project

This project demonstrates a data pipeline from various sources into DuckDB, migration to a PostGIS-enabled PostgreSQL database, and visualization via a Django API and React frontend.

## Project Structure

- `duckdb-ingestion/`: Python scripts for downloading data, importing into DuckDB, and organizing the data into a normalized schema.
- `migration-scripts/`: Scripts to migrate normalized data from DuckDB to PostgreSQL (PostGIS).
- `django-backend/`: API built with Django REST Framework connecting to PostgreSQL.
- `react-frontend/`: React application with a map interface to display spatial data.

## Workflow Overview
1. **Ingest**: Fetch data from external sources and store in local DuckDB.
2. **Migrate**: Transfer and transform data into a production-ready PostgreSQL/PostGIS database.
3. **Serve**: Expose data through a RESTful API.
4. **Visualize**: Display geospatial data on an interactive map.

## Data Resources
Below are the sources that I have discovered thus far to try to integrate into my final project. I'm planning on starting with the location data and if time allows for it, I'll extend the project into hiking trail and campsite data.

### Location Data
- https://ridb.recreation.gov/download
- https://www.nps.gov/subjects/developer/api-documentation.htm
- https://gisdata.mn.gov/dataset/bdry-dnr-lrs-prk
- https://gisdata.mn.gov/dataset/struc-dnr-wld-mgmt-areas-pub
- https://gisdata.mn.gov/dataset/bdry-dnr-managed-areas
- https://files.dnr.state.mn.us/destinations/state_parks/publications/pat_guide/2025.pdf - Manual Map that provides MN State Park Activity information. May need to collect lat/lon information from Google or OpenStreetMaps
- https://www.tpl.org/park-data-downloads -- shapefile
- Google Places (New) API
- https://gis.data.mn.gov/datasets/c76bcca7eb564523b6a1cf2807cbc481_3/explore?location=45.235305%2C-93.037062%2C10

### Hiking Trail Data
- https://www.fws.gov/service/national-wildlife-refuge-system-gis-data-and-mapping-tools
- https://gisdata.mn.gov/dataset/trans-state-trails-minnesota
- https://gisdata.mn.gov/dataset/trans-state-park-trails-roads

### Campsite Data
- https://ridb.recreation.gov/download
- https://www.nps.gov/subjects/developer/api-documentation.htm
- https://gisdata.mn.gov/dataset/struc-state-forest-campgrounds
- https://gisdata.mn.gov/dataset/struc-parks-and-trails-campsites


### Used Resources for Documentation and Citing purposes
- https://duckdb.org/docs/current/
- https://chatgpt.com/share/69e556f5-b744-83ea-99ff-f87fb17c2c13
