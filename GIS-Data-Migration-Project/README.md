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
