# GIS Data Migration API (Backend)

This is a FastAPI-based backend service designed to migrate and serve GIS data from a PostGIS database. It uses GeoPandas for spatial data handling and SQLAlchemy for database connectivity.

## Prerequisites

- Python 3.10+
- PostgreSQL with PostGIS extension installed locally.
- A virtual environment (`venv`) already created in this directory.

## Installation

1. **Activate the Virtual Environment**:
   - **Windows**:
     ```powershell
     .\venv\Scripts\activate
     ```
   - **macOS/Linux**:
     ```bash
     source venv/bin/activate
     ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

The application uses environment variables managed by a `.env` file.

1. **Setup Environment File**:
   Copy the example file to create your local configuration:
   ```bash
   copy .env.example .env
   ```

2. **Configure Database**:
   Open `.env` and update the following variables with your local PostgreSQL credentials.

## Running the Server

Start the FastAPI development server with auto-reload enabled:

```bash
python main.py
```

The API will be accessible at `http://localhost:8000`.

## API Endpoints

### 1. Bounding Box Search
- **URL**: `GET /api/locations/bbox`
- **Query Params**: `min_lon`, `min_lat`, `max_lon`, `max_lat`
- **Description**: Returns locations within the specified map view coordinates.

### 2. Location Search
- **URL**: `GET /api/locations/search`
- **Query Params**: `q` (search string)
- **Description**: Searches locations by name or attributes.

### 3. Location Detail
- **URL**: `GET /api/locations/{location_key}`
- **Path Param**: `location_key` (Integer)
- **Description**: Returns detailed information for a specific location by its numerical ID.

## Documentation
Once the server is running, you can access the interactive API documentation:
- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
