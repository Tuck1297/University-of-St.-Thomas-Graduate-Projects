from fastapi import FastAPI, HTTPException, Depends, Query, Response
from fastapi.middleware.cors import CORSMiddleware
import geopandas as gpd
import pandas as pd
import json
from sqlalchemy import create_engine, text
from config import get_settings, Settings
from shapely.geometry import Point

settings = get_settings()
app = FastAPI(title=settings.API_TITLE)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_engine():
    return create_engine(settings.database_url)

@app.get("/")
def read_root():
    return {"message": f"{settings.API_TITLE} is running"}

def parse_attributes(df):
    """Helper to ensure the attributes column is a real JSON object/dict."""
    # Look for both cases as they might vary by DB/Pandas version
    col_name = "ATTRIBUTES" if "ATTRIBUTES" in df.columns else "attributes"
    if col_name in df.columns:
        df[col_name] = df[col_name].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )
    return df

@app.get("/api/locations/box")
def get_locations_in_bbox(
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    settings: Settings = Depends(get_settings)
):
    """
    1. Bounding Box Endpoint
    Retrieves locations within the specified coordinates.
    """
    try:
        engine = get_engine()
        query = text("""
            SELECT
                location_key,
                name,
                description,
                latitude,
                longitude,
                ATTRIBUTES::text,
                location_type_key
            FROM normalized."Locations"
            WHERE active_flag = TRUE
            AND ST_Within(
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            );
        """)
        
        params = {
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat
        }
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
        
        if df.empty:
            return Response(content=json.dumps({"type": "FeatureCollection", "features": []}), media_type="application/json")

        # Manually create GeoDataFrame from lat/lon
        gdf = gpd.GeoDataFrame(
            df, 
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs="EPSG:4326"
        )
        
        gdf = parse_attributes(gdf)
        return Response(content=gdf.to_json(), media_type="application/json")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/locations/search")
def search_locations(
    q: str = Query(..., description="Search term for locations"),
    settings: Settings = Depends(get_settings)
):
    """
    2. Location Search Endpoint
    Searches for locations based on a string parameter.
    """
    try:
        lower_q = q.lower()
        engine = get_engine()
        query = text("""
            SELECT
                location_key,
                name,
                description,
                latitude,
                longitude,
                ATTRIBUTES::text,
                location_type_key,
                relevance_score
            FROM (
                SELECT
                    location_key,
                    name,
                    description,
                    latitude,
                    longitude,
                    ATTRIBUTES,
                    location_type_key,
                    GREATEST(
                        word_similarity(:q, lower(name)),
                        word_similarity(:q, lower(description))
                    ) AS relevance_score
                FROM normalized."Locations"
                WHERE active_flag = TRUE
                AND (
                    name ILIKE :q_pattern
                    OR description ILIKE :q_pattern
                )
            ) scored
            ORDER BY relevance_score DESC
            LIMIT 30;
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"q": lower_q, "q_pattern": f"%{q}%"})
        
        if df.empty:
            return Response(content=json.dumps({"type": "FeatureCollection", "features": []}), media_type="application/json")

        # Manually create GeoDataFrame from lat/lon
        gdf = gpd.GeoDataFrame(
            df, 
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs="EPSG:4326"
        )
        
        gdf = parse_attributes(gdf)
        return Response(content=gdf.to_json(), media_type="application/json")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/locations/{location_key}")
def get_location_by_key(
    location_key: int,
    settings: Settings = Depends(get_settings)
):
    """
    3. Numerical Location Key Endpoint
    Retrieves a specific location by its numerical key.
    """
    try:
        engine = get_engine()
        query = f"""
            SELECT *
            FROM normalized."v_location_details"
            WHERE location_key = {location_key}
            AND active_flag = TRUE
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            raise HTTPException(status_code=404, detail="Location not found")

        # Return the first (and only) record as a dictionary
        return df.iloc[0].to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.API_HOST, port=settings.API_PORT, reload=True)
