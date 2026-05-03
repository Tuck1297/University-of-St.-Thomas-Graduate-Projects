-- DuckDB SQL Queries - Migrate MN GIS Boundary Data

-- Load Spatial Extension
INSTALL spatial;
LOAD spatial;

-- Views Created for MN GIS Boundary Data Migration

-- View unique designations from the GIS boundary data
CREATE OR REPLACE VIEW mn_gis.v_boundary_location_types AS
SELECT DISTINCT AREA_TYPE AS location_type FROM mn_gis.dnr_stat_plan_areas_prk WHERE AREA_TYPE IS NOT NULL;

-- View to transform DNR Statutory Planning Areas (Boundaries)
CREATE OR REPLACE VIEW mn_gis.v_boundary_locations AS
SELECT
    AREA_NAME AS name,
    ST_X(ST_Centroid(ST_Transform(geom, 'EPSG:26915', 'EPSG:4326'))) AS latitude,
    ST_Y(ST_Centroid(ST_Transform(geom, 'EPSG:26915', 'EPSG:4326'))) AS longitude,
    'MN' AS state_abbre,
    'Minnesota' AS state,
    AREA_TYPE AS location_type,
    7 AS data_source_key, -- MN GIS Boundaries
    CAST(OGC_FID AS VARCHAR) AS orig_data_source_key,
    OGC_FID AS migration_primary_key,
    TO_JSON(STRUCT_PACK(
        gis_acres := GIS_ACRES,
        legislative_id := LEGISLATIV,
        program_project := PGRM_PROJE,
        guid := GUID
    )) AS attributes
FROM mn_gis.dnr_stat_plan_areas_prk;

-- Start Transaction

BEGIN TRANSACTION;

-- 1. Ensure the data source exists
INSERT INTO normalized.DataSources (data_source_key, name, description)
SELECT 7, 'MN GIS Boundary Data', 'Boundary data sourced from the Minnesota Geospatial Commons website.'
WHERE NOT EXISTS (SELECT 1 FROM normalized.DataSources WHERE data_source_key = 7);

-- 2. Migrate unique designations to the location types dimension table
INSERT INTO normalized.LocationTypes (location_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(location_type_key), 0) FROM normalized.LocationTypes) + row_number() OVER (),
    location_type,
    'MN GIS Boundary Designation'
FROM mn_gis.v_boundary_location_types
WHERE location_type NOT IN (SELECT name FROM normalized.LocationTypes);

-- 3. Migrate Boundary Locations
-- Use LEFT JOIN to ensure we don't lose records if types are missing, or better, use the IDs we just inserted.
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, state_abbre, state, data_source_key, location_type_key, orig_data_source_key, migration_primary_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.state_abbre, v.state, v.data_source_key, 
    COALESCE(lt.location_type_key, 0), -- Default to 0 (UNKNOWN) if join fails, though it shouldn't
    v.orig_data_source_key, v.migration_primary_key, v.attributes
FROM mn_gis.v_boundary_locations v
LEFT JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

COMMIT;
