-- DuckDB SQL Queries - Migrate MN GIS Data

-- Views Created for MN GIS Data Migration

-- View unique designations from the GIS data to populate the location types dimension table
CREATE OR REPLACE VIEW mn_gis.v_location_types AS
SELECT DISTINCT UNIT_TYPE AS location_type FROM mn_gis.dnr_management_units_prk WHERE UNIT_TYPE IS NOT NULL
UNION
SELECT 'State Forest Campground' AS location_type;

-- View to transform DNR Management Units
CREATE OR REPLACE VIEW mn_gis.v_management_units AS
SELECT
    UNIT_NAME AS name,
    ST_Y(ST_Centroid(geom)) AS latitude,
    ST_X(ST_Centroid(geom)) AS longitude,
    'MN' AS state_abbre,
    'Minnesota' AS state,
    UNIT_TYPE AS location_type,
    5 AS data_source_key, -- MN GIS
    CAST(OGC_FID AS VARCHAR) AS orig_data_source_key,
    OGC_FID AS migration_primary_key,
    TO_JSON(STRUCT_PACK(
        gis_acres := GIS_ACRES,
        legislative_id := LEGISLATIV
    )) AS attributes
FROM mn_gis.dnr_management_units_prk;

-- View to transform State Forest Campgrounds
CREATE OR REPLACE VIEW mn_gis.v_forest_campgrounds AS
SELECT
    FACILITY_N AS name,
    ST_Y(ST_Centroid(geom)) AS latitude,
    ST_X(ST_Centroid(geom)) AS longitude,
    'MN' AS state_abbre,
    'Minnesota' AS state,
    'State Forest Campground' AS location_type,
    5 AS data_source_key, -- MN GIS
    GLOBALID AS orig_data_source_key,
    OGC_FID AS migration_primary_key,
    COUNTY AS county,
    TO_JSON(STRUCT_PACK(
        site_type := SITE_TYPE,
        state_forest := STATE_FORE
    )) AS attributes
FROM mn_gis.state_forest_campgrounds_and_day_use_areas;


-- Start Transaction

BEGIN TRANSACTION;

-- Migrate unique designations to the location types dimension table
INSERT INTO normalized.LocationTypes (location_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(location_type_key), 0) FROM normalized.LocationTypes) + row_number() OVER (),
    location_type,
    'MN GIS Designation'
FROM mn_gis.v_location_types
WHERE location_type NOT IN (SELECT name FROM normalized.LocationTypes);

-- Migrate Management Units
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, state_abbre, state, data_source_key, location_type_key, orig_data_source_key, migration_primary_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.state_abbre, v.state, v.data_source_key, lt.location_type_key, v.orig_data_source_key, v.migration_primary_key, v.attributes
FROM mn_gis.v_management_units v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

-- Migrate Forest Campgrounds
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, state_abbre, state, data_source_key, location_type_key, orig_data_source_key, migration_primary_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.state_abbre, v.state, v.data_source_key, lt.location_type_key, v.orig_data_source_key, v.migration_primary_key, v.attributes
FROM mn_gis.v_forest_campgrounds v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

-- Migrate Addresses for Forest Campgrounds
INSERT INTO normalized.Addresses (
    address_key, location_key, address_type_key, city, state_abbre, state, county, country_code
)
SELECT
    (SELECT COALESCE(MAX(address_key), 0) FROM normalized.Addresses) + row_number() OVER (),
    l.location_key,
    (SELECT address_type_key FROM normalized.AddressTypes WHERE name = 'Physical' LIMIT 1),
    v.county, -- Using county as city if city is unknown? The original script used s.COUNTY for city.
    'MN',
    'Minnesota',
    v.county,
    'US'
FROM mn_gis.v_forest_campgrounds v
JOIN normalized.Locations l ON v.orig_data_source_key = l.orig_data_source_key AND l.data_source_key = 5;

COMMIT;
