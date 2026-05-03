-- DuckDB SQL Queries - Migrate MN GIS Campsite Data

-- Load Spatial Extension
INSTALL spatial;
LOAD spatial;

-- Views Created for MN GIS Campsite Data Migration

-- View unique designations for campsites and campgrounds
CREATE OR REPLACE VIEW mn_gis_campsites.v_campsite_location_types AS
SELECT DISTINCT CAMPGROUND_TYPE_NAME AS location_type FROM mn_gis_campsites.struc_parks_and_trails_campsites WHERE CAMPGROUND_TYPE_NAME IS NOT NULL
UNION
SELECT DISTINCT CAMPING_UNIT_TYPE AS location_type FROM mn_gis_campsites.struc_parks_and_trails_campsites WHERE CAMPING_UNIT_TYPE IS NOT NULL;

-- View to transform Campground records (Parents)
-- We treat unique combinations of Campground Name and Management Unit as distinct locations
CREATE OR REPLACE VIEW mn_gis_campsites.v_campsite_campgrounds AS
SELECT DISTINCT
    CAMPGROUND_NAME AS name,
    'MN' AS state_abbre,
    'Minnesota' AS state,
    -- Get the first available coordinate from children to apply to the parent
    FIRST(ST_X(ST_Transform("Shape", 'EPSG:26915', 'EPSG:4326'))) AS latitude,
    FIRST(ST_Y(ST_Transform("Shape", 'EPSG:26915', 'EPSG:4326'))) AS longitude,
    COALESCE(CAMPGROUND_TYPE_NAME, 'Campground') AS location_type,
    CONTAINING_MGMT_UNIT_NAME AS parent_mgmt_unit,
    6 AS data_source_key, -- MN GIS Campgrounds
    'CAMPGROUND_' || lower(regexp_replace(CAMPGROUND_NAME, '[^a-z0-9]', '', 'g')) AS orig_data_source_key,
    TO_JSON(STRUCT_PACK(
        district := MAX(PAT_DISTRICT_NAME),
        region := MAX(DNR_REGION_NAME),
        county := MAX(COUNTY_NAME)
    )) AS attributes
FROM mn_gis_campsites.struc_parks_and_trails_campsites
WHERE CAMPGROUND_NAME IS NOT NULL
GROUP BY CAMPGROUND_NAME, CAMPGROUND_TYPE_NAME, CONTAINING_MGMT_UNIT_NAME;

-- View to transform individual Campsite records (Children)
CREATE OR REPLACE VIEW mn_gis_campsites.v_campsites AS
SELECT
    CAMPING_UNIT_NAME AS name,
    ST_X(ST_Transform("Shape", 'EPSG:26915', 'EPSG:4326')) AS latitude,
    ST_Y(ST_Transform("Shape", 'EPSG:26915', 'EPSG:4326')) AS longitude,
    'MN' AS state_abbre,
    'Minnesota' AS state,
    COALESCE(CAMPING_UNIT_TYPE, 'Campsite') AS location_type,
    CAMPGROUND_NAME AS parent_campground,
    6 AS data_source_key, -- MN GIS Campgrounds
    STABLE_PROD_GUID AS orig_data_source_key,
    TO_JSON(STRUCT_PACK(
        unit_type := CAMPING_UNIT_TYPE,
        description := CAMPING_UNIT_DESCRIP,
        loop := CAMPGROUND_LOOP_NAME,
        amps_30 := AMPS_30_FLAG = 'Y',
        amps_50 := AMPS_50_FLAG = 'Y',
        pad_length := SITE_PAD_LENGTH_FT,
        pad_width := SITE_PAD_WIDTH_FT,
        pad_type := SITE_PAD_TYPE_NAME,
        pull_through := PULL_THROUGH_BACK_IN_FLAG = 'P',
        tent_pad_dims := TENT_PAD_DIMENSIONS_FT_X_FT,
        picnic_table := PICNIC_TABLE_FLAG = 'Y',
        accessible := ADA_ACCESSIBLE_FLAG = 'Y',
        pets_allowed := PETS_ALLOWED_FLAG = 'Y',
        fire_ring := FIRE_RING_FLAG = 'Y',
        capacity_people := CAPACITY_PEOPLE,
        capacity_vehicles := CAPACITY_VEHICLES
    )) AS attributes
FROM mn_gis_campsites.struc_parks_and_trails_campsites;

-- Start Transaction

BEGIN TRANSACTION;

-- 1. Migrate unique designations to the location types dimension table
INSERT INTO normalized.LocationTypes (location_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(location_type_key), 0) FROM normalized.LocationTypes) + row_number() OVER (),
    location_type,
    'MN GIS Campsite/Campground'
FROM mn_gis_campsites.v_campsite_location_types
WHERE location_type NOT IN (SELECT name FROM normalized.LocationTypes);

-- 2. Migrate Campgrounds (as children of Management Units)
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, state_abbre, state, data_source_key, location_type_key, orig_data_source_key, location_parent_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.state_abbre, v.state, v.data_source_key, lt.location_type_key, v.orig_data_source_key, 
    p.location_key, -- Parent is the Management Unit (State Park/Forest)
    v.attributes
FROM mn_gis_campsites.v_campsite_campgrounds v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name
LEFT JOIN normalized.Locations p ON 
    lower(trim(regexp_replace(regexp_replace(p.name, ' (State Park|State Forest|State Recreation Area|SRA|State Wayside)$', ''), '’', ''''))) = 
    lower(trim(regexp_replace(regexp_replace(v.parent_mgmt_unit, ' (State Park|State Forest|State Recreation Area|SRA|State Wayside)$', ''), '’', '''')))
  AND p.data_source_key IN (5, 6, 7, 8, 9) AND p.active_flag = TRUE;

-- 3. Migrate individual Campsites (as children of Campgrounds)
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, state_abbre, state, data_source_key, location_type_key, orig_data_source_key, location_parent_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.state_abbre, v.state, v.data_source_key, lt.location_type_key, v.orig_data_source_key,
    p.location_key, -- Parent is the Campground
    v.attributes
FROM mn_gis_campsites.v_campsites v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name
LEFT JOIN normalized.Locations p ON p.name = v.parent_campground
  AND p.data_source_key = 6 AND p.location_type_key IN (SELECT location_type_key FROM normalized.LocationTypes WHERE name IN ('Campground', 'Horse Campground', 'State Forest Campground and Day Use Area'));

COMMIT;
