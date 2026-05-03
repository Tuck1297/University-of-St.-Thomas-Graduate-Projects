-- DuckDB SQL Queries - Associate MN Campgrounds to Merged Locations

-- This script elevates Merged Master records (ID 8) to ID 9 if they are destined to have campgrounds.
-- This MUST be run BEFORE 'Migrate MN GIS Campsite Data.sql' to avoid DuckDB foreign key update limitations.

INSTALL json;
LOAD json;
INSTALL spatial;
LOAD spatial;

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS mn_gis_campsites;

-- View to transform Campground records (Parents) - needed for identification
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

BEGIN TRANSACTION;

-- 1. Ensure the master combined data source exists
INSERT INTO normalized.DataSources (data_source_key, name, description)
SELECT 9, 'MN Combined Master Record', 'Fully consolidated data from MN DNR PDF, MN GIS Boundary, and MN GIS Campground records'
WHERE NOT EXISTS (SELECT 1 FROM normalized.DataSources WHERE data_source_key = 9);

-- 2. Identify which Master records (ID 8) will have campgrounds based on raw data
-- and elevate them to ID 9 BEFORE they become parents.
WITH campgrounds_to_be AS (
    SELECT DISTINCT parent_mgmt_unit
    FROM mn_gis_campsites.v_campsite_campgrounds
),
master_to_elevate AS (
    SELECT 
        m.location_key
    FROM normalized.Locations m
    JOIN campgrounds_to_be c ON 
        regexp_replace(lower(replace(regexp_replace(m.name, ' (State Park|State Forest|State Recreation Area|SRA|State Wayside)$', ''), '’', '''')), '[^a-z0-9]', '', 'g') = 
        regexp_replace(lower(replace(regexp_replace(c.parent_mgmt_unit, ' (State Park|State Forest|State Recreation Area|SRA|State Wayside)$', ''), '’', '''')), '[^a-z0-9]', '', 'g')
    WHERE m.data_source_key = 8 AND m.active_flag = TRUE
)
UPDATE normalized.Locations
SET 
    data_source_key = 9,
    updated_dt = CURRENT_TIMESTAMP
WHERE location_key IN (SELECT location_key FROM master_to_elevate);

COMMIT;

-- Verify results
SELECT 
    data_source_key,
    count(*) as record_count
FROM normalized.Locations
WHERE data_source_key IN (8, 9)
GROUP BY 1;
