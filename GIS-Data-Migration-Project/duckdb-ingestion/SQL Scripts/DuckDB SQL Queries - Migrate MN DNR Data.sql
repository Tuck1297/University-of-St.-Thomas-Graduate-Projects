-- DuckDB SQL Queries - Migrate MN DNR Data

-- Views Created for MN DNR Data Migration

-- View unique designations from the extracted PDF data to populate the location types dimension table
CREATE OR REPLACE VIEW mn_dnr.v_location_types AS
SELECT DISTINCT
    'State Park' AS location_type
FROM mn_dnr.extracted_table_0
WHERE "0" IS NOT NULL AND length("0") > 3 AND "0" NOT LIKE 'Page %';

-- View to transform the extracted PDF data for migration to the locations table
CREATE OR REPLACE VIEW mn_dnr.v_locations AS
SELECT
    "0" AS name,
    'MN' AS state_abbre,
    'Minnesota' AS state,
    'State Park' AS location_type,
    2 AS data_source_key, -- MN DNR
    'DNR_PDF_' || "0" AS orig_data_source_key,
    NULL AS migration_primary_key,
    0.0 AS latitude,
    0.0 AS longitude,
    '' AS description,
    TO_JSON(STRUCT_PACK(
        total_acres := "1",
        overnight_visits := "2",
        day_visits := "3",
        total_visits := "4"
    )) AS attributes
FROM mn_dnr.extracted_table_0
WHERE "0" IS NOT NULL AND length("0") > 3 AND "0" NOT LIKE 'Page %';


-- Start Transaction

BEGIN TRANSACTION;

-- Migrate unique designations to the location types dimension table
INSERT INTO normalized.LocationTypes (location_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(location_type_key), 0) FROM normalized.LocationTypes) + row_number() OVER (),
    location_type,
    'MN DNR State Park'
FROM mn_dnr.v_location_types
WHERE location_type NOT IN (SELECT name FROM normalized.LocationTypes);

-- Migrate the locations data
INSERT INTO normalized.Locations (
    location_key,
    name,
    data_source_key,
    location_type_key,
    orig_data_source_key,
    latitude,
    longitude,
    state_abbre,
    state,
    attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name,
    v.data_source_key,
    lt.location_type_key,
    v.orig_data_source_key,
    v.latitude,
    v.longitude,
    v.state_abbre,
    v.state,
    v.attributes
FROM mn_dnr.v_locations v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

COMMIT;
