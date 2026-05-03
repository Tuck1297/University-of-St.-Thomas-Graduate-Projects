-- DuckDB SQL Queries - Merge MN DNR and GIS Data

-- This script merges all MN data sources (PDF, Units, and Boundaries) into a single unified record per location.
-- It identifies matching locations by name and consolidates attributes and IDs.

-- Ensure the spatial and json extensions are loaded
INSTALL json;
LOAD json;

BEGIN TRANSACTION;

-- 1. Ensure the combined data source exists
INSERT INTO normalized.DataSources (data_source_key, name, description)
SELECT 8, 'MN DNR and GIS Boundary Combined', 'Combined data from MN DNR PDF and MN GIS Boundary records'
WHERE NOT EXISTS (SELECT 1 FROM normalized.DataSources WHERE data_source_key = 8);

-- 2. Create a temporary table for cleaned names to simplify matching
CREATE OR REPLACE TABLE mn_temp_cleaned AS
SELECT
    location_key,
    data_source_key,
    name,
    attributes,
    orig_data_source_key,
    -- Cleaning name for matching: 
    -- 1. Remove ' SRA', ' State Park', ' State Forest', etc. suffixes
    -- 2. Replace curly apostrophes with straight ones
    -- 3. Convert to lowercase
    -- 4. Remove all non-alphanumeric characters
    regexp_replace(lower(replace(regexp_replace(name, ' (State Park|State Forest|State Recreation Area|SRA|State Wayside)$', ''), '’', '''')), '[^a-z0-9]', '', 'g') as clean_name
FROM normalized.Locations
WHERE data_source_key IN (2, 5, 7) AND active_flag = TRUE AND location_parent_key IS NULL;

-- 3. Identify the "Primary" record for each unique clean_name
-- Priority: 
-- 1. MN GIS Units (5) - usually has good coordinates and name
-- 2. MN GIS Boundaries (7) - fallback for coordinates
-- 3. MN DNR PDF (2) - fallback if no GIS data
CREATE OR REPLACE TABLE mn_primary_records AS
WITH ranked_records AS (
    SELECT
        location_key,
        clean_name,
        data_source_key,
        row_number() OVER (PARTITION BY clean_name ORDER BY 
            CASE data_source_key 
                WHEN 5 THEN 1 
                WHEN 7 THEN 2 
                WHEN 2 THEN 3 
                ELSE 4 
            END,
            location_key ASC
        ) as rank
    FROM mn_temp_cleaned
)
SELECT location_key, clean_name
FROM ranked_records
WHERE rank = 1;

-- 4. Consolidate attributes and IDs from all matching records into the Primary record
-- We use a loop-like approach by joining all related records
CREATE OR REPLACE TABLE mn_consolidated_data AS
SELECT
    p.location_key as primary_key,
    -- Aggregated attributes using json_merge_patch in a specific order
    -- Start with PDF (2) as base for rich attributes, then overlay Boundary (7), then Unit (5)
    -- This ensures Unit data (highest priority) wins on conflicts
    (
        SELECT json_merge_patch(
            json_merge_patch(
                COALESCE(MAX(CASE WHEN t.data_source_key = 2 THEN t.attributes END), '{}'::JSON),
                COALESCE(MAX(CASE WHEN t.data_source_key = 7 THEN t.attributes END), '{}'::JSON)
            ),
            COALESCE(MAX(CASE WHEN t.data_source_key = 5 THEN t.attributes END), '{}'::JSON)
        )
    ) as merged_attributes,
    -- Concatenate original keys for traceability
    group_concat(t.data_source_key || ':' || t.orig_data_source_key, '; ') as merged_orig_keys
FROM mn_primary_records p
JOIN mn_temp_cleaned t ON p.clean_name = t.clean_name
GROUP BY p.location_key;

-- 5. Update Primary records with consolidated data
UPDATE normalized.Locations
SET
    attributes = c.merged_attributes,
    data_source_key = 8,
    orig_data_source_key = c.merged_orig_keys,
    updated_dt = CURRENT_TIMESTAMP
FROM mn_consolidated_data c
WHERE location_key = c.primary_key;

-- 6. Deactivate all non-primary MN records
UPDATE normalized.Locations
SET
    active_flag = FALSE,
    updated_dt = CURRENT_TIMESTAMP
FROM mn_temp_cleaned t
LEFT JOIN mn_primary_records p ON t.location_key = p.location_key
WHERE p.location_key IS NULL
  AND Locations.location_key = t.location_key;

-- 7. Cleanup
DROP TABLE IF EXISTS mn_temp_cleaned;
DROP TABLE IF EXISTS mn_primary_records;
DROP TABLE IF EXISTS mn_consolidated_data;

COMMIT;

-- Verify results
SELECT 
    data_source_key,
    active_flag,
    count(*) as record_count 
FROM normalized.Locations 
WHERE data_source_key IN (2, 5, 7, 8)
GROUP BY data_source_key, active_flag
ORDER BY data_source_key, active_flag;
