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
        winter_camping := trim("3") = '•',
        spring_camping := trim("4") = '•',
        summer_camping := trim("5") = '•',
        fall_camping := trim("6") = '•',
        site_drive_in := trim("7") = '•',
        site_accessible_drive_in := trim("8") = '•',
        max_rv_length := "9",
        rv_pull_through_sites := trim("10") = '•',
        "30_amp_hookup_sites" := trim("11") = '•',
        "50_amp_hookup_sites" := trim("12") = '•',
        horse_campsites := trim("13") = '•',
        backpack_sites := trim("14") = '•',
        group_campsites := trim("15") = '•',
        has_showers := trim("16") = '•',
        has_accessible_showers := trim("17") = '•',
        has_flush_toilets := trim("18") = '•',
        has_accessible_flush_toilets := trim("19") = '•',
        has_dump_station := trim("20") = '•',
        camper_cabins := trim("21") = '•',
        accessible_camper_cabins := trim("22") = '•',
        other_lodging := trim("23") = '•',
        hiking_trails := trim("24") = '•',
        paved_trails := trim("25") = '•',
        groomed_cross_country_ski_trails := trim("26") = '•',
        swimming_beach := trim("27") = '•',
        fishing_pier := trim("28") = '•',
        accessible_fishing_pier := trim("29") = '•',
        boat_rental := trim("30") = '•',
        showshoe_rental := trim("31") = '•',
        picnic_shelter := trim("32") = '•',
        accessible_picnic_shelter := trim("33") = '•',
        nature_programs := trim("34") = '•',
        accessible_track_chair := trim("35") = '•'
    )) AS attributes
FROM
	mn_dnr.extracted_table_0
WHERE
	"0" != ''
	AND "0" NOT LIKE '%NAME OF STATE PARK%';


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
