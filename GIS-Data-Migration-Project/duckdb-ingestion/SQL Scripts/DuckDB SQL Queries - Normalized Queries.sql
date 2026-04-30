INSTALL spatial;
LOAD spatial;

-- Hierarchical Query: Get the full path for each location
-- This query uses a Recursive CTE to show the hierarchy of locations (e.g., Park -> Campground -> Site)
WITH RECURSIVE LocationHierarchy AS (
    -- Anchor member: Root locations (those with no parent or self-parenting unknown)
    SELECT 
        location_key,
        name,
        location_parent_key,
        location_type_key,
        name AS hierarchy_path,
        1 AS level
    FROM normalized.Locations
    WHERE location_parent_key IS NULL OR location_key = 0

    UNION ALL

    -- Recursive member: Join children to their parents
    SELECT 
        l.location_key,
        l.name,
        l.location_parent_key,
        l.location_type_key,
        lh.hierarchy_path || ' > ' || l.name,
        lh.level + 1
    FROM normalized.Locations l
    JOIN LocationHierarchy lh ON l.location_parent_key = lh.location_key
    WHERE l.location_key <> 0 -- Avoid infinite loop with the 'UNKNOWN' placeholder if it references itself
)
SELECT 
    lh.*,
    lt.name as location_type_name
FROM LocationHierarchy lh
JOIN normalized.LocationTypes lt ON lh.location_type_key = lt.location_type_key
ORDER BY hierarchy_path;

-- Spatial Query: Find locations within a certain geographic boundary
-- NOTE: This one was AI Generated and is only a starting point estimate that uses the Haversine formula for distance calculation.
-- TODO: Still need to try to utilize the DuckDB Spatial Extension functions to see if I can get better performance and more accurate results.

WITH params AS (
    SELECT 
        [LAT_HERE]  AS user_lat,
        [LON_HERE] AS user_lon,
        210.0     AS radius_miles
),
locations_with_distance AS (
    SELECT 
        l.*,
        lt.name AS location_type,
        ds.name AS data_source,
        -- Haversine formula: calculates distance in miles between two lat/lon points
        3958.8 * 2 * ASIN(SQRT(
            POWER(SIN(RADIANS(l.latitude  - p.user_lat) / 2), 2) +
            COS(RADIANS(p.user_lat)) * COS(RADIANS(l.latitude)) *
            POWER(SIN(RADIANS(l.longitude - p.user_lon) / 2), 2)
        )) AS distance_miles
    FROM normalized.Locations l
    JOIN normalized.LocationTypes lt ON l.location_type_key = lt.location_type_key
    JOIN normalized.DataSources ds ON l.data_source_key = ds.data_source_key
    CROSS JOIN params p
    WHERE l.location_key <> 0
      AND l.latitude IS NOT NULL
      AND l.longitude IS NOT NULL
)
SELECT *
FROM locations_with_distance
WHERE distance_miles <= (SELECT radius_miles FROM params)
ORDER BY distance_miles ASC;

-- Unified Join Query: Grouping Media, Websites, and Addresses into Arrays
-- This query aggregates related entities into JSON arrays for easy consumption by visualization tools.
SELECT 
    l.location_key,
    l.name,
    l.latitude,
    l.longitude,
    lt.name AS location_type,
    ds.name AS data_source,
    l.state,
    l.description,
    
    -- Aggregate Addresses into a JSON array of objects
    (SELECT to_json(LIST(struct_pack(
        type := adt.name,
        line_1 := a.address_line_1,
        line_2 := a.address_line_2,
        city := a.city,
        state := a.state_abbre,
        postal_code := a.postal_code,
        county := a.county,
        country := a.country_code
    ))) 
     FROM normalized.Addresses a 
     LEFT JOIN normalized.AddressTypes adt ON a.address_type_key = adt.address_type_key
     WHERE a.location_key = l.location_key) AS addresses_json,

    -- Aggregate Websites into a JSON array of objects
    (SELECT to_json(LIST(struct_pack(
        type := wbt.name,
        url := w.url,
        description := w.description
    )))
     FROM normalized.Websites w
     LEFT JOIN normalized.WebsiteTypes wbt ON w.website_type_key = wbt.website_type_key
     WHERE w.location_key = l.location_key) AS websites_json,

    -- Aggregate Media into a JSON array of objects
    (SELECT to_json(LIST(struct_pack(
        type := mdt.name,
        url := m.url,
        title := m.title,
        caption := m.caption,
        credit := m.credit,
        width := m.width,
        height := m.height
    )))
     FROM normalized.Media m
     LEFT JOIN normalized.MediaTypes mdt ON m.media_type_key = mdt.media_type_key
     WHERE m.location_key = l.location_key) AS media_json,

    -- Aggregate Contact Info
    (SELECT to_json(LIST(struct_pack(type := type, phone := phone_number))) FROM normalized.ContactPhoneNumbers WHERE location_key = l.location_key) AS phones_json,
    (SELECT to_json(LIST(struct_pack(type := type, email := email_address))) FROM normalized.ContactEmailAddresses WHERE location_key = l.location_key) AS emails_json

FROM normalized.Locations l
JOIN normalized.LocationTypes lt ON l.location_type_key = lt.location_type_key
JOIN normalized.DataSources ds ON l.data_source_key = ds.data_source_key
WHERE l.location_key <> 0 -- Exclude UNKNOWN placeholder
LIMIT 100;
