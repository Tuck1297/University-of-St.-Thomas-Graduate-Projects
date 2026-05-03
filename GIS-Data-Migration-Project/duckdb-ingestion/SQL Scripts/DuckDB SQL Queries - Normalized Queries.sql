INSTALL spatial;
LOAD spatial;

-- Sanity check normalized data
SELECT COUNT(*), 'locations' AS table_name FROM normalized.locations
UNION ALL
SELECT COUNT(*) AS total_location_types, 'LocationTypes' AS table_name FROM normalized.LocationTypes
UNION ALL
SELECT COUNT(*) AS total_data_sources, 'DataSources' AS table_name FROM normalized.DataSources
UNION ALL
SELECT COUNT(*) AS total_addresses, 'Addresses' AS table_name FROM normalized.Addresses
UNION ALL
SELECT COUNT(*) AS total_address_types, 'AddressTypes' AS table_name FROM normalized.AddressTypes
UNION ALL
SELECT COUNT(*) AS total_contact_emails, 'ContactEmailAddresses' AS table_name FROM normalized.ContactEmailAddresses
UNION ALL
SELECT COUNT(*) AS total_contact_phones, 'ContactPhoneNumbers' AS table_name FROM normalized.ContactPhoneNumbers
UNION ALL
SELECT COUNT(*) AS total_media, 'Media' AS table_name FROM normalized.Media
UNION ALL
SELECT COUNT(*) AS total_media_types, 'MediaTypes' AS table_name FROM normalized.MediaTypes
UNION ALL
SELECT COUNT(*) AS total_operating_hours, 'OperatingHours' AS table_name FROM normalized.OperatingHours
UNION ALL
SELECT COUNT(*) AS total_operating_hours_times, 'OperatingHoursTimes' AS table_name FROM normalized.OperatingHoursTimes
UNION ALL
SELECT COUNT(*) AS total_websites, 'Websites' AS table_name FROM normalized.Websites
UNION ALL
SELECT COUNT(*) AS total_website_types, 'WebsiteTypes' AS table_name FROM normalized.WebsiteTypes;

-- Hierarchical Query: Get the full path for each location
-- This query uses a Recursive CTE to show the hierarchy of locations (e.g., Park -> Campground -> Site)
WITH RECURSIVE LocationHierarchy AS (
	SELECT
		location_key,
		name,
		location_parent_key,
		location_type_key,
		name AS hierarchy_path,
		1 AS Level
	FROM normalized.locations
	WHERE location_parent_key IS NULL OR location_key = 0
	UNION ALL
	SELECT
		NL.location_key,
		NL.name,
		NL.location_parent_key,
		NL.location_type_key,
		LH.hierarchy_path || ' > ' || LH.name,
		LH.level + 1
	FROM normalized.locations NL
	JOIN LocationHierarchy LH ON NL.location_parent_key = LH.location_key
	WHERE NL.location_key <> 0
)
SELECT
    LH.*,
    LT.name as location_type_name
FROM LocationHierarchy LH
JOIN normalized.LocationTypes LT ON LH.location_type_key = LT.location_type_key
ORDER BY hierarchy_path;

-- View Created from the above query to easily access the location hierarchy
CREATE OR REPLACE VIEW normalized.v_location_hierarchy AS
WITH RECURSIVE LocationHierarchy AS (
	SELECT
		location_key,
		name,
		location_parent_key,
		location_type_key,
		name AS hierarchy_path,
		1 AS Level
	FROM normalized.locations
	WHERE location_parent_key IS NULL OR location_key = 0
	UNION ALL
	SELECT
		NL.location_key,
		NL.name,
		NL.location_parent_key,
		NL.location_type_key,
		LH.hierarchy_path || ' > ' || LH.name,
		LH.level + 1
	FROM normalized.locations NL
	JOIN LocationHierarchy LH ON NL.location_parent_key = LH.location_key
	WHERE NL.location_key <> 0
)
SELECT
    LH.*,
    LT.name as location_type_name
FROM LocationHierarchy LH
JOIN normalized.LocationTypes LT ON LH.location_type_key = LT.location_type_key
ORDER BY hierarchy_path;

-- Spatial Query: Find locations within a certain geographic boundary
-- NOTE: This query below was AI Generated and is only a starting point estimate that uses the Haversine formula for distance calculation.
--       The Haversine formula is not the most accurate method for distance calculations on a sphere, but it is commonly used for its simplicity. For more accurate results.
-- NOTE: After some research I have determined that DuckDB does not support spatial functions as much as PostGIS when it comes to
--       latitude and longitude values. Therefore, I'm putting off geospatial queries in DuckDB until I can migrate the data to PostGIS and PostgresSQL.

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

-- Unified Join Query: Grouping all data
-- The query below retrieves all relevant data from all tables in this normalized schema. Each core type's data is represented
-- as a JSON array of objects where applicable.

CREATE OR REPLACE VIEW normalized.v_location_details AS
SELECT
    l.name,
    l.location_key,
    l.location_parent_key,
    l.latitude,
    l.longitude,
    l.weather_info,
    l.directions_info,
    l.ai_generative_description,
    l.description,
    l.states,
    l.attributes,

    lt.name AS location_type,
    ds.name AS data_source_name,

    -- Addresses
    TO_JSON(
        LIST(DISTINCT STRUCT_PACK(
            postal_code := a.postal_code,
            city := a.city,
            state_abbre := a.state_abbre,
            address_line_1 := a.address_line_1,
            address_line_2 := a.address_line_2,
            address_line_3 := a.address_line_3,
            address_type := aty.name
        )) FILTER (WHERE a.location_key IS NOT NULL)
    ) AS addresses,

    -- Emails
    TO_JSON(
        LIST(DISTINCT STRUCT_PACK(
            email_address := cea.email_address,
            description := cea.description,
            email_type := cea.type
        )) FILTER (WHERE cea.location_key IS NOT NULL)
    ) AS email_addresses,

    -- Phones
    TO_JSON(
        LIST(DISTINCT STRUCT_PACK(
            phone_number := cpn.phone_number,
            description := cpn.description,
            extension := cpn.extension,
            type := cpn.type
        )) FILTER (WHERE cpn.location_key IS NOT NULL)
    ) AS phone_numbers,

    -- Media
    TO_JSON(
        LIST(DISTINCT STRUCT_PACK(
            credit := m.credit,
            title := m.title,
            altText := m.altText,
            caption := m.caption,
            url := m.url,
            subtitle := m.subtitle,
            height := m.height,
            width := m.width,
            media_type := mt.name
        )) FILTER (WHERE m.location_key IS NOT NULL)
    ) AS media,

    -- Websites
    TO_JSON(
        LIST(DISTINCT STRUCT_PACK(
            url := w.url,
            description := w.description,
            website_type := wt.name
        )) FILTER (WHERE w.location_key IS NOT NULL)
    ) AS websites,

    -- Operating Hours
    TO_JSON(
        LIST(DISTINCT STRUCT_PACK(
            name := oh.name,
            description := oh.description,
            type := oh.type,
            hours := (
                SELECT LIST(STRUCT_PACK(
                    week_day := oht.day_of_week,
                    start_hour := oht.day_start_hour,
                    start_minute := oht.day_start_minute,
                    end_hour := oht.day_end_hour,
                    end_minute := oht.day_end_minute,
                    description := oht.descriptive_time,
                    specific_day := oht.specific_day
                ))
                FROM normalized.OperatingHoursTimes oht
                WHERE oht.operating_hours_key = oh.operating_hours_key
            )
        )) FILTER (WHERE oh.location_key IS NOT NULL)
    ) AS operating_hours,

    -- Children Locations (e.g., Campgrounds within a Park)
    TO_JSON(
        (
            SELECT LIST(
                STRUCT_PACK(
                    location_key := c.location_key,
                    name := c.name,
                    location_type := lt2.name,
                    latitude := c.latitude,
                    longitude := c.longitude
                )
            )
            FROM normalized.Locations c
            JOIN normalized.LocationTypes lt2
                ON c.location_type_key = lt2.location_type_key
            WHERE c.location_parent_key = l.location_key
        )
    ) AS children

FROM normalized.Locations l

JOIN normalized.DataSources ds
    ON l.data_source_key = ds.data_source_key

JOIN normalized.LocationTypes lt
    ON l.location_type_key = lt.location_type_key

LEFT JOIN normalized.Addresses a
    ON a.location_key = l.location_key
LEFT JOIN normalized.AddressTypes aty
    ON a.address_type_key = aty.address_type_key

LEFT JOIN normalized.ContactEmailAddresses cea
    ON cea.location_key = l.location_key

LEFT JOIN normalized.ContactPhoneNumbers cpn
    ON cpn.location_key = l.location_key

LEFT JOIN normalized.Media m
    ON m.location_key = l.location_key
LEFT JOIN normalized.MediaTypes mt
    ON m.media_type_key = mt.media_type_key

LEFT JOIN normalized.OperatingHours oh
    ON oh.location_key = l.location_key

LEFT JOIN normalized.Websites w
    ON w.location_key = l.location_key
LEFT JOIN normalized.WebsiteTypes wt
    ON w.website_type_key = wt.website_type_key

GROUP BY
    l.name,
    l.location_key,
    l.location_parent_key,
    l.latitude,
    l.longitude,
    l.weather_info,
    l.directions_info,
    l.ai_generative_description,
    l.description,
    l.states,
    l.attributes,
    lt.name,
    ds.name;

SELECT * FROM normalized.v_location_details
WHERE location_key = [SOME_LOCATION_KEY_HERE];