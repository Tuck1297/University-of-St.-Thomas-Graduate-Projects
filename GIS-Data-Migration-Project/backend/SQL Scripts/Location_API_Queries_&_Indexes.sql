-- PostgreSQL SQL normalized SCHEMA namespace Queries

-- Configure Extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- For geospatial queries
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- For similarity search


-- Database space used for the entire database connected to

SELECT
	pg_database.datname AS database_name,
	pg_database_size(current_database()) / 1024.0 / 1024.0 AS size_mb
FROM pg_database
WHERE datname = current_database();

-- Space taken by each table
SELECT
    relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS human_readable_size,
    pg_total_relation_size(relid) / 1024.0 / 1024.0 AS size_mb
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

-- Geospatial View based on Map bounds that will return either top level locations with each of their children location keys

SELECT
    location_key,
    name,
    description,
    latitude,
    longitude,
    ATTRIBUTES
FROM normalized."Locations"
WHERE active_flag = TRUE
AND ST_Within(
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),
    ST_MakeEnvelope(-93.68649898905576, 44.988737408669515, -93.42591613314968, 45.07522451764426, 4326) -- min_lon, min_lat, max_lon, max_lat
);

-- Search Endpoint based on Location name and/or description that API can use

-- Trigram indexes for the fuzzy search
CREATE INDEX idx_locations_name_trgm ON normalized."Locations" USING GIN (name gin_trgm_ops);
CREATE INDEX idx_locations_desc_trgm ON normalized."Locations" USING GIN (description gin_trgm_ops);

-- B-tree index for the active_flag filter
CREATE INDEX idx_locations_active_flag ON normalized."Locations" (active_flag);


SELECT
    location_key,
    name,
    description,
    latitude,
    longitude,
    ATTRIBUTES,
    relevance_score
FROM (
    SELECT
        location_key,
        name,
        description,
        latitude,
        longitude,
        ATTRIBUTES,
        GREATEST(
            word_similarity('gooseberry falls'::text, name),
            word_similarity('gooseberry falls'::text, description)
        ) AS relevance_score
    FROM normalized."Locations"
    WHERE active_flag = TRUE
    AND (
        'gooseberry falls'::text <% name
        OR 'gooseberry falls'::text <% description
    )
) scored
WHERE relevance_score > 0.3
ORDER BY relevance_score DESC
LIMIT 30;



SELECT
    location_key,
    name,
    description,
    latitude,
    longitude,
    ATTRIBUTES,
    relevance_score
FROM (
    SELECT
        location_key,
        name,
        description,
        latitude,
        longitude,
        ATTRIBUTES,
        GREATEST(
            word_similarity('gooseberry falls'::text, name),
            word_similarity('gooseberry falls'::text, description)
        ) AS relevance_score
    FROM normalized."Locations"
    WHERE active_flag = TRUE
    AND (
        name ILIKE '%gooseberry falls%'
        OR description ILIKE '%gooseberry falls%'
    )
) scored
ORDER BY relevance_score DESC
LIMIT 30;

-- Location Details view that API can use

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
    JSON_AGG(DISTINCT JSONB_BUILD_OBJECT(
        'postal_code', a.postal_code,
        'city', a.city,
        'state_abbre', a.state_abbre,
        'address_line_1', a.address_line_1,
        'address_line_2', a.address_line_2,
        'address_line_3', a.address_line_3,
        'address_type', aty.name
    )) FILTER (WHERE a.location_key IS NOT NULL) AS addresses,

    -- Emails
    JSON_AGG(DISTINCT JSONB_BUILD_OBJECT(
        'email_address', cea.email_address,
        'description', cea.description,
        'email_type', cea.type
    )) FILTER (WHERE cea.location_key IS NOT NULL) AS email_addresses,

    -- Phones
    JSON_AGG(DISTINCT JSONB_BUILD_OBJECT(
        'phone_number', cpn.phone_number,
        'description', cpn.description,
        'extension', cpn.extension,
        'type', cpn.type
    )) FILTER (WHERE cpn.location_key IS NOT NULL) AS phone_numbers,

    -- Media
    JSON_AGG(DISTINCT JSONB_BUILD_OBJECT(
        'credit', m.credit,
        'title', m.title,
        'altText', m."altText",
        'caption', m.caption,
        'url', m.url,
        'subtitle', m.subtitle,
        'height', m.height,
        'width', m.width,
        'media_type', mt.name
    )) FILTER (WHERE m.location_key IS NOT NULL) AS media,

    -- Websites
    JSON_AGG(DISTINCT JSONB_BUILD_OBJECT(
        'url', w.url,
        'description', w.description,
        'website_type', wt.name
    )) FILTER (WHERE w.location_key IS NOT NULL) AS websites,

    -- Operating Hours
    JSON_AGG(DISTINCT JSONB_BUILD_OBJECT(
        'name', oh.name,
        'description', oh.description,
        'type', oh.type
    )) FILTER (WHERE oh.location_key IS NOT NULL) AS operating_hours,

    -- Children Locations
    (
        SELECT JSON_AGG(JSONB_BUILD_OBJECT(
            'location_key', c.location_key,
            'name', c.name,
            'location_type', lt2.name,
            'latitude', c.latitude,
            'longitude', c.longitude
        ))
        FROM normalized."Locations" c
        JOIN normalized."LocationTypes" lt2
            ON c.location_type_key = lt2.location_type_key
        WHERE c.location_parent_key = l.location_key
    ) AS children

FROM normalized."Locations" l

JOIN normalized."DataSources" ds
    ON l.data_source_key = ds.data_source_key

JOIN normalized."LocationTypes" lt
    ON l.location_type_key = lt.location_type_key

LEFT JOIN normalized."Addresses" a
    ON a.location_key = l.location_key
LEFT JOIN normalized."AddressTypes" aty
    ON a.address_type_key = aty.address_type_key

LEFT JOIN normalized."ContactEmailAddresses" cea
    ON cea.location_key = l.location_key

LEFT JOIN normalized."ContactPhoneNumbers" cpn
    ON cpn.location_key = l.location_key

LEFT JOIN normalized."Media" m
    ON m.location_key = l.location_key
LEFT JOIN normalized."MediaTypes" mt
    ON m.media_type_key = mt.media_type_key

LEFT JOIN normalized."OperatingHours" oh
    ON oh.location_key = l.location_key

LEFT JOIN normalized."Websites" w
    ON w.location_key = l.location_key
LEFT JOIN normalized."WebsiteTypes" wt
    ON w.website_type_key = wt.website_type_key

WHERE l.location_key = 30

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

