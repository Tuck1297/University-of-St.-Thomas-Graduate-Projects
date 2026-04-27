-- DuckDB SQL Queries - Migrate NPS Data

-- Views Created for NPS Data Migration

-- View unique designations from the parks table to populate the location types dimension table during the migration
CREATE OR REPLACE VIEW nps.v_location_types AS
SELECT DISTINCT
    designation AS location_type
FROM nps.parks
WHERE designation IS NOT NULL AND designation != '';

-- View to denormalize the parks data by including activity and topic names as comma-separated lists
CREATE OR REPLACE VIEW nps.v_locations AS
SELECT
    id AS migration_primary_key,
    npsId AS orig_data_source_key,
    name,
    description,
    latitude,
    longitude,
    states,
    weatherInfo as weather_info,
    COALESCE(NULLIF(designation, ''), 'UNKNOWN') AS location_type, -- Data Source Key determined from here during the migration.
    1 AS data_source_key, -- NPS datasource
    TO_JSON(STRUCT_PACK(
        park_code := p.parkCode,
    	activities := (
	    	SELECT LIST(a.name ORDER BY ac.ord)
	        FROM UNNEST(p.activities) WITH ORDINALITY AS ac(id, ord)
	        LEFT JOIN nps.activities a ON a.id = ac.id
    	),
    	topics := (
    	    SELECT LIST(t.name ORDER BY top.ord)
	        FROM UNNEST(p.topics) WITH ORDINALITY AS top(id, ord)
	        LEFT JOIN nps.topics t ON t.id = top.id
    	)
    )) AS attributes
FROM nps.parks p;

-- View to denormalize the campgrounds data by including campground amenities and campground campsites into comma-separated lists
CREATE OR REPLACE VIEW nps.v_location_campgrounds AS
SELECT
    c.npsId AS orig_data_source_key,
    c.id AS migration_primary_key,
    c.description,
    c.latitude,
    c.longitude,
    c.name,
    c.weatherOverview AS weather_info,
    p.states AS states,
    1 AS data_source_key, -- NPS datasource
    6 AS location_type_key, -- Campground
    TO_JSON(STRUCT_PACK(
    	wheel_chair_access := c.wheelChairAccess,
    	internet_info := c.internetInfo,
    	rv_allowed := c.rvAllowed,
    	cell_phone_info := c.cellPhoneInfo,
    	fire_stove_policy := c.fireStovePolicy,
    	rv_max_length := c.rvMaxLength,
    	additional_info := c.additionalInfo,
    	trail_max_length := c.trailMaxLength,
    	ada_info := c.adaInfo,
    	rv_info := c.rvInfo,
    	trail_allowed := c.trailAllowed,
    	reservation_sites_first_come := c.reservationSitesFirstCome,
    	access_roads := c.accessRoads,
    	classifications := c.classifications,
    	campsites_info := STRUCT_PACK(
    		other := cc.other,
    		"group" := cc."group",
    		horse := cc.horse,
    		total_sites := cc.totalSites,
    		tent_only := cc.tentOnly,
    		rv_only := cc.rvOnly,
    		electrical_hookups := cc.electricalHookups,
    		walk_boat_to := cc.walkBoatTo
    	),
    	campground_amenities := STRUCT_PACK(
    		trash_recycling_collection := ca.trashRecyclingCollection,
    		toilets := ca.toilets,
    		internet_connectivity := ca.internetConnectivity,
    		showers := ca.showers,
    		cell_phone_reception := ca.cellPhoneReception,
    		laundry := ca.laundry,
    		amphitheater := ca.amphitheater,
    		dump_station := ca.dumpStation,
    		camp_store := ca.campStore,
    		staff_or_volunteer_host_onsite := ca.staffOrVolunteerHostOnsite,
    		potable_water := ca.potableWater,
    		ice_available_for_sale := ca.iceAvailableForSale,
    		firewood_for_sale := ca.firewoodForSale,
    		food_storage_lockers := ca.foodStorageLockers
    	),
    	park_code := c.parkCode -- Store for now, for later association and lookup to construct parent-child relationships between parks and campgrounds. We can remove this field later if we don't need it after the migration is complete.
    )) AS attributes
 FROM nps.campgrounds c
	JOIN nps.campground_amenities ca
		ON c.id = ca.campgroundId
	JOIN nps.campground_campsites cc
		ON c.id = cc.campgroundId
    JOIN nps.parks p
        ON c.parkCode = p.parkCode;

-- View to retrieve all media items for a given park/campground
CREATE OR REPLACE VIEW nps.v_media AS
SELECT * FROM (
SELECT
	id,
	parkId,
	campgroundId,
	credit,
	title,
	altText,
	caption,
	url,
	'' as subtitle,
	0 as height,
	0 as width,
	'photo' as media_type
FROM
nps.images

UNION ALL

SELECT
	id,
	parkId,
	campgroundId,
	'NPS' as credit,
	title,
	'' as altText,
	'' as caption,
	url,
	'' as subtitle,
	0 as height,
	0 as width,
	SPLIT_PART(type, '/', 2) AS media_type
FROM
nps.multimedia
WHERE media_type NOT LIKE '%galleries%'
)
ORDER BY media_type;

-- Views to retrieve all park and campground website urls for migration to the website entity
CREATE OR REPLACE VIEW nps.v_park_websites AS
SELECT
	id AS park_id,
    id AS migration_primary_key,
	directionsInfo AS directions_info,
	directionsUrl AS directions_url,
	url AS primary_url
FROM nps.parks;

CREATE OR REPLACE VIEW nps.v_campground_websites AS
SELECT
	c.id as campground_id,
    c.id AS migration_primary_key,
	c.regulationsUrl AS regulations_url,
    c.directionsUrl AS directions_url,
    c.reservationsUrl AS reservations_url,
    c.reservationsDescription AS reservations_description,
    p.url AS primary_url
FROM nps.campgrounds c
JOIN nps.parks p
	ON p.parkCode = c.parkCode;

-- View to retrieve the operating hours header data (names and periods)
CREATE OR REPLACE VIEW nps.v_operating_hours AS
SELECT
    id,
    parkId,
    campgroundId,
    name,
    description,
    'Regular' as type,
    NULL as startDate,
    NULL as endDate
FROM nps.operating_hours
UNION ALL
SELECT
    oh.id,
    oh.parkId,
    oh.campgroundId,
    e.name,
    oh.description,
    'Exception' as type,
    e.startDate,
    e.endDate
FROM nps.operating_hours_exceptions e
JOIN nps.operating_hours oh ON e.operatingHoursId = oh.id;

-- View to retrieve and transform the operating hours detail data (times per day)
CREATE OR REPLACE VIEW nps.v_operating_hours_times AS
WITH base_hours AS (
    SELECT
        id as operating_hours_id,
        parkId,
        campgroundId,
        'Regular' as type,
        NULL as startDate,
        NULL as endDate,
        monday, tuesday, wednesday, thursday, friday, saturday, sunday
    FROM nps.operating_hours
    UNION ALL
    SELECT
        oh.id as operating_hours_id,
        oh.parkId,
        oh.campgroundId,
        'Exception' as type,
        e.startDate,
        e.endDate,
        e.monday, e.tuesday, e.wednesday, e.thursday, e.friday, e.saturday, e.sunday
    FROM nps.operating_hours_exceptions e
    JOIN nps.operating_hours oh ON e.operatingHoursId = oh.id
),
unpivoted AS (
    SELECT
        operating_hours_id,
        parkId,
        campgroundId,
        type,
        startDate,
        endDate,
        day_name,
        hours_text,
        CASE
            WHEN day_name = 'monday' THEN 1
            WHEN day_name = 'tuesday' THEN 2
            WHEN day_name = 'wednesday' THEN 3
            WHEN day_name = 'thursday' THEN 4
            WHEN day_name = 'friday' THEN 5
            WHEN day_name = 'saturday' THEN 6
            WHEN day_name = 'sunday' THEN 7
        END AS day_of_week
    FROM base_hours
    UNPIVOT (hours_text FOR day_name IN (monday, tuesday, wednesday, thursday, friday, saturday, sunday))
),
parsed_times AS (
    SELECT
        *,
        regexp_extract(hours_text, '^(\d{1,2}):(\d{2})(AM|PM)', ['h', 'm', 'p']) as s,
        regexp_extract(hours_text, '(\d{1,2}):(\d{2})(AM|PM)$', ['h', 'm', 'p']) as e
    FROM unpivoted
)
SELECT
    operating_hours_id,
    parkId,
    campgroundId,
    type,
    startDate,
    endDate,
    day_name,
    hours_text,
    day_of_week,
    CASE
        WHEN hours_text = 'All Day' THEN 0
        WHEN s.h IS NOT NULL AND s.h != '' THEN
            TRY_CAST(s.h AS INT) + CASE WHEN s.p = 'PM' AND s.h != '12' THEN 12 WHEN s.p = 'AM' AND s.h = '12' THEN -12 ELSE 0 END
        ELSE NULL
    END AS day_start_hour,
    CASE
        WHEN hours_text = 'All Day' THEN 0
        WHEN s.m IS NOT NULL AND s.m != '' THEN TRY_CAST(s.m AS INT)
        ELSE NULL
    END AS day_start_minute,
    CASE
        WHEN hours_text = 'All Day' THEN 23
        WHEN e.h IS NOT NULL AND e.h != '' THEN
            TRY_CAST(e.h AS INT) + CASE WHEN e.p = 'PM' AND e.h != '12' THEN 12 WHEN e.p = 'AM' AND e.h = '12' THEN -12 ELSE 0 END
        ELSE NULL
    END AS day_end_hour,
    CASE
        WHEN hours_text = 'All Day' THEN 59
        WHEN e.m IS NOT NULL AND e.m != '' THEN TRY_CAST(e.m AS INT)
        ELSE NULL
    END AS day_end_minute
FROM parsed_times;

-- View to retrieve all unique NPS Address Types for migration to the AddressTypes dimension table
CREATE OR REPLACE VIEW nps.v_address_types AS
SELECT DISTINCT
    type AS address_type
FROM nps.addresses
WHERE type IS NOT NULL;

-- View to retrieve the location and campground addresses
CREATE OR REPLACE VIEW nps.v_addresses AS
SELECT
    id,
    parkId,
    campgroundId,
    postalCode  AS postal_code,
    city,
    stateCode AS state_abbre,
    countryCode AS country_code,
    line1 AS address_line_1,
    line2 AS address_line_2,
    line3 AS address_line_3,
    type
FROM nps.addresses;

-- View to retrieve the location and campground contacts
CREATE OR REPLACE VIEW nps.v_contact_emails AS
SELECT
    id,
    parkId,
    campgroundId,
    emailAddress as email_address,
    description,
    '' as type
FROM nps.contact_email_addresses;

CREATE OR REPLACE VIEW nps.v_contact_phones AS
SELECT
    id,
    parkId,
    campgroundId,
    phoneNumber as phone_number,
    extension,
    description,
    type
FROM nps.contact_phone_numbers;

-- Start Transaction

BEGIN TRANSACTION;

-- Migrate unique park designations to the location types dimension table
INSERT INTO normalized.LocationTypes (location_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(location_type_key), 0) FROM normalized.LocationTypes) + row_number() OVER (),
    location_type,
    'NPS Designation: ' || location_type
FROM nps.v_location_types
WHERE location_type NOT IN (SELECT name FROM normalized.LocationTypes);

-- Migrate the denormalized parks data to the locations table
INSERT INTO normalized.Locations (
    location_key,
    name,
    data_source_key,
    location_type_key,
    orig_data_source_key,
    migration_primary_key,
    latitude,
    longitude,
    description,
    weather_info,
    states,
    attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name,
    v.data_source_key,
    lt.location_type_key,
    v.orig_data_source_key,
    v.migration_primary_key,
    v.latitude,
    v.longitude,
    v.description,
    v.weather_info,
    v.states,
    v.attributes
FROM nps.v_locations v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

-- Migrate the denormalized campgrounds data to the locations table
INSERT INTO normalized.Locations (
    location_key,
    name,
    data_source_key,
    location_type_key,
    orig_data_source_key,
    migration_primary_key,
    latitude,
    longitude,
    description,
    weather_info,
    states,
    attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    name,
    data_source_key,
    location_type_key,
    orig_data_source_key,
    migration_primary_key,
    latitude,
    longitude,
    description,
    weather_info,
    states,
    attributes
FROM nps.v_location_campgrounds;

-- Update parent-child relationship for campgrounds (linking them to their respective parks)
UPDATE normalized.Locations
SET location_parent_key = p.location_key
FROM (
    SELECT location_key, json_extract_string(attributes, '$.park_code') as park_code
    FROM normalized.Locations
    WHERE data_source_key = 1 AND location_type_key != 6
) p
WHERE normalized.Locations.data_source_key = 1
  AND normalized.Locations.location_type_key = 6
  AND json_extract_string(normalized.Locations.attributes, '$.park_code') = p.park_code;

-- Migrate the media data to the media table
INSERT INTO normalized.Media (
    media_key,
    media_type_key,
    location_key,
    credit,
    title,
    altText,
    caption,
    url,
    subtitle,
    height,
    width
)
SELECT
    (SELECT COALESCE(MAX(media_key), 0) FROM normalized.Media) + row_number() OVER (),
    COALESCE(mt.media_type_key, 0),
    l.location_key,
    v.credit,
    v.title,
    v.altText,
    v.caption,
    v.url,
    v.subtitle,
    v.height,
    v.width
FROM nps.v_media v
JOIN normalized.Locations l ON
    (v.parkId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key != 6)
    OR
    (v.campgroundId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key = 6)
LEFT JOIN normalized.MediaTypes mt ON mt.name = CASE
    WHEN v.media_type IN ('photo', 'image', 'jpg', 'jpeg', 'png') THEN 'Photo'
    WHEN v.media_type IN ('video', 'mp4', 'mov') THEN 'Video'
    WHEN v.media_type IN ('audio', 'mp3', 'wav') THEN 'Audio'
    ELSE 'UNKNOWN'
END;

-- Migrate the park and campground website urls to the website entity
INSERT INTO normalized.Websites (website_key, website_type_key, location_key, url, description)
SELECT
    (SELECT COALESCE(MAX(website_key), 0) FROM normalized.Websites) + row_number() OVER (),
    v.website_type_key,
    l.location_key,
    v.url,
    v.description
FROM (
    -- Park Websites (location_type_key != 6)
    SELECT migration_primary_key, 1 as website_type_key, primary_url as url, 'Primary Website' as description, 'PARK' as source FROM nps.v_park_websites WHERE primary_url IS NOT NULL
    UNION ALL
    SELECT migration_primary_key, 2 as website_type_key, directions_url as url, 'Directions Website', 'PARK' as source FROM nps.v_park_websites WHERE directions_url IS NOT NULL
    UNION ALL
    -- Campground Websites (location_type_key = 6)
    SELECT migration_primary_key, 1 as website_type_key, primary_url as url, 'Primary Park Website' as description, 'CAMPGROUND' as source FROM nps.v_campground_websites WHERE primary_url IS NOT NULL
    UNION ALL
    SELECT migration_primary_key, 2 as website_type_key, directions_url as url, 'Directions Website', 'CAMPGROUND' as source FROM nps.v_campground_websites WHERE directions_url IS NOT NULL
    UNION ALL
    SELECT migration_primary_key, 3 as website_type_key, regulations_url as url, 'Regulations Website', 'CAMPGROUND' as source FROM nps.v_campground_websites WHERE regulations_url IS NOT NULL
    UNION ALL
    SELECT migration_primary_key, 4 as website_type_key, reservations_url as url, COALESCE(reservations_description, 'Reservations Website'), 'CAMPGROUND' as source FROM nps.v_campground_websites WHERE reservations_url IS NOT NULL
) v
JOIN normalized.Locations l ON v.migration_primary_key = l.migration_primary_key AND l.data_source_key = 1
AND (
    (v.source = 'PARK' AND l.location_type_key != 6)
    OR
    (v.source = 'CAMPGROUND' AND l.location_type_key = 6)
);

-- Migrate the location operating hours (Header)
INSERT INTO normalized.OperatingHours (
    operating_hours_key,
    location_key,
    name,
    description,
    type,
    start_time_period,
    end_time_period
)
SELECT
    (SELECT COALESCE(MAX(operating_hours_key), 0) FROM normalized.OperatingHours) + row_number() OVER (),
    l.location_key,
    v.name,
    v.description,
    v.type,
    TRY_CAST(v.startDate AS TIMESTAMP),
    TRY_CAST(v.endDate AS TIMESTAMP)
FROM nps.v_operating_hours v
JOIN normalized.Locations l ON
    (v.parkId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key != 6)
    OR
    (v.campgroundId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key = 6);

-- Migrate the location operating hours times (Detail)
INSERT INTO normalized.OperatingHoursTimes (
    operating_hours_time_key,
    operating_hours_key,
    day_of_week,
    day_start_hour,
    day_start_minute,
    day_end_hour,
    day_end_minute,
    descriptive_time
)
SELECT
    (SELECT COALESCE(MAX(operating_hours_time_key), 0) FROM normalized.OperatingHoursTimes) + row_number() OVER (),
    oh.operating_hours_key,
    v.day_of_week,
    v.day_start_hour,
    v.day_start_minute,
    v.day_end_hour,
    v.day_end_minute,
    v.hours_text
FROM nps.v_operating_hours_times v
JOIN normalized.Locations l ON
    (v.parkId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key != 6)
    OR
    (v.campgroundId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key = 6)
JOIN normalized.OperatingHours oh ON
    oh.location_key = l.location_key
    AND oh.name = (SELECT name FROM nps.v_operating_hours WHERE id = v.operating_hours_id AND type = v.type LIMIT 1) -- Match on name and type to get the correct header
    AND oh.type = v.type;

-- Migrate the location and campground addresses
-- First, ensure all NPS address types are in the AddressTypes table
INSERT INTO normalized.AddressTypes (address_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(address_type_key), 0) FROM normalized.AddressTypes) + row_number() OVER (),
    trim(type),
    'NPS Address Type'
FROM (SELECT DISTINCT type FROM nps.v_addresses)
WHERE trim(type) NOT IN (SELECT name FROM normalized.AddressTypes);

INSERT INTO normalized.Addresses (
    address_key,
    location_key,
    address_type_key,
    postal_code,
    city,
    state_abbre,
    address_line_1,
    address_line_2,
    address_line_3,
    country_code
)
SELECT
    (SELECT COALESCE(MAX(address_key), 0) FROM normalized.Addresses) + row_number() OVER (),
    l.location_key,
    addr_type.address_type_key,
    v.postal_code,
    v.city,
    v.state_abbre,
    v.address_line_1,
    v.address_line_2,
    v.address_line_3,
    v.country_code
FROM nps.v_addresses v
JOIN normalized.Locations l ON
    (v.parkId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key != 6)
    OR
    (v.campgroundId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key = 6)
JOIN normalized.AddressTypes addr_type ON trim(v.type) = trim(addr_type.name);

-- Migrate the location and campground contacts (Emails)
INSERT INTO normalized.ContactEmailAddresses (
    contact_email_address_key,
    location_key,
    email_address,
    description,
    type
)
SELECT
    (SELECT COALESCE(MAX(contact_email_address_key), 0) FROM normalized.ContactEmailAddresses) + row_number() OVER (),
    l.location_key,
    v.email_address,
    v.description,
    'Email'
FROM nps.v_contact_emails v
JOIN normalized.Locations l ON
    (v.parkId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key != 6)
    OR
    (v.campgroundId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key = 6);

-- Migrate the location and campground contacts (Phones)
INSERT INTO normalized.ContactPhoneNumbers (
    contact_phone_number_key,
    location_key,
    phone_number,
    description,
    extension,
    type
)
SELECT
    (SELECT COALESCE(MAX(contact_phone_number_key), 0) FROM normalized.ContactPhoneNumbers) + row_number() OVER (),
    l.location_key,
    v.phone_number,
    v.description,
    v.extension,
    v.type
FROM nps.v_contact_phones v
JOIN normalized.Locations l ON
    (v.parkId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key != 6)
    OR
    (v.campgroundId = l.migration_primary_key AND l.data_source_key = 1 AND l.location_type_key = 6);

COMMIT;