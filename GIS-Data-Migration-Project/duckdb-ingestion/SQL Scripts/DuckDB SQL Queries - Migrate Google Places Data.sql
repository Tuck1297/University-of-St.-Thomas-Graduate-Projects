-- DuckDB SQL Queries - Migrate Google Places Data

-- Views Created for Google Places Data Migration

-- View unique designations from the google places data to populate the location types dimension table
CREATE OR REPLACE VIEW google.v_location_types AS
SELECT DISTINCT primaryType AS location_type FROM google.places WHERE primaryType IS NOT NULL AND primaryType != '';

-- View to transform Google Places
CREATE OR REPLACE VIEW google.v_places AS
SELECT
    primaryDisplayName AS name,
    4 AS data_source_key, -- Google Places API
    googlePlaceId AS orig_data_source_key,
    id AS migration_primary_key,
    latitude,
    longitude,
    geminiGenerativeSummary AS ai_generative_description,
    editorialSummary AS description,
    administrativeArea AS state_abbre,
    state,
    primaryType AS location_type,
    TO_JSON(STRUCT_PACK(
        google_name := googleName,
        business_status := businessStatus,
        good_for_children := goodForChildren,
        allow_dogs := allowDogs,
        accept_credit_cards := acceptCreditCards,
        accept_debit_cards := acceptDebitCards,
        accept_cash_only := acceptCashOnly,
        free_parking_lot := freeParkingLot,
        paid_parking_lot := paidParkingLot,
        free_street_parking := freeStreetParking,
        wheelchair_accessible_parking := wheelchairAccessibleParking,
        wheelchair_accessible_seating := wheelchairAccessibleSeating,
        search_api_type :=
        	CASE
        		WHEN statePark = TRUE THEN 'State Park'
        		WHEN stateForest = TRUE THEN 'State Forest'
        		WHEN stateWayside = TRUE THEN 'State Wayside'
        		WHEN nationalForest = TRUE THEN 'National Forest'
        		WHEN countyPark = TRUE THEN 'County Park'
        		WHEN naturalWildlifeArea = TRUE THEN 'Natural Wildlife Area'
        		WHEN cityPark = TRUE THEN 'City Park'
        		ELSE 'UNKNOWN'
        	END
    )) AS attributes,
    nationalPhoneNumber AS phone,
    websiteUri AS website_url
FROM google.places;

-- View to transform Photos
CREATE OR REPLACE VIEW google.v_media AS
SELECT
	id,
    googlePlaceId,
    authorDisplayName AS credit,
    SPLIT_PART(googlePhotoName, '/', 4) AS title,
    CONCAT('https://places.googleapis.com/v1/', googlePhotoName, '/media') AS url,
    widthPx AS width,
    heightPx AS height,
    'Photo' AS media_type
FROM google.photos;

-- View to transform Google Places Addresses
CREATE OR REPLACE VIEW google.v_addresses AS
SELECT
    id,
    googlePlaceId,
    COALESCE(
        NULLIF(postalCode, ''),
        REGEXP_EXTRACT(formattedAddress, '([0-9]{5}(-[0-9]{4})?)', 1),
        ''
    ) AS postal_code,
    COALESCE(locality, '') AS city,
    COALESCE(administrativeArea, '') AS state_abbre,
    COALESCE(state, '') AS state,
    CASE
    	WHEN trim(COALESCE(regionCode, '')) = '' THEN '###'
    	ELSE regionCode
    END AS country_code,
    'Physical' AS type,
     -- Address Line 1
    COALESCE(
        CASE
            WHEN COALESCE(ARRAY_LENGTH(addressLines), 0) = 0
                THEN shortFormattedAddress
            ELSE addressLines[1]
        END,
        ''
    ) AS address_line_1,
    -- Address Line 2
    COALESCE(
        CASE
            WHEN ARRAY_LENGTH(addressLines) >= 2
                THEN addressLines[2]
            ELSE NULL
        END,
        ''
    ) AS address_line_2,
    -- Address Line 3
    COALESCE(
        CASE
            WHEN ARRAY_LENGTH(addressLines) >= 3
                THEN addressLines[3]
            ELSE NULL
        END,
        ''
    ) AS address_line_3
FROM google.places;

-- View to transform Google Places Contact Phones
CREATE OR REPLACE VIEW google.v_contact_phones AS
SELECT
	id,
    googlePlaceId,
    nationalPhoneNumber AS phone_number,
    'Primary Google Listed Number' AS type
FROM google.places
WHERE nationalPhoneNumber IS NOT NULL
AND nationalPhoneNumber!= '';

-- View to transform Google Places Websites
CREATE OR REPLACE VIEW google.v_websites AS
SELECT
	id,
	googlePlaceId,
	websiteUri AS website_url,
	'Primary Google URL' AS description,
    'Primary' as type
FROM google.places
WHERE websiteUri IS NOT NULL AND websiteUri != ''
UNION ALL
SELECT
	id,
	googlePlaceId,
	googleMapsUri AS website_url,
	'Google Map Url' AS description,
    'Directions' as type
FROM google.places
WHERE googleMapsUri IS NOT NULL AND googleMapsUri != '';

-- View to retrieve the operating hours header data
CREATE OR REPLACE VIEW google.v_operating_hours AS
SELECT
    DISTINCT googlePlaceId,
    'Regular' AS name,
    'Google Regular Operating Hours' AS description,
    'Regular' AS type
FROM google.operating_hours
WHERE hoursType != 'current';

-- View to retrieve the operating hours detail data
CREATE OR REPLACE VIEW google.v_operating_hours_times AS
SELECT
	id,
    googlePlaceId,
    CASE WHEN hoursType = 'regular' THEN 'Regular' ELSE 'Exception' END AS type,
    CASE WHEN dayOfWeek = 0 THEN 7 ELSE dayOfWeek END AS day_of_week,
    openHour AS day_start_hour,
    openMinute AS day_start_minute,
    closeHour AS day_end_hour,
    closeMinute AS day_end_minute,
    specificDate AS specific_day,
    CASE
	    WHEN openHour = 0 AND openMinute = 0 AND closeHour = 0 AND closeMinute = 0 THEN 'All Day'
        WHEN openHour IS NOT NULL THEN
            LPAD(CAST(CASE WHEN openHour % 12 = 0 THEN 12 ELSE openHour % 12 END AS VARCHAR), 2, '0') || ':' ||
            LPAD(CAST(openMinute AS VARCHAR), 2, '0') || ' ' ||
            CASE WHEN openHour >= 12 THEN 'PM' ELSE 'AM' END || ' - ' ||
            LPAD(CAST(CASE WHEN closeHour % 12 = 0 THEN 12 ELSE closeHour % 12 END AS VARCHAR), 2, '0') || ':' ||
            LPAD(CAST(closeMinute AS VARCHAR), 2, '0') || ' ' ||
            CASE WHEN closeHour >= 12 THEN 'PM' ELSE 'AM' END
        ELSE 'Closed or UNKNOWN'
    END AS descriptive_time
FROM google.operating_hours where hoursType != 'current';

-- Start Transaction

BEGIN TRANSACTION;

-- Migrate unique designations to the location types dimension table
INSERT INTO normalized.LocationTypes (location_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(location_type_key), 0) FROM normalized.LocationTypes) + row_number() OVER (),
    location_type,
    'Google Places Type'
FROM google.v_location_types
WHERE location_type NOT IN (SELECT name FROM normalized.LocationTypes);

-- Migrate Places
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, description, ai_generative_description, state_abbre, state, data_source_key, location_type_key, orig_data_source_key, migration_primary_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.description, v.ai_generative_description, v.state_abbre, v.state, v.data_source_key, lt.location_type_key, v.orig_data_source_key, v.migration_primary_key, v.attributes
FROM google.v_places v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

-- Migrate Addresses
-- First, ensure all Google address types are in the AddressTypes table
INSERT INTO normalized.AddressTypes (address_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(address_type_key), 0) FROM normalized.AddressTypes) + row_number() OVER (),
    trim(type),
    'Google Places Address Type'
FROM (SELECT DISTINCT type FROM google.v_addresses WHERE type IS NOT NULL)
WHERE trim(type) NOT IN (SELECT name FROM normalized.AddressTypes);

INSERT INTO normalized.Addresses (
    address_key, location_key, address_type_key, postal_code, city, state_abbre, state, address_line_1, address_line_2, address_line_3, country_code
)
SELECT
    (SELECT COALESCE(MAX(address_key), 0) FROM normalized.Addresses) + row_number() OVER (),
    l.location_key,
    addr_type.address_type_key,
    v.postal_code,
    v.city,
    v.state_abbre,
    v.state,
    v.address_line_1,
    v.address_line_2,
    v.address_line_3,
    v.country_code
FROM google.v_addresses v
JOIN normalized.Locations l ON v.googlePlaceId = l.orig_data_source_key AND l.data_source_key = 4
JOIN normalized.AddressTypes addr_type ON trim(v.type) = trim(addr_type.name);

-- Migrate Contacts (Phones)
INSERT INTO normalized.ContactPhoneNumbers (contact_phone_number_key, location_key, phone_number, type, description)
SELECT
    (SELECT COALESCE(MAX(contact_phone_number_key), 0) FROM normalized.ContactPhoneNumbers) + row_number() OVER (),
    l.location_key,
    v.phone_number,
    v.type,
    '' -- description
FROM google.v_contact_phones v
JOIN normalized.Locations l ON v.googlePlaceId = l.orig_data_source_key AND l.data_source_key = 4;

-- Migrate Websites
-- First, ensure all Google website types are in the WebsiteTypes table
INSERT INTO normalized.WebsiteTypes (website_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(website_type_key), 0) FROM normalized.WebsiteTypes) + row_number() OVER (),
    trim(type),
    'Google Places Website Type'
FROM (SELECT DISTINCT type FROM google.v_websites WHERE type IS NOT NULL)
WHERE trim(type) NOT IN (SELECT name FROM normalized.WebsiteTypes);

INSERT INTO normalized.Websites (website_key, website_type_key, location_key, url, description)
SELECT
    (SELECT COALESCE(MAX(website_key), 0) FROM normalized.Websites) + row_number() OVER (),
    wt.website_type_key,
    l.location_key,
    v.website_url,
    COALESCE(v.description, '')
FROM (
    SELECT id, googlePlaceId, website_url, 'Primary Google URL' AS description, 'Primary' as type FROM google.v_websites WHERE type = 'Primary'
    UNION ALL
    SELECT id, googlePlaceId, website_url, 'Google Map Url' AS description, 'Directions' as type FROM google.v_websites WHERE type = 'Directions'
) v
JOIN normalized.Locations l ON v.googlePlaceId = l.orig_data_source_key AND l.data_source_key = 4
JOIN normalized.WebsiteTypes wt ON wt.name = v.type;

-- Migrate Operating Hours (Header)
INSERT INTO normalized.OperatingHours (
    operating_hours_key, location_key, name, description, type
)
SELECT
    (SELECT COALESCE(MAX(operating_hours_key), 0) FROM normalized.OperatingHours) + row_number() OVER (),
    l.location_key,
    v.name,
    v.description,
    v.type
FROM google.v_operating_hours v
JOIN normalized.Locations l ON v.googlePlaceId = l.orig_data_source_key AND l.data_source_key = 4;

-- Migrate Operating Hours Times (Detail)
INSERT INTO normalized.OperatingHoursTimes (
    operating_hours_time_key, operating_hours_key, day_of_week, day_start_hour, day_start_minute, day_end_hour, day_end_minute, descriptive_time, specific_day
)
SELECT
    (SELECT COALESCE(MAX(operating_hours_time_key), 0) FROM normalized.OperatingHoursTimes) + row_number() OVER (),
    oh.operating_hours_key,
    v.day_of_week,
    v.day_start_hour,
    v.day_start_minute,
    v.day_end_hour,
    v.day_end_minute,
    v.descriptive_time,
    v.specific_day
FROM google.v_operating_hours_times v
JOIN normalized.Locations l ON v.googlePlaceId = l.orig_data_source_key AND l.data_source_key = 4
JOIN normalized.OperatingHours oh ON oh.location_key = l.location_key AND oh.type = v.type;

-- Migrate Media
INSERT INTO normalized.Media (
    media_key, media_type_key, location_key, credit, title, url, width, height
)
SELECT
    (SELECT COALESCE(MAX(media_key), 0) FROM normalized.Media) + row_number() OVER (),
    COALESCE(mt.media_type_key, 0),
    l.location_key,
    v.credit,
    v.title,
    v.url,
    v.width,
    v.height
FROM google.v_media v
JOIN normalized.Locations l ON v.googlePlaceId = l.orig_data_source_key AND l.data_source_key = 4
LEFT JOIN normalized.MediaTypes mt ON mt.name = 'Photo';

COMMIT;
