-- DuckDB SQL Queries - Migrate Google Places Data

-- Views Created for Google Places Data Migration

-- View unique designations from the google places data to populate the location types dimension table
CREATE OR REPLACE VIEW google.v_location_types AS
SELECT DISTINCT primaryType AS location_type FROM google.places WHERE primaryType IS NOT NULL;

-- View to transform Google Places
CREATE OR REPLACE VIEW google.v_places AS
SELECT
    primaryDisplayName AS name,
    latitude,
    longitude,
    editorialSummary AS description,
    geminiGenerativeSummary AS ai_generative_description,
    administrativeArea AS state_abbre,
    state,
    primaryType AS location_type,
    4 AS data_source_key, -- Google Places API
    googlePlaceId AS orig_data_source_key,
    id AS migration_primary_key,
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
        short_formatted_address := shortFormattedAddress
    )) AS attributes,
    nationalPhoneNumber AS phone,
    websiteUri AS website_url
FROM google.places;

-- View to transform Photos
CREATE OR REPLACE VIEW google.v_media AS
SELECT
    googlePlaceId AS orig_id,
    authorDisplayName AS credit,
    googlePhotoName AS title,
    photoUri AS url,
    widthPx AS width,
    heightPx AS height,
    'Image' AS media_type
FROM google.photos;


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
INSERT INTO normalized.Addresses (
    address_key, location_key, address_type_key, postal_code, city, state_abbre, state, address_line_1, country_code
)
SELECT
    (SELECT COALESCE(MAX(address_key), 0) FROM normalized.Addresses) + row_number() OVER (),
    l.location_key,
    (SELECT address_type_key FROM normalized.AddressTypes WHERE name = 'Physical' LIMIT 1),
    gp.postalCode,
    gp.locality,
    gp.administrativeArea,
    gp.state,
    gp.formattedAddress,
    gp.regionCode
FROM google.places gp
JOIN normalized.Locations l ON gp.googlePlaceId = l.orig_data_source_key AND l.data_source_key = 4;

-- Migrate Contacts (Phones)
INSERT INTO normalized.ContactPhoneNumbers (contact_phone_number_key, location_key, phone_number, type)
SELECT
    (SELECT COALESCE(MAX(contact_phone_number_key), 0) FROM normalized.ContactPhoneNumbers) + row_number() OVER (),
    l.location_key,
    v.phone,
    'Mobile'
FROM google.v_places v
JOIN normalized.Locations l ON v.orig_data_source_key = l.orig_data_source_key AND l.data_source_key = 4
WHERE v.phone IS NOT NULL;

-- Migrate Websites
INSERT INTO normalized.Websites (website_key, website_type_key, location_key, url, description)
SELECT
    (SELECT COALESCE(MAX(website_key), 0) FROM normalized.Websites) + row_number() OVER (),
    (SELECT website_type_key FROM normalized.WebsiteTypes WHERE name = 'Primary' LIMIT 1),
    l.location_key,
    v.website_url,
    'Official Website'
FROM google.v_places v
JOIN normalized.Locations l ON v.orig_data_source_key = l.orig_data_source_key AND l.data_source_key = 4
WHERE v.website_url IS NOT NULL;

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
JOIN normalized.Locations l ON v.orig_id = l.orig_data_source_key AND l.data_source_key = 4
LEFT JOIN normalized.MediaTypes mt ON mt.name = 'Photo';

COMMIT;
