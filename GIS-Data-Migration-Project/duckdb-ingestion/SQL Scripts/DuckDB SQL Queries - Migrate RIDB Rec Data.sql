-- DuckDB SQL Queries - Migrate RIDB Rec Data

-- Views Created for RIDB Data Migration

-- View unique designations to populate the location types dimension table
CREATE OR REPLACE VIEW ridb.v_location_types AS
SELECT DISTINCT 'Recreation Area' AS location_type
UNION
SELECT DISTINCT 'Facility' AS location_type;

-- View to transform RecAreas
CREATE OR REPLACE VIEW ridb.v_rec_areas AS
SELECT
    RecAreaName AS name,
    RecAreaLatitude AS latitude,
    RecAreaLongitude AS longitude,
    RecAreaDirections AS directions_info,
    RecAreaDescription AS description,
    'Recreation Area' AS location_type,
    3 AS data_source_key, -- RIDB
    CAST(RecAreaID AS VARCHAR) AS orig_data_source_key,
    NULL AS migration_primary_key,
    TO_JSON(STRUCT_PACK(
        org_rec_area_id := OrgRecAreaID,
        stay_limit := StayLimit,
        keywords := Keywords,
        reservable := Reservable,
        enabled := Enabled,
        last_updated := LastUpdatedDate
    )) AS attributes,
    RecAreaPhone AS phone,
    RecAreaEmail AS email
FROM ridb.RecAreas_API_v1;

-- View to transform Facilities
CREATE OR REPLACE VIEW ridb.v_facilities AS
SELECT
    FacilityName AS name,
    FacilityLatitude AS latitude,
    FacilityLongitude AS longitude,
    FacilityDirections AS directions_info,
    FacilityDescription AS description,
    'Facility' AS location_type,
    3 AS data_source_key, -- RIDB
    CAST(FacilityID AS VARCHAR) AS orig_data_source_key,
    NULL AS migration_primary_key,
    TO_JSON(STRUCT_PACK(
        facility_type := FacilityTypeDescription,
        stay_limit := StayLimit,
        keywords := Keywords,
        reservable := Reservable,
        enabled := Enabled,
        last_updated := LastUpdatedDate
    )) AS attributes,
    FacilityPhone AS phone,
    FacilityEmail AS email
FROM ridb.Facilities_API_v1;

-- View to transform Addresses
CREATE OR REPLACE VIEW ridb.v_addresses AS
SELECT
    CAST(RecAreaID AS VARCHAR) AS orig_id,
    'RECAREA' as source,
    PostalCode AS postal_code,
    City AS city,
    AddressStateCode AS state_abbre,
    RecAreaStreetAddress1 AS line1,
    RecAreaStreetAddress2 AS line2,
    RecAreaStreetAddress3 AS line3,
    AddressCountryCode AS country_code,
    RecAreaAddressType AS type
FROM ridb.RecAreaAddresses_API_v1
UNION ALL
SELECT
    CAST(FacilityID AS VARCHAR) AS orig_id,
    'FACILITY' as source,
    PostalCode AS postal_code,
    City AS city,
    AddressStateCode AS state_abbre,
    FacilityStreetAddress1 AS line1,
    FacilityStreetAddress2 AS line2,
    FacilityStreetAddress3 AS line3,
    AddressCountryCode AS country_code,
    FacilityAddressType AS type
FROM ridb.FacilityAddresses_API_v1;

-- View to transform Media
CREATE OR REPLACE VIEW ridb.v_media AS
SELECT
    EntityID as orig_id,
    EntityType as entity_type,
    Title AS title,
    Subtitle AS subtitle,
    Description AS caption,
    URL AS url,
    Height AS height,
    Width AS width,
    Credits AS credit,
    MediaType AS media_type
FROM ridb.Media_API_v1;


-- Start Transaction

BEGIN TRANSACTION;

-- Migrate unique designations to the location types dimension table
INSERT INTO normalized.LocationTypes (location_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(location_type_key), 0) FROM normalized.LocationTypes) + row_number() OVER (),
    location_type,
    'RIDB Entity Type'
FROM ridb.v_location_types
WHERE location_type NOT IN (SELECT name FROM normalized.LocationTypes);

-- Migrate RecAreas
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, directions_info, description, data_source_key, location_type_key, orig_data_source_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.directions_info, v.description, v.data_source_key, lt.location_type_key, v.orig_data_source_key, v.attributes
FROM ridb.v_rec_areas v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

-- Migrate Facilities
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, directions_info, description, data_source_key, location_type_key, orig_data_source_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.directions_info, v.description, v.data_source_key, lt.location_type_key, v.orig_data_source_key, v.attributes
FROM ridb.v_facilities v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

-- Migrate Addresses
INSERT INTO normalized.AddressTypes (address_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(address_type_key), 0) FROM normalized.AddressTypes) + row_number() OVER (),
    trim(type),
    'RIDB Address Type'
FROM (SELECT DISTINCT type FROM ridb.v_addresses WHERE type IS NOT NULL)
WHERE trim(type) NOT IN (SELECT name FROM normalized.AddressTypes);

INSERT INTO normalized.Addresses (
    address_key, location_key, address_type_key, postal_code, city, state_abbre, address_line_1, address_line_2, address_line_3, country_code
)
SELECT
    (SELECT COALESCE(MAX(address_key), 0) FROM normalized.Addresses) + row_number() OVER (),
    l.location_key,
    addr_type.address_type_key,
    v.postal_code,
    v.city,
    v.state_abbre,
    v.line1,
    v.line2,
    v.line3,
    v.country_code
FROM ridb.v_addresses v
JOIN normalized.Locations l ON v.orig_id = l.orig_data_source_key AND l.data_source_key = 3
JOIN normalized.AddressTypes addr_type ON trim(v.type) = trim(addr_type.name);

-- Migrate Contacts (Phones)
INSERT INTO normalized.ContactPhoneNumbers (contact_phone_number_key, location_key, phone_number, type)
SELECT
    (SELECT COALESCE(MAX(contact_phone_number_key), 0) FROM normalized.ContactPhoneNumbers) + row_number() OVER (),
    l.location_key,
    v.phone,
    'Work'
FROM (SELECT orig_data_source_key, phone FROM ridb.v_rec_areas WHERE phone IS NOT NULL UNION ALL SELECT orig_data_source_key, phone FROM ridb.v_facilities WHERE phone IS NOT NULL) v
JOIN normalized.Locations l ON v.orig_data_source_key = l.orig_data_source_key AND l.data_source_key = 3;

-- Migrate Contacts (Emails)
INSERT INTO normalized.ContactEmailAddresses (contact_email_address_key, location_key, email_address, type)
SELECT
    (SELECT COALESCE(MAX(contact_email_address_key), 0) FROM normalized.ContactEmailAddresses) + row_number() OVER (),
    l.location_key,
    v.email,
    'Work'
FROM (SELECT orig_data_source_key, email FROM ridb.v_rec_areas WHERE email IS NOT NULL UNION ALL SELECT orig_data_source_key, email FROM ridb.v_facilities WHERE email IS NOT NULL) v
JOIN normalized.Locations l ON v.orig_data_source_key = l.orig_data_source_key AND l.data_source_key = 3;

-- Migrate Media
INSERT INTO normalized.Media (
    media_key, media_type_key, location_key, title, subtitle, caption, url, height, width, credit
)
SELECT
    (SELECT COALESCE(MAX(media_key), 0) FROM normalized.Media) + row_number() OVER (),
    COALESCE(mt.media_type_key, 0),
    l.location_key,
    v.title,
    v.subtitle,
    v.caption,
    v.url,
    v.height,
    v.width,
    v.credit
FROM ridb.v_media v
JOIN normalized.Locations l ON v.orig_id = l.orig_data_source_key AND l.data_source_key = 3
LEFT JOIN normalized.MediaTypes mt ON mt.name = CASE
    WHEN v.media_type IN ('Image', 'Photo') THEN 'Photo'
    WHEN v.media_type = 'Video' THEN 'Video'
    ELSE 'UNKNOWN'
END;

COMMIT;
