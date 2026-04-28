-- DuckDB SQL Queries - Migrate RIDB Rec Data

-- Views Created for RIDB Data Migration

-- View unique designations to populate the location types dimension table
CREATE OR REPLACE VIEW ridb.v_location_types AS
SELECT DISTINCT 'Recreation Area' AS location_type
UNION
SELECT DISTINCT 'Facility' AS location_type
UNION
SELECT DISTINCT 'Campsite' AS location_type;

-- Intermediate views for aggregated data
CREATE OR REPLACE VIEW ridb.v_aggregated_rec_area_activities AS
SELECT 
    ea.EntityID,
    json_group_object(a.ActivityName, 
        CASE 
            WHEN ea.ActivityDescription IS NULL OR ea.ActivityDescription = '' 
            THEN to_json(true) 
            ELSE to_json(ea.ActivityDescription) 
        END
    ) as activities_json
FROM ridb.EntityActivities_API_v1 ea
JOIN ridb.Activities_API_v1 a ON ea.ActivityID = a.ActivityID
WHERE ea.EntityType = 'Rec Area'
GROUP BY ea.EntityID;

CREATE OR REPLACE VIEW ridb.v_aggregated_facility_activities AS
SELECT 
    ea.EntityID,
    json_group_object(a.ActivityName, 
        CASE 
            WHEN ea.ActivityDescription IS NULL OR ea.ActivityDescription = '' 
            THEN to_json(true) 
            ELSE to_json(ea.ActivityDescription) 
        END
    ) as activities_json
FROM ridb.EntityActivities_API_v1 ea
JOIN ridb.Activities_API_v1 a ON ea.ActivityID = a.ActivityID
WHERE ea.EntityType = 'Facility'
GROUP BY ea.EntityID;

CREATE OR REPLACE VIEW ridb.v_aggregated_campsite_attributes AS
SELECT 
    EntityID,
    json_group_object(AttributeName, 
        CASE 
            WHEN AttributeValue IS NULL OR AttributeValue = '' 
            THEN to_json(true) 
            ELSE to_json(AttributeValue) 
        END
    ) as attr_json
FROM ridb.CampsiteAttributes_API_v1
WHERE EntityType = 'Campsite'
GROUP BY EntityID;

-- View to transform RecAreas
CREATE OR REPLACE VIEW ridb.v_rec_areas AS
SELECT
    v.RecAreaName AS name,
    v.RecAreaLatitude AS latitude,
    v.RecAreaLongitude AS longitude,
    v.RecAreaDirections AS directions_info,
    v.RecAreaDescription AS description,
    'Recreation Area' AS location_type,
    3 AS data_source_key, -- RIDB
    CAST(v.RecAreaID AS VARCHAR) AS orig_data_source_key,
    v.RecAreaID as ridb_id,
    TO_JSON(STRUCT_PACK(
        org_rec_area_id := v.OrgRecAreaID,
        stay_limit := v.StayLimit,
        keywords := v.Keywords,
        reservable := v.Reservable,
        enabled := v.Enabled,
        last_updated := v.LastUpdatedDate,
        activities := agg.activities_json
    )) AS attributes,
    v.RecAreaPhone AS phone,
    v.RecAreaEmail AS email,
    v.RecAreaReservationURL AS reservation_url,
    v.RecAreaMapURL AS map_url
FROM ridb.RecAreas_API_v1 v
LEFT JOIN ridb.v_aggregated_rec_area_activities agg ON v.RecAreaID = agg.EntityID;

-- View to transform Facilities
CREATE OR REPLACE VIEW ridb.v_facilities AS
SELECT
    v.FacilityName AS name,
    v.FacilityLatitude AS latitude,
    v.FacilityLongitude AS longitude,
    v.FacilityDirections AS directions_info,
    v.FacilityDescription AS description,
    'Facility' AS location_type,
    3 AS data_source_key, -- RIDB
    CAST(v.FacilityID AS VARCHAR) AS orig_data_source_key,
    v.FacilityID as ridb_id,
    v.ParentRecAreaID as parent_ridb_id,
    TO_JSON(STRUCT_PACK(
        facility_type := v.FacilityTypeDescription,
        stay_limit := v.StayLimit,
        keywords := v.Keywords,
        reservable := v.Reservable,
        enabled := v.Enabled,
        last_updated := v.LastUpdatedDate,
        activities := agg.activities_json
    )) AS attributes,
    v.FacilityPhone AS phone,
    v.FacilityEmail AS email,
    v.FacilityReservationURL AS reservation_url,
    v.FacilityMapURL AS map_url
FROM ridb.Facilities_API_v1 v
LEFT JOIN ridb.v_aggregated_facility_activities agg ON v.FacilityID = agg.EntityID;

-- View to transform Campsites
CREATE OR REPLACE VIEW ridb.v_campsites AS
SELECT
    v.CampsiteName AS name,
    v.CampsiteLatitude AS latitude,
    v.CampsiteLongitude AS longitude,
    '' AS directions_info,
    '' AS description,
    'Campsite' AS location_type,
    3 AS data_source_key, -- RIDB
    CAST(v.CampsiteID AS VARCHAR) AS orig_data_source_key,
    v.CampsiteID as ridb_id,
    v.FacilityID as parent_ridb_id,
    -- Combine base attributes with EAV attributes
    json_merge_patch(
        TO_JSON(STRUCT_PACK(
            campsite_type := v.CampsiteType,
            type_of_use := v.TypeOfUse,
            loop := v.Loop,
            accessible := v.CampsiteAccessible,
            created_date := v.CreatedDate,
            last_updated := v.LastUpdatedDate
        )),
        agg.attr_json
    ) AS attributes
FROM ridb.Campsites_API_v1 v
LEFT JOIN ridb.v_aggregated_campsite_attributes agg ON v.CampsiteID = agg.EntityID;

-- View to transform Addresses
CREATE OR REPLACE VIEW ridb.v_addresses AS
SELECT
    CAST(RecAreaID AS VARCHAR) AS orig_id,
    'RECAREA' as source,
    COALESCE(PostalCode, '') AS postal_code,
    COALESCE(City, '') AS city,
    COALESCE(AddressStateCode, '') AS state_abbre,
    COALESCE(RecAreaStreetAddress1, '') AS line1,
    COALESCE(RecAreaStreetAddress2, '') AS line2,
    COALESCE(RecAreaStreetAddress3, '') AS line3,
    COALESCE(AddressCountryCode, '') AS country_code,
    RecAreaAddressType AS type
FROM ridb.RecAreaAddresses_API_v1
UNION ALL
SELECT
    CAST(FacilityID AS VARCHAR) AS orig_id,
    'FACILITY' as source,
    COALESCE(PostalCode, '') AS postal_code,
    COALESCE(City, '') AS city,
    COALESCE(AddressStateCode, '') AS state_abbre,
    COALESCE(FacilityStreetAddress1, '') AS line1,
    COALESCE(FacilityStreetAddress2, '') AS line2,
    COALESCE(FacilityStreetAddress3, '') AS line3,
    COALESCE(AddressCountryCode, '') AS country_code,
    FacilityAddressType AS type
FROM ridb.FacilityAddresses_API_v1;

-- View to transform Websites
CREATE OR REPLACE VIEW ridb.v_websites AS
SELECT orig_data_source_key, reservation_url AS url, 'Reservations' AS type, 'Official Reservation Website' as description FROM ridb.v_rec_areas WHERE reservation_url IS NOT NULL AND reservation_url != ''
UNION ALL
SELECT orig_data_source_key, map_url AS url, 'Directions' AS type, 'Map/Directions Website' as description FROM ridb.v_rec_areas WHERE map_url IS NOT NULL AND map_url != ''
UNION ALL
SELECT orig_data_source_key, reservation_url AS url, 'Reservations' AS type, 'Official Reservation Website' as description FROM ridb.v_facilities WHERE reservation_url IS NOT NULL AND reservation_url != ''
UNION ALL
SELECT orig_data_source_key, map_url AS url, 'Directions' AS type, 'Map/Directions Website' as description FROM ridb.v_facilities WHERE map_url IS NOT NULL AND map_url != '';


-- Start Transaction

BEGIN TRANSACTION;

-- 1. LocationTypes
INSERT INTO normalized.LocationTypes (location_type_key, name, description)
SELECT
    (SELECT COALESCE(MAX(location_type_key), 0) FROM normalized.LocationTypes) + row_number() OVER (),
    location_type,
    'RIDB Entity Type'
FROM ridb.v_location_types
WHERE location_type NOT IN (SELECT name FROM normalized.LocationTypes);

-- 2. RecAreas (Level 1)
INSERT INTO normalized.Locations (
    location_key, name, latitude, longitude, directions_info, description, data_source_key, location_type_key, orig_data_source_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name, v.latitude, v.longitude, v.directions_info, v.description, v.data_source_key, lt.location_type_key, v.orig_data_source_key, v.attributes
FROM ridb.v_rec_areas v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name;

-- 3. Facilities (Level 2)
INSERT INTO normalized.Locations (
    location_key, name, location_parent_key, latitude, longitude, directions_info, description, data_source_key, location_type_key, orig_data_source_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name,
    p.location_key, -- Parent is RecArea
    v.latitude, v.longitude, v.directions_info, v.description, v.data_source_key, lt.location_type_key, v.orig_data_source_key, v.attributes
FROM ridb.v_facilities v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name
LEFT JOIN normalized.Locations p ON CAST(v.parent_ridb_id AS VARCHAR) = p.orig_data_source_key AND p.data_source_key = 3;

-- 4. Campsites (Level 3)
INSERT INTO normalized.Locations (
    location_key, name, location_parent_key, latitude, longitude, directions_info, description, data_source_key, location_type_key, orig_data_source_key, attributes
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM normalized.Locations) + row_number() OVER (),
    v.name,
    p.location_key, -- Parent is Facility
    v.latitude, v.longitude, v.directions_info, v.description, v.data_source_key, lt.location_type_key, v.orig_data_source_key, v.attributes
FROM ridb.v_campsites v
JOIN normalized.LocationTypes lt ON v.location_type = lt.name
LEFT JOIN normalized.Locations p ON CAST(v.parent_ridb_id AS VARCHAR) = p.orig_data_source_key AND p.data_source_key = 3;

-- 5. Addresses
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

-- 6. Contacts (Phones)
INSERT INTO normalized.ContactPhoneNumbers (contact_phone_number_key, location_key, phone_number, type)
SELECT
    (SELECT COALESCE(MAX(contact_phone_number_key), 0) FROM normalized.ContactPhoneNumbers) + row_number() OVER (),
    l.location_key,
    v.phone,
    'Work'
FROM (SELECT orig_data_source_key, phone FROM ridb.v_rec_areas WHERE phone IS NOT NULL AND phone != '' UNION ALL SELECT orig_data_source_key, phone FROM ridb.v_facilities WHERE phone IS NOT NULL AND phone != '') v
JOIN normalized.Locations l ON v.orig_data_source_key = l.orig_data_source_key AND l.data_source_key = 3;

-- 7. Contacts (Emails)
INSERT INTO normalized.ContactEmailAddresses (contact_email_address_key, location_key, email_address, type)
SELECT
    (SELECT COALESCE(MAX(contact_email_address_key), 0) FROM normalized.ContactEmailAddresses) + row_number() OVER (),
    l.location_key,
    v.email,
    'Work'
FROM (SELECT orig_data_source_key, email FROM ridb.v_rec_areas WHERE email IS NOT NULL AND email != '' UNION ALL SELECT orig_data_source_key, email FROM ridb.v_facilities WHERE email IS NOT NULL AND email != '') v
JOIN normalized.Locations l ON v.orig_data_source_key = l.orig_data_source_key AND l.data_source_key = 3;

-- 8. Websites
INSERT INTO normalized.Websites (website_key, website_type_key, location_key, url, description)
SELECT
    (SELECT COALESCE(MAX(website_key), 0) FROM normalized.Websites) + row_number() OVER (),
    wt.website_type_key,
    l.location_key,
    v.url,
    v.description
FROM ridb.v_websites v
JOIN normalized.Locations l ON v.orig_data_source_key = l.orig_data_source_key AND l.data_source_key = 3
JOIN normalized.WebsiteTypes wt ON wt.name = v.type;

-- 9. Media
INSERT INTO normalized.Media (
    media_key, media_type_key, location_key, title, subtitle, caption, url, height, width, credit
)
SELECT
    (SELECT COALESCE(MAX(media_key), 0) FROM normalized.Media) + row_number() OVER (),
    COALESCE(mt.media_type_key, 0),
    l.location_key,
    v.Title,
    v.Subtitle,
    v.Description,
    v.URL,
    v.Height,
    v.Width,
    v.Credits
FROM ridb.Media_API_v1 v
JOIN normalized.Locations l ON CAST(v.EntityID AS VARCHAR) = l.orig_data_source_key AND l.data_source_key = 3
LEFT JOIN normalized.MediaTypes mt ON mt.name = CASE
    WHEN v.MediaType IN ('Image', 'Photo') THEN 'Photo'
    WHEN v.MediaType = 'Video' THEN 'Video'
    ELSE 'UNKNOWN'
END;

COMMIT;
