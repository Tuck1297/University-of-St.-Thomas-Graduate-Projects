-- DuckDB SQL Queries - Migrate NPS Data

-- Views Created for NPS Data Migration

-- View unique designations from the parks table to populate the location types dimension table during the migration
CREATE VIEW nps.v_location_types AS
SELECT DISTINCT
    designation AS location_type
FROM nps.parks
WHERE designation IS NOT NULL

-- View to denormalize the parks data by including activity and topic names as comma-separated lists
CREATE VIEW nps.v_locations AS
SELECT
    id AS migration_primary_key,
    npsId AS orig_data_source_key,
    name,
    description,
    latitude,
    longitude,
    states,
    weatherInfo as weather_info,
    designation AS location_type, -- Data Source Key determined from here during the migration.
    1 AS data_source_key, -- NPS datasource
    TO_JSON(STRUCT_PACK(
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
CREATE VIEW nps.v_location_campgrounds AS
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
CREATE VIEW nps.v_media AS
SELECT * FROM (
SELECT
	id,
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

UNION

SELECT
	id,
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
CREATE VIEW nps.park_websites AS
SELECT
	id AS park_id,
	directionsInfo AS directions_info,
	directionsUrl AS directions_url,
	url AS primary_url
FROM nps.parks;

CREATE VIEW nps.campground_websites
SELECT
	c.id as campground_id,
	c.regulationsUrl AS regulations_url,
    c.directionsUrl AS directions_url,
    c.reservationsUrl AS reservations_url,
    c.reservationsDescription AS reservations_description,
    p.url AS primary_url
FROM nps.campgrounds c
JOIN nps.parks p
	ON p.parkCode = c.parkCode;

-- View to retrieve and transform the operating hours data for parks and campgrounds for migration to the location operating hours entity






-- Start Transaction

BEGIN TRANSACTION;

-- Migrate unique park designations to the location types dimension table
-- view created

-- Migrate the denormalized parks data to the locations table
-- view created

-- Migrate the denormalized campgrounds data to the locations table
-- view created

-- Migrate the media data to the media table, associating each media item with the correct location based on the original data source keys and location types
-- view created

-- Migrate the park and campground website urls to the website entity
-- cire created

-- Migrate the location operating hours
-- TODO: create view

-- Migrate the location and campground addresses
-- TODO: create view

-- Migrate the location and campground contacts
-- TODO: create view


COMMIT;