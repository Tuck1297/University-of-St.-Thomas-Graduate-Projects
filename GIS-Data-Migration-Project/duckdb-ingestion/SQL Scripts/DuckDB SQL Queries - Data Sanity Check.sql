-- Google Script Sanity Check

SELECT 'AddressDescriptorsCount' AS metric, COUNT(*) AS value FROM google.address_descriptors
UNION ALL
SELECT 'OperatingHoursCount', COUNT(*) FROM google.operating_hours
UNION ALL
SELECT 'PhotosCount', COUNT(*) FROM google.photos
UNION ALL
SELECT 'PlacesCount', COUNT(*) FROM google.places;

-- Find all places that are State Park related
SELECT * FROM google.places where primaryDisplayName like '%State Park%';

-- Find all duplicate places
SELECT googleName, count(*) AS count FROM google.places GROUP BY googleName
ORDER BY count DESC;

-- Location similar names based on primaryDisplayName
SELECT * FROM google.places where
primaryDisplayName in (
'Central Park',
'Memorial Park',
'Lions Park',
'Riverside Park',
'City Park',
'Centennial Park',
'County Park',
'Superior National Forest',
'North Park',
'Shady Oaks Campground',
'Chippewa National Forest',
'Clear Lake Park',
'Bear Park',
'Lost River State Forest',
'Keene Creek Park',
'Cascade Falls',
'Sand Dunes State Forest',
'Spring Lake Park Reserve',
'Otsego County Park',
'Riverfront Park',
'Wolf Creek Falls',
'Gooseberry Falls',
'Trailhead Park',
'Tioga Beach',
'Three Rivers Park District',
'Silver Lake Park',
'Veterans Park',
'Gateway Park',
'Finland State Forest',
'Solana State Forest',
'Baker Park Reserve',
'Levee Park',
'Lagoon Park',
'Koochiching State Forest',
'Northeast Park',
'Legion Park',
'George Washington State Forest',
'Hidden Falls',
'Paul Bunyan State Forest',
'Rice Park',
'Harrison Park',
'Snake River Campground',
'City Square Park',
'Pillsbury State Forest',
'Grand Portage',
'Maplewood Park',
'Woodridge Park',
'Alexander Park',
'Pine Island State Forest',
'Rum River State Forest',
'Illgen Falls',
'Huntersville State Forest',
'Fond du Lac State Forest',
'Savanna State Forest',
'Interstate State Park',
'Pine Grove Park',
'Spring Lake Park',
'Mayo Park',
'Erlandson Park',
'Northwest Angle State Forest',
'Westside Park',
'Heritage Park',
'Lakeside Park'
);

SELECT * FROM google.address_descriptors;

SELECT * FROM google.photos;

-- MN DNR Script Sanity Check

SELECT 'StateParkInfoCount' AS metric, COUNT(*) AS VALUE FROM mn_dnr.extracted_table_0;

SELECT * FROM mn_dnr.extracted_table_0;

-- MN GIS Script Sanity Check

SELECT 'ParkUnitsCount' AS metric, COUNT(*) AS VALUE FROM mn_gis.dnr_management_units_prk
UNION ALL
SELECT 'ParkUnitsPtsCount', COUNT(*) FROM mn_gis.dnr_management_units_prk_ref_pts
UNION ALL
SELECT 'PlanAreasCount', COUNT(*) FROM mn_gis.dnr_stat_plan_areas_prk
UNION ALL
SELECT 'StateForestCampgroundsCount', COUNT(*) FROM mn_gis.state_forest_campgrounds_and_day_use_areas
UNION ALL
SELECT 'StateForestCampgroundsOrigCount', COUNT(*) FROM mn_gis.state_forest_campgrounds_and_day_use_areas_orig;

SELECT * FROM mn_gis.dnr_management_units_prk;
SELECT * FROM mn_gis.dnr_management_units_prk_ref_pts;
SELECT * FROM mn_gis.dnr_stat_plan_areas_prk;
SELECT * FROM mn_gis.state_forest_campgrounds_and_day_use_areas;
SELECT * FROM mn_gis.state_forest_campgrounds_and_day_use_areas_orig;

-- MN GIS Campsite Script Sanity Check

SELECT 'CampsitesCount' AS metric, COUNT(*) AS VALUE FROM mn_gis_campsites.struc_parks_and_trails_campsites;

SELECT * FROM mn_gis_campsites.struc_parks_and_trails_campsites;

-- NPS Script Sanity Check

SELECT 'ActivityCount' AS metric, COUNT(*) AS VALUE FROM nps.activities
UNION ALL
SELECT 'AddressCount', COUNT(*) FROM nps.addresses
UNION ALL
SELECT 'AmenityCount', COUNT(*) FROM nps.amenities
UNION ALL
SELECT 'CampgroundAmenityCount', COUNT(*) FROM nps.campground_amenities
UNION ALL
SELECT 'CampgroundCampsiteCount', COUNT(*) FROM nps.campground_campsites
UNION ALL
SELECT 'CampgroundCount', COUNT(*) FROM nps.campgrounds
UNION ALL
SELECT 'EmailAddressCount', COUNT(*) FROM nps.contact_email_addresses
UNION ALL
SELECT 'PhoneNumbersCount', COUNT(*) FROM nps.contact_phone_numbers
UNION ALL
SELECT 'EntranceFeesCount', COUNT(*) FROM nps.entrance_fees
UNION ALL
SELECT 'EntrancePassesCount', COUNT(*) FROM nps.entrance_passes
UNION ALL
SELECT 'FeesCount', COUNT(*) FROM nps.fees
UNION ALL
SELECT 'ImagesCount', COUNT(*) FROM nps.images
UNION ALL
SELECT 'MultimediaCount', COUNT(*) FROM nps.multimedia
UNION ALL
SELECT 'OperatingHoursCount', COUNT(*) FROM nps.operating_hours
UNION ALL
SELECT 'OperatingHoursExceptionsCount', COUNT(*) FROM nps.operating_hours_exceptions
UNION ALL
SELECT 'ParkCount', COUNT(*) FROM nps.parks
UNION ALL
SELECT 'TopicCount', COUNT(*) FROM nps.topics;


SELECT * FROM nps.activities;

SELECT * FROM nps.addresses;

SELECT * FROM nps.amenities;

SELECT * FROM nps.campground_amenities;

SELECT * FROM nps.campground_campsites;

SELECT * FROM nps.campgrounds;

SELECT * FROM nps.contact_email_addresses;

SELECT * FROM nps.contact_phone_numbers;

SELECT * FROM nps.entrance_fees;

SELECT * FROM nps.entrance_passes;

SELECT * FROM nps.fees;

SELECT * FROM nps.images;

SELECT * FROM nps.multimedia;

SELECT * FROM nps.operating_hours;

SELECT * FROM nps.operating_hours_exceptions;

SELECT * FROM nps.parks;

SELECT * FROM nps.topics;


-- RIDB Script Sanity Check

SELECT 'ActivitiesCount' AS metric, COUNT(*) AS VALUE FROM  ridb.Activities_API_v1
UNION ALL
SELECT 'CampsiteAttributes', COUNT(*) FROM ridb.CampsiteAttributes_API_v1
UNION ALL
SELECT 'CampsitesCount', COUNT(*) FROM ridb.Campsites_API_v1
UNION ALL
SELECT 'EntityActivityCount', COUNT(*) FROM ridb.EntityActivities_API_v1
UNION ALL
SELECT 'EventsCount', COUNT(*) FROM ridb.Events_API_v1
UNION ALL
SELECT 'FacilitiesCount', COUNT(*) FROM ridb.Facilities_API_v1
UNION ALL
SELECT 'FacilityAddressesCount', COUNT(*) FROM ridb.FacilityAddresses_API_v1
UNION ALL
SELECT 'LinksCount', COUNT(*) FROM ridb.Links_API_v1
UNION ALL
SELECT 'MediaCount', COUNT(*) FROM ridb.Media_API_v1
UNION ALL
SELECT 'MemberToursCount', COUNT(*) FROM ridb.MemberTours_API_v1
UNION ALL
SELECT 'OrganizationsCount', COUNT(*) FROM ridb.Organizations_API_v1
UNION ALL
SELECT 'OrgEntitiesCount', COUNT(*) FROM ridb.OrgEntities_API_v1
UNION ALL
SELECT 'PermitEntranceAttribCount', COUNT(*) FROM ridb.PermitEntranceAttributes_API_v1
UNION ALL
SELECT 'PermitEntrancesCount', COUNT(*) FROM ridb.PermitEntrances_API_v1
UNION ALL
SELECT 'PermitEntranceZoneCount', COUNT(*) FROM ridb.PermitEntranceZones_API_v1
UNION ALL
SELECT 'PermittedEquipCount', COUNT(*) FROM ridb.PermittedEquipment_API_v1
UNION ALL
SELECT 'RecAreaAddressesCount', COUNT(*) FROM ridb.RecAreaAddresses_API_v1
UNION ALL
SELECT 'RecAreaFacilitiesCount', COUNT(*) FROM ridb.RecAreaAddresses_API_v1
UNION ALL
SELECT 'RecAreasCount', COUNT(*) FROM ridb.RecAreas_API_v1
UNION ALL
SELECT 'TourAttributesCount', COUNT(*) FROM ridb.TourAttributes_API_v1
UNION ALL
SELECT 'ToursCount', COUNT(*) FROM ridb.Tours_API_v1;


SELECT * FROM  ridb.Activities_API_v1;

SELECT * FROM ridb.CampsiteAttributes_API_v1;

SELECT * FROM ridb.Campsites_API_v1;

SELECT * FROM ridb.EntityActivities_API_v1;

SELECT * FROM ridb.Events_API_v1;

SELECT * FROM ridb.Facilities_API_v1;

SELECT * FROM ridb.FacilityAddresses_API_v1;

SELECT * FROM ridb.Links_API_v1;

SELECT * FROM ridb.Media_API_v1;

SELECT * FROM ridb.MemberTours_API_v1;

SELECT * FROM ridb.Organizations_API_v1 ;

SELECT * FROM ridb.OrgEntities_API_v1;

SELECT * FROM ridb.PermitEntranceAttributes_API_v1;

SELECT * FROM ridb.PermitEntrances_API_v1;

SELECT * FROM ridb.PermitEntranceZones_API_v1;

SELECT * FROM ridb.PermittedEquipment_API_v1;

SELECT * FROM ridb.RecAreaAddresses_API_v1;

SELECT * FROM ridb.RecAreaAddresses_API_v1;

SELECT * FROM ridb.RecAreas_API_v1;

SELECT * FROM ridb.TourAttributes_API_v1;

SELECT * FROM ridb.Tours_API_v1;



