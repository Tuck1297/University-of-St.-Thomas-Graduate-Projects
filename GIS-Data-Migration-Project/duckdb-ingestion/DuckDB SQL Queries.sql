-- Google Script Sanity Check

SELECT 'AddressDescriptorsCount' AS metric, COUNT(*) AS value FROM google.address_descriptors
UNION ALL
SELECT 'OperatingHoursCount', COUNT(*) FROM google.operating_hours
UNION ALL
SELECT 'PhotosCount', COUNT(*) FROM google.photos
UNION ALL
SELECT 'PlacesCount', COUNT(*) FROM google.places;

-- Find all places that are State Park related
select * from google.places where primaryDisplayName like '%State Park%';

-- Find all duplicate places
select primaryDisplayName, count(*) AS count from google.places GROUP BY primaryDisplayName
ORDER BY count DESC; 

SELECT * from google.places where 
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
'Lakeside Park',
)

select * from google.address_descriptors;

select * from google.photos;

-- MN DNR Script Sanity Check

SELECT 'StateParkInfoCount' AS metric, COUNT(*) AS value from mn_dnr.extracted_table_0;

SELECT * from mn_dnr.extracted_table_0;

-- MN GIS Script Sanity Check

SELECT 'ParkUnitsCount' AS metric, COUNT(*) AS value FROM mn_gis.dnr_management_units_prk
UNION ALL
SELECT 'ParkUnitsPtsCount', COUNT(*) FROM mn_gis.dnr_management_units_prk_ref_pts
UNION ALL
SELECT 'PlanAreasCount', COUNT(*) FROM mn_gis.dnr_stat_plan_areas_prk
UNION ALL
SELECT 'StateForestCampgroundsCount', COUNT(*) FROM mn_gis.state_forest_campgrounds_and_day_use_areas
UNION ALL
SELECT 'StateForestCampgroundsOrigCount', COUNT(*) FROM mn_gis.state_forest_campgrounds_and_day_use_areas_orig;

select * from mn_gis.dnr_management_units_prk;
select * from mn_gis.dnr_management_units_prk_ref_pts;
select * from mn_gis.dnr_stat_plan_areas_prk;
select * from mn_gis.state_forest_campgrounds_and_day_use_areas;
select * from mn_gis.state_forest_campgrounds_and_day_use_areas_orig;

-- NPS Script Sanity Check

SELECT 'ActivityCount' AS metric, COUNT(*) AS value FROM nps.activities
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


select * from nps.activities;

select * from nps.addresses;

select * from nps.amenities;

select * from nps.campground_amenities;

select * from nps.campground_campsites;

select * from nps.campgrounds;

select * from nps.contact_email_addresses;

select * from nps.contact_phone_numbers;

select * from nps.entrance_fees;

select * from nps.entrance_passes;

select * from nps.fees;

select * from nps.images;

select * from nps.multimedia;

select * from nps.operating_hours;

select * from nps.operating_hours_exceptions;

select * from nps.parks;

select * from nps.topics; 


-- RIDB Script Sanity Check

SELECT 'ActivitiesCount' AS metric, COUNT(*) AS value FROM  ridb.Activities_API_v1
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



