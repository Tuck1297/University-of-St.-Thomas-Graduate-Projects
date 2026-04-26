-- DuckDB Normalized Schema

-- Ensure that DuckDB json and spatial extensions are installed.
INSTALL json;
LOAD json;
INSTALL spatial;
LOAD spatial;

-- Start Transaction

BEGIN TRANSACTION;

-- Create Schema
CREATE SCHEMA IF NOT EXISTS normalized;

-- Drop Tables to make way for creating new tables

/* NOTE: If you don't want to drop tables without checking if there is data in them, you will have to run
         that check in a python script or manually before running this SQL script. */

DROP TABLE IF EXISTS normalized.OperatingHours;
DROP TABLE IF EXISTS normalized.Media;
DROP TABLE IF EXISTS normalized.MediaTypes;
DROP TABLE IF EXISTS normalized.Websites;
DROP TABLE IF EXISTS normalized.WebsiteTypes;
DROP TABLE IF EXISTS normalized.ContactPhoneNumbers;
DROP TABLE IF EXISTS normalized.ContactEmailAddresses;
DROP TABLE IF EXISTS normalized.Addresses;
DROP TABLE IF EXISTS normalized.AddressTypes;
DROP TABLE IF EXISTS normalized.Locations;
DROP TABLE IF EXISTS normalized.DataSources;
DROP TABLE IF EXISTS normalized.LocationTypes;

-- Create Tables

CREATE TABLE normalized.DataSources (
    data_source_key INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL DEFAULT '',
    description VARCHAR(200) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert default data sources
INSERT INTO normalized.DataSources (data_source_key, name, description) VALUES
(0, 'UNKNOWN', 'Unknown data source'),
(1, 'NPS', 'Data Collected from the National Park Service (NPS) API'),
(2, 'MN DNR', 'Data Collected from the Minnesota Department of Natural Resources (MN DNR)'),
(3, 'RIDB', 'Data Collected from the Recreation.gov website.'),
(4, 'Google Places API', 'Data sourced from the Google Places API.'),
(5, 'MN GIS', 'Data sourced from the Minnesota Geospatial Commons website.');


CREATE TABLE normalized.LocationTypes (
    location_type_key INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL DEFAULT '',
    description VARCHAR(200) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert default location types
INSERT INTO normalized.LocationTypes (location_type_key, name, description) VALUES
(0, 'UNKNOWN', 'Unknown location type'),
(1, 'National Park', 'A national park location'),
(2, 'State Park', 'A state park location'),
(3, 'Recreation Area', 'A recreation area location'),
(4, 'National Forest', 'A national forest location'),
(5, 'State Forest', 'A state forest location'),
(6, 'Campground', 'A campground location'),
(7, 'Visitor Center', 'A visitor center location');

CREATE TABLE normalized.Locations (
    location_key INTEGER PRIMARY KEY,
    name VARCHAR(300) NOT NULL DEFAULT '',
    location_parent_key INTEGER NOT NULL DEFAULT 0,
    data_source_key INTEGER NOT NULL DEFAULT 0,
    location_type_key INTEGER NOT NULL DEFAULT 0,
    orig_data_source_key VARCHAR(300), -- This is the original unique identifier for the location from the source system (e.g., npsId for NPS data)
    migration_primary_key INTEGER, -- This is the primary key from the table that the data was placed into during collection. This can be used for traceability back to the raw data.
    latitude DOUBLE NOT NULL DEFAULT 0.0,
    longitude DOUBLE NOT NULL DEFAULT 0.0,
    directions_info VARCHAR(1000) NOT NULL DEFAULT '',
    weather_info VARCHAR(1000) NOT NULL DEFAULT '',
    ai_generative_description VARCHAR(1000) NOT NULL DEFAULT '',
    description VARCHAR(1000) NOT NULL DEFAULT '',
    state_abbre VARCHAR(5) NOT NULL DEFAULT '',
    state VARCHAR(30) NOT NULL DEFAULT '',
    states STRING[], -- For locations that span multiple states, we can store an array of state abbreviations or names
    attributes JSON,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (data_source_key) REFERENCES normalized.DataSources(data_source_key),
    FOREIGN KEY (location_type_key) REFERENCES normalized.LocationTypes(location_type_key),
    FOREIGN KEY (location_parent_key) REFERENCES normalized.Locations(location_key)
);

CREATE TABLE normalized.OperatingHours (
    operating_hours_key INTEGER PRIMARY KEY,
    location_key INTEGER NOT NULL DEFAULT 0,
    name VARCHAR(100) NOT NULL DEFAULT '',
    description VARCHAR(200) NOT NULL DEFAULT '',
    day_of_week INTEGER, -- 1=Monday, 2=Tuesday, ..., 7=Sunday
    day_start_hour INTEGER, -- Hour of the day when the location opens (0-23)
    day_end_hour INTEGER, -- Hour of the day when the location closes (0-23
    day_start_minute INTEGER, -- Minute of the hour when the location opens (0-59)
    day_end_minute INTEGER, -- Minute of the hour when the location closes (0-59)
    descriptive_time VARCHAR(100) NOT NULL DEFAULT '',
    start_time_period TIMESTAMP, -- Period in which these operating hours are valid (e.g., for seasonal hours)
    end_time_period TIMESTAMP, -- Period in which these operating hours are valid (e.g., for seasonal hours)
    specific_day DATE, -- Specific date for which these operating hours apply (e.g., for holidays)
    type VARCHAR(25) NOT NULL DEFAULT '',
    timezone VARCHAR(30) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_key) REFERENCES normalized.Locations(location_key),
    CONSTRAINT chk_day_of_week CHECK (day_of_week >= 1 AND day_of_week <= 7),
);

CREATE TABLE normalized.MediaTypes (
    media_type_key INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL DEFAULT '',
    description VARCHAR(200) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert default media types
INSERT INTO normalized.MediaTypes (media_type_key, name, description) VALUES
(0, 'UNKNOWN', 'Unknown media type'),
(1, 'Photo', 'Photographic image'),
(2, 'Video', 'Video content'),
(3, 'Audio', 'Audio recording');

CREATE TABLE normalized.Media (
    media_key INTEGER PRIMARY KEY,
    media_type_key INTEGER NOT NULL DEFAULT 0,
    location_key INTEGER NOT NULL DEFAULT 0,
    credit VARCHAR(100) NOT NULL DEFAULT '',
    title VARCHAR(300) NOT NULL DEFAULT '',
    altText VARCHAR(100) NOT NULL DEFAULT '',
    caption VARCHAR(300) NOT NULL DEFAULT '',
    url VARCHAR(300) NOT NULL DEFAULT '',
    subtitle VARCHAR(200) NOT NULL DEFAULT '',
    height INTEGER NOT NULL DEFAULT 0,
    width INTEGER NOT NULL DEFAULT 0,
    third_party_key VARCHAR(300) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_key) REFERENCES normalized.Locations(location_key),
    FOREIGN KEY (media_type_key) REFERENCES normalized.MediaTypes(media_type_key)
);

CREATE TABLE normalized.WebsiteTypes (
    website_type_key INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL DEFAULT '',
    description VARCHAR(200) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert default website types
INSERT INTO normalized.WebsiteTypes (website_type_key, name, description) VALUES
(0, 'UNKNOWN', 'Unknown website type');

CREATE TABLE normalized.Websites (
    website_key INTEGER PRIMARY KEY,
    website_type_key INTEGER NOT NULL DEFAULT 0,
    location_key INTEGER NOT NULL DEFAULT 0,
    url VARCHAR(300) NOT NULL DEFAULT '',
    description VARCHAR(500) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_key) REFERENCES normalized.Locations(location_key),
    FOREIGN KEY (website_type_key) REFERENCES normalized.WebsiteTypes(website_type_key)
);

CREATE TABLE normalized.ContactPhoneNumbers (
    contact_phone_number_key INTEGER PRIMARY KEY,
    location_key INTEGER NOT NULL DEFAULT 0,
    phone_number VARCHAR(16) NOT NULL DEFAULT '',
    description VARCHAR(200) NOT NULL DEFAULT '',
    extension VARCHAR(3) NOT NULL DEFAULT '',
    type VARCHAR(25) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_key) REFERENCES normalized.Locations(location_key)
);

CREATE TABLE normalized.ContactEmailAddresses (
    contact_email_address_key INTEGER PRIMARY KEY,
    location_key INTEGER NOT NULL DEFAULT 0,
    email_address VARCHAR(150) NOT NULL DEFAULT '',
    description VARCHAR(200) NOT NULL DEFAULT '',
    type VARCHAR(25) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_key) REFERENCES normalized.Locations(location_key)
);

CREATE TABLE normalized.AddressTypes (
    address_type_key INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL DEFAULT '',
    description VARCHAR(200) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Insert default address types
INSERT INTO normalized.AddressTypes (address_type_key, name, description) VALUES
(0, 'UNKNOWN', 'Unknown address type');

CREATE TABLE normalized.Addresses (
    address_key INTEGER PRIMARY KEY,
    location_key INTEGER NOT NULL DEFAULT 0,
    address_type_key INTEGER NOT NULL DEFAULT 0,
    postal_code VARCHAR(12) NOT NULL DEFAULT '',
    city VARCHAR(150) NOT NULL DEFAULT '',
    state_abbre VARCHAR(5) NOT NULL DEFAULT '',
    state VARCHAR(50) NOT NULL DEFAULT '',
    address_line_1 VARCHAR(100) NOT NULL DEFAULT '',
    address_line_2 VARCHAR(100) NOT NULL DEFAULT '',
    address_line_3 VARCHAR(100) NOT NULL DEFAULT '',
    county VARCHAR(200) NOT NULL DEFAULT '',
    country_code VARCHAR(5) NOT NULL DEFAULT '',
    created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_key) REFERENCES normalized.Locations(location_key),
    FOREIGN KEY (address_type_key) REFERENCES normalized.AddressTypes(address_type_key)
);

-- Commit

COMMIT;
