/**
 * Attributes specific to Google Places data
 */
export interface GoogleAttributes {
  google_place_id?: string;
  rating?: number;
  user_rating_count?: number;
  price_level?: number;
  business_status?: string;
  primary_type?: string;
  editorial_summary?: string;
  gemini_generative_summary?: string;
}

/**
 * Attributes specific to National Park Service (NPS) data
 */
export interface NpsAttributes {
  park_code: string;
  activities?: string[];
  topics?: string[];
}

/**
 * Attributes specific to Recreation.gov (RIDB) data
 */
export interface RidbAttributes {
  org_rec_area_id?: string | null;
  facility_type?: string;
  stay_limit?: string | null;
  keywords?: string | null;
  reservable?: boolean;
  enabled?: boolean;
  last_updated?: string;
  activities?: Record<string, boolean> | string[] | null;
  tours?: string | number | boolean | null | undefined | Record<string, unknown> | Array<unknown>;
  campsite_type?: string;
  type_of_use?: string;
  loop?: string | null;
  accessible?: boolean;
  [key: string]: string | number | boolean | null | undefined | Record<string, unknown> | Array<unknown>;
}

/**
 * Attributes specific to MN DNR data
 */
export interface MnDnrAttributes {
  winter_camping?: boolean;
  spring_camping?: boolean;
  summer_camping?: boolean;
  fall_camping?: boolean;
  site_drive_in?: boolean;
  site_accessible_drive_in?: boolean;
  max_rv_length?: string;
  rv_pull_through_sites?: boolean;
  horse_campsites?: boolean;
  backpack_sites?: boolean;
  group_campsites?: boolean;
  has_showers?: boolean;
  has_accessible_showers?: boolean;
  has_flush_toilets?: boolean;
  has_accessible_flush_toilets?: boolean;
  has_dump_station?: boolean;
  camper_cabins?: boolean;
  accessible_camper_cabins?: boolean;
  other_lodging?: boolean;
  hiking_trails?: boolean;
  paved_trails?: boolean;
  groomed_cross_country_ski_trails?: boolean;
  swimming_beach?: boolean;
  fishing_pier?: boolean;
  accessible_fishing_pier?: boolean;
  boat_rental?: boolean;
  showshoe_rental?: boolean;
  picnic_shelter?: boolean;
  accessible_picnic_shelter?: boolean;
  nature_programs?: boolean;
  accessible_track_chair?: boolean;
  district?: string;
  region?: string;
  county?: string;
}

/**
 * Attributes specific to MN GIS data
 */
export interface MnGisAttributes {
  gis_acres?: number;
  legislative_id?: string;
  program_project?: string;
  guid?: string;
  state_forest?: string;
  pat_admin?: string;
}

/**
 * A union type representing any possible attribute structure.
 * No base class assumption, just the raw source types.
 */
export type UnifiedLocationAttributes = 
  | GoogleAttributes 
  | NpsAttributes 
  | RidbAttributes 
  | MnDnrAttributes 
  | MnGisAttributes
  | Record<string, unknown>;

/**
 * Sub-types for the complex LocationDetails interface
 */

export interface LocationAddress {
  postal_code: string | null;
  city: string | null;
  state_abbre: string | null;
  address_line_1: string | null;
  address_line_2: string | null;
  address_line_3: string | null;
  address_type: string | null;
}

export interface LocationEmail {
  email_address: string;
  description: string | null;
  email_type: string | null;
}

export interface LocationPhone {
  phone_number: string;
  description: string | null;
  extension: string | null;
  type: string | null;
}

export interface LocationMedia {
  credit: string | null;
  title: string | null;
  altText: string | null;
  caption: string | null;
  url: string;
  subtitle: string | null;
  height: number | null;
  width: number | null;
  media_type: string | null;
}

export interface LocationWebsite {
  url: string;
  description: string | null;
  website_type: string | null;
}

export interface LocationOperatingHour {
  name: string | null;
  description: string | null;
  type: string | null;
}

export interface ChildLocation {
  location_key: number;
  name: string;
  location_type: string;
  latitude: number;
  longitude: number;
}

/**
 * Comprehensive interface representing the full location data 
 * returned by the detailed PostgreSQL query.
 */
export interface LocationDetails {
  name: string;
  location_key: number;
  location_parent_key: number | null;
  latitude: number;
  longitude: number;
  weather_info: string;
  directions_info: string;
  ai_generative_description: string;
  description: string;
  states: string | null;
  attributes: UnifiedLocationAttributes | null;
  active_flag: boolean;
  location_type: string;
  data_source_name: string;
  
  addresses: LocationAddress[] | null;
  email_addresses: LocationEmail[] | null;
  phone_numbers: LocationPhone[] | null;
  media: LocationMedia[] | null;
  websites: LocationWebsite[] | null;
  operating_hours: LocationOperatingHour[] | null;
  children: ChildLocation[] | null;
}

/**
 * Helper to check if location has Google Place ID for photo retrieval
 */
export function hasGooglePlaceId(attrs: unknown): attrs is { google_place_id: string } {
  return !!attrs && typeof attrs === 'object' && ('google_place_id' in attrs || 'googlePlaceId' in attrs);
}
