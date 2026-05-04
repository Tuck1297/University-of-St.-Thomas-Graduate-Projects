import { useQuery } from "@tanstack/react-query";
import ky from "ky";
import type { UnifiedLocationAttributes, LocationDetails } from "../types/location.types";

const API_BASE_URL = "http://localhost:8000/api";

const api = ky.create({ prefix: API_BASE_URL });

export interface LocationFeature {
  type: "Feature";
  geometry: {
    type: "Point";
    coordinates: [number, number]; // [lon, lat]
  };
  properties: {
    location_key: number;
    location_type_key: number;
    name: string;
    description: string;
    latitude: number;
    longitude: number;
    ATTRIBUTES: UnifiedLocationAttributes;
    [key: string]: string | number | boolean | UnifiedLocationAttributes | null | undefined;
  };
}

export interface GeoJSONResponse {
  type: "FeatureCollection";
  features: LocationFeature[];
}

export interface BBox {
  min_lon: number;
  min_lat: number;
  max_lon: number;
  max_lat: number;
}

export function useLocations(bbox: BBox | null) {
  return useQuery({
    queryKey: ["locations", bbox],
    queryFn: async () => {
      if (!bbox) return null;
      return api
        .get("locations/box", { searchParams: bbox as unknown as Record<string, string | number | boolean> })
        .json<GeoJSONResponse>();
    },
    enabled: !!bbox,
    staleTime: 1000 * 60 * 5, // 5 minutes
  });
}

export function useSearch(query: string) {
  return useQuery({
    queryKey: ["search", query],
    queryFn: async () => {
      if (!query) return null;
      return api
        .get("locations/search", { searchParams: { q: query } })
        .json<GeoJSONResponse>();
    },
    enabled: !!query,
    staleTime: 1000 * 60 * 2, // 2 minutes
  });
}

export function useLocationDetails(location_key: number | null) {
  return useQuery({
    queryKey: ["location", location_key],
    queryFn: async () => {
      if (location_key === null) return null;
      return api.get(`locations/${location_key}`).json<LocationDetails>();
    },
    enabled: location_key !== null,
    staleTime: 1000 * 60 * 10, // 10 minutes
  });
}
