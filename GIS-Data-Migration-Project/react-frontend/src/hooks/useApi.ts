import { useQuery, keepPreviousData } from "@tanstack/react-query";
import ky from "ky";
import { useEffect, useState, useMemo, useRef } from "react";
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

/**
 * Basic bounding box query
 */
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
    staleTime: 1000 * 60 * 5,
    placeholderData: keepPreviousData,
  });
}

/**
 * Persistent location query that merges new results with old results 
 * to prevent map "flashing" and maintain a growing cache of seen locations.
 */
export function usePersistentLocations(bbox: BBox | null) {
  const query = useLocations(bbox);
  const [masterCache, setMasterCache] = useState<Map<number, LocationFeature>>(new Map());

  // Use a ref to track processed data to avoid redundant updates
  const lastProcessedData = useRef<GeoJSONResponse | null>(null);

  useEffect(() => {
    const data = query.data;
    if (data?.features && data !== lastProcessedData.current) {
      lastProcessedData.current = data;
      setMasterCache((prev) => {
        const next = new Map(prev);
        let hasNew = false;
        
        data.features.forEach((f) => {
          if (!next.has(f.properties.location_key)) {
            next.set(f.properties.location_key, f);
            hasNew = true;
          }
        });

        if (!hasNew) return prev;

        // Limit cache size to prevent performance degradation
        if (next.size > 1000) {
          const keysToDelete = Array.from(next.keys()).slice(0, next.size - 1000);
          keysToDelete.forEach(k => next.delete(k));
        }

        return next;
      });
    }
  }, [query.data]);

  const allFeatures = useMemo(() => Array.from(masterCache.values()), [masterCache]);

  return {
    ...query,
    data: {
      type: "FeatureCollection" as const,
      features: allFeatures,
    },
  };
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
    staleTime: 1000 * 60 * 2,
    placeholderData: keepPreviousData,
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
    staleTime: 1000 * 60 * 10,
  });
}
