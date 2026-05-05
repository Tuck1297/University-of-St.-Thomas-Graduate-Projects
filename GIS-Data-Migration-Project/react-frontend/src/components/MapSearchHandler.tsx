"use client";

import { useEffect, useRef } from "react";
import L from "leaflet";
import { useLeafletMap } from "./MapContext";
import type { PoiMarker } from "../types/map.types";

interface MapSearchHandlerProps {
  results: PoiMarker[];
  isSearching: boolean;
}

export default function MapSearchHandler({ results, isSearching }: MapSearchHandlerProps) {
  const map = useLeafletMap();
  const lastResultsRef = useRef<string>("");

  useEffect(() => {
    // Only fit bounds if we are actively searching and have results
    if (!isSearching || results.length === 0) return;

    // Create a unique string to represent the results to avoid unnecessary fits
    const resultsString = results.map(r => r.id).sort().join(",");
    if (resultsString === lastResultsRef.current) return;
    lastResultsRef.current = resultsString;

    const bounds = L.latLngBounds(results.map(r => [r.lat, r.lng]));
    
    // Fit map to results with some padding
    map.fitBounds(bounds, {
      padding: [50, 50],
      maxZoom: 15, // Don't zoom in too far if there's only one result
      animate: true,
      duration: 0.5
    });
  }, [results, isSearching, map]);

  return null;
}
