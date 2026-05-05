"use client";

import { useEffect, useRef } from "react";
import { useLeafletMap } from "./MapContext";
import type { BBox } from "../hooks/useApi";

interface MapBoundsHandlerProps {
  onBoundsChange: (bounds: BBox) => void;
}

/**
 * Rounds a number to a fixed number of decimal places to avoid
 * tiny fluctuations in map coordinates from triggering updates.
 */
function roundCoord(num: number) {
  return Math.round(num * 1000000) / 1000000;
}

export default function MapBoundsHandler({ onBoundsChange }: MapBoundsHandlerProps) {
  const map = useLeafletMap();
  const lastBoundsRef = useRef<string>("");

  useEffect(() => {
    const handleMoveEnd = () => {
      const bounds = map.getBounds();
      const southWest = bounds.getSouthWest();
      const northEast = bounds.getNorthEast();

      const newBbox: BBox = {
        min_lon: roundCoord(southWest.lng),
        min_lat: roundCoord(southWest.lat),
        max_lon: roundCoord(northEast.lng),
        max_lat: roundCoord(northEast.lat),
      };

      // Stringify for easy comparison of primitive values
      const boundsString = JSON.stringify(newBbox);
      
      if (boundsString !== lastBoundsRef.current) {
        lastBoundsRef.current = boundsString;
        onBoundsChange(newBbox);
      }
    };

    // Initial bounds on mount
    handleMoveEnd();

    map.on("moveend", handleMoveEnd);
    return () => {
      map.off("moveend", handleMoveEnd);
    };
  }, [map, onBoundsChange]);

  return null;
}
