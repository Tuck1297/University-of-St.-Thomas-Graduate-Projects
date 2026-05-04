"use client";

import { useMemo, useState } from "react";
import MapWrapper from "../core/MapWrapper";
import CustomMarker from "../core/CustomMarker";
import MapPanHandler from "../core/MapPanHandler";
import MapBoundsHandler from "../core/MapBoundsHandler";
import { useResponsiveMode } from "../hooks/useResponsiveMode";
import { useMapStore } from "../hooks/useMapStore";
import { LocationCard } from "./LocationCard";
import { FloatingSearch } from "./FloatingSearch";
import { FloatingActions } from "./FloatingActions";
import { useLocations, useSearch, type BBox, type LocationFeature } from "../../hooks/useApi";
import { useDebouncedValue } from "@mantine/hooks";
import type { PoiMarker } from "../../types/map.types";
import { getStyleForType } from "../../types/locationTypeMapping";
import type { MarkerClusterLike } from "../core/MapContext";
import L from "leaflet";

// Minneapolis center
const MINNEAPOLIS_CENTER: [number, number] = [44.96, -93.27];
const DEFAULT_ZOOM = 12;

// Teardrop / pin shape: rounded top, pointed bottom
function PinIcon({ color, emoji }: { color: string; emoji: string }) {
  return (
    <div
      style={{
        width: 32,
        height: 40,
        position: "relative",
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "center",
      }}
      aria-hidden="true"
    >
      <div
        style={{
          width: 32,
          height: 32,
          borderRadius: "50% 50% 50% 50% / 60% 60% 40% 40%",
          background: color,
          boxShadow: "0 2px 6px rgba(0,0,0,0.3)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          position: "relative",
          zIndex: 1,
        }}
      >
        <span style={{ fontSize: 14, lineHeight: 1 }}>{emoji}</span>
      </div>
      <div
        style={{
          position: "absolute",
          bottom: 0,
          left: "50%",
          transform: "translateX(-50%)",
          width: 0,
          height: 0,
          borderLeft: "7px solid transparent",
          borderRight: "7px solid transparent",
          borderTop: `10px solid ${color}`,
          zIndex: 0,
        }}
      />
    </div>
  );
}

// Cluster icon factory
function createClusterIcon(cluster: unknown) {
  const count = (cluster as MarkerClusterLike).getChildCount();
  return L.divIcon({
    html: `
      <div style="
        width: 36px;
        height: 36px;
        border-radius: 50%;
        background: #ffffff;
        border: 2px solid #4285f4;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 13px;
        font-weight: 700;
        color: #4285f4;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      ">${count}</div>
    `,
    className: "explore-cluster-icon",
    iconSize: [36, 36],
    iconAnchor: [18, 18],
  });
}

function mapFeatureToPoi(feature: LocationFeature): PoiMarker {
  const { properties } = feature;

  return {
    id: String(properties.location_key),
    lat: properties.latitude,
    lng: properties.longitude,
    name: properties.name,
    locationTypeKey: properties.location_type_key,
    description: properties.description || "",
  };
}

export function ExploreLayout() {
  const selectedMarker = useMapStore((s) => s.selectedMarker);
  const setSelectedMarker = useMapStore((s) => s.setSelectedMarker);
  const filters = useMapStore((s) => s.filters);
  const mode = useResponsiveMode();

  const [bounds, setBounds] = useState<BBox | null>(null);
  const [debouncedBounds] = useDebouncedValue(bounds, 2000);

  // Queries
  const { data: bboxData, isLoading: isBboxLoading } = useLocations(debouncedBounds);
  const { data: searchData, isLoading: isSearchLoading } = useSearch(filters.search);

  // Apply filters and merge results
  const filteredPois = useMemo(() => {
    const isSearching = filters.search.trim() !== "";
    const rawFeatures = isSearching 
      ? searchData?.features || [] 
      : bboxData?.features || [];

    return rawFeatures.map(mapFeatureToPoi).filter((poi) => {
      const matchesType =
        filters.locationTypes.length === 0 ||
        filters.locationTypes.includes(poi.locationTypeKey);

      return matchesType;
    });
  }, [filters.search, filters.locationTypes, bboxData, searchData]);

  const clusterOptions = useMemo(
    () => ({
      iconCreateFunction: createClusterIcon,
    }),
    [],
  );

  return (
    <div
      style={{
        position: "relative",
        width: "100%",
        height: "100dvh",
        overflow: "hidden",
      }}
    >
      {/* Full-viewport map */}
      <div style={{ position: "absolute", inset: 0 }}>
        <MapWrapper
          center={MINNEAPOLIS_CENTER}
          zoom={DEFAULT_ZOOM}
          clusterOptions={clusterOptions}
          style={{ width: "100%", height: "100%" }}
        >
          {/* FloatingActions must be INSIDE MapContainer to use useMap() */}
          <FloatingActions />

          {/* MapPanHandler pans/flies to selected marker */}
          <MapPanHandler
            marker={selectedMarker}
            mode={mode}
            sidebarWidth={366}
            drawerHeight={320}
          />

          {/* Track map bounds */}
          <MapBoundsHandler onBoundsChange={setBounds} />

          {/* POI markers */}
          {filteredPois.map((poi) => {
            const style = getStyleForType(poi.locationTypeKey);
            return (
              <CustomMarker
                key={poi.id}
                position={[poi.lat, poi.lng]}
                icon={
                  <PinIcon
                    color={style.color}
                    emoji={style.emoji}
                  />
                }
                iconSize={[32, 40]}
                iconAnchor={[16, 40]}
                popupAnchor={[0, -44]}
                onClick={() => setSelectedMarker(poi)}
              />
            );
          })}
        </MapWrapper>
      </div>

      {/* Loading indicator */}
      {(isBboxLoading || isSearchLoading) && (
        <div style={{
          position: "absolute",
          top: 70,
          left: "50%",
          transform: "translateX(-50%)",
          zIndex: 1001,
          background: "rgba(255,255,255,0.8)",
          padding: "4px 12px",
          borderRadius: 16,
          fontSize: 12,
          boxShadow: "0 2px 4px rgba(0,0,0,0.1)"
        }}>
          Loading results...
        </div>
      )}

      {/* Floating search bar — overlaid on the map */}
      <FloatingSearch />

      {/* Location detail card — overlaid on the map */}
      <LocationCard />
    </div>
  );
}
