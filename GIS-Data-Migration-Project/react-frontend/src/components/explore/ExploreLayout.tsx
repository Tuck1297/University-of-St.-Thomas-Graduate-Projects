"use client";

import { useMemo } from "react";
import MapWrapper from "../core/MapWrapper";
import CustomMarker from "../core/CustomMarker";
import MapPanHandler from "../core/MapPanHandler";
import { useResponsiveMode } from "../hooks/useResponsiveMode";
import { useMapStore } from "../hooks/useMapStore";
import { poiData } from "../data/pois";
import { LocationCard } from "./LocationCard";
import { FloatingSearch } from "./FloatingSearch";
import { FloatingActions } from "./FloatingActions";
import type { PoiMarker } from "../types/map.types";
import L from "leaflet";

// Minneapolis center
const MINNEAPOLIS_CENTER: [number, number] = [44.96, -93.27];
const DEFAULT_ZOOM = 12;

const CATEGORY_COLORS: Record<PoiMarker["category"], string> = {
  restaurant: "#ea4335",
  hotel: "#4285f4",
  gas: "#34a853",
  park: "#8ab34f",
  shop: "#9334e6",
};

const CATEGORY_EMOJIS: Record<PoiMarker["category"], string> = {
  restaurant: "🍽",
  hotel: "🏨",
  gas: "⛽",
  park: "🌳",
  shop: "🛍",
};

// Teardrop / pin shape: rounded top, pointed bottom
// Built as a self-contained HTML string for renderToString inside CustomMarker
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
      {/* Pin body — circle */}
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
      {/* Pin tail — triangle pointing down */}
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

// Minimal interface matching the subset of L.MarkerCluster that we use,
// since L.MarkerCluster is not exported by @types/leaflet (it comes from
// leaflet.markercluster which augments the L namespace at runtime only).
interface MarkerClusterLike {
  getChildCount(): number;
}

// Cluster icon factory for react-leaflet-cluster
function createClusterIcon(cluster: MarkerClusterLike) {
  const count = cluster.getChildCount();
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

export function ExploreLayout() {
  const selectedMarker = useMapStore((s) => s.selectedMarker);
  const setSelectedMarker = useMapStore((s) => s.setSelectedMarker);
  const filters = useMapStore((s) => s.filters);
  const mode = useResponsiveMode();

  // Apply filters: search by name, filter by category
  const filteredPois = useMemo(() => {
    return poiData.filter((poi) => {
      const matchesSearch =
        filters.search.trim() === "" ||
        poi.name.toLowerCase().includes(filters.search.toLowerCase());

      const matchesCategory =
        filters.categories.length === 0 ||
        filters.categories.includes(poi.category);

      return matchesSearch && matchesCategory;
    });
  }, [filters.search, filters.categories]);

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

          {/* POI markers */}
          {filteredPois.map((poi) => (
            <CustomMarker
              key={poi.id}
              position={[poi.lat, poi.lng]}
              icon={
                <PinIcon
                  color={CATEGORY_COLORS[poi.category]}
                  emoji={CATEGORY_EMOJIS[poi.category]}
                />
              }
              iconSize={[32, 40]}
              iconAnchor={[16, 40]}
              popupAnchor={[0, -44]}
              onClick={() => setSelectedMarker(poi)}
            />
          ))}
        </MapWrapper>
      </div>

      {/* Floating search bar — overlaid on the map */}
      <FloatingSearch />

      {/* Location detail card — overlaid on the map */}
      <LocationCard />
    </div>
  );
}
