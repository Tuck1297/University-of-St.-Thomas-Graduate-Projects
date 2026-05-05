"use client";

import { useMemo, useState } from "react";
import MapWrapper from "./MapWrapper";
import CustomMarker from "./CustomMarker";
import MapPanHandler from "./MapPanHandler";
import MapBoundsHandler from "./MapBoundsHandler";
import MapSearchHandler from "./MapSearchHandler";
import { useResponsiveMode } from "../hooks/useResponsiveMode";
import { useMapStore } from "../hooks/useMapStore";
import { LocationCard } from "./LocationCard";
import { FloatingSearch } from "./FloatingSearch";
import { FloatingActions } from "./FloatingActions";
import { usePersistentLocations, useSearch, type BBox, type LocationFeature } from "../hooks/useApi";
import { useDebouncedValue } from "@mantine/hooks";
import type { PoiMarker } from "../types/map.types";
import { getStyleForType } from "../types/locationTypeMapping";

// Minneapolis center
const MINNEAPOLIS_CENTER: [number, number] = [44.96, -93.27];
const DEFAULT_ZOOM = 12;
const MAX_ZOOM = 21;

const VOYAGER_TILE_URL = "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png";
const VOYAGER_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/">CARTO</a>';

const OSM_TILE_URL = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
const OSM_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';

const SATELLITE_TILE_URL = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}";
const SATELLITE_ATTRIBUTION = 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community';

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
  const userLocation = useMapStore((s) => s.userLocation);
  const setUserLocation = useMapStore((s) => s.setUserLocation);
  const filters = useMapStore((s) => s.filters);
  const mapType = useMapStore((s) => s.mapType);
  const mode = useResponsiveMode();

  const activeMarker = selectedMarker || userLocation;
const [bounds, setBounds] = useState<BBox | null>(null);
const [debouncedBounds] = useDebouncedValue(bounds, 2000);

// Queries
const { data: bboxData, isLoading: isBboxLoading } = usePersistentLocations(debouncedBounds);
const { data: searchData, isLoading: isSearchLoading } = useSearch(filters.search);

// Apply filters and merge results
const isSearching = filters.search.trim() !== "";

const filteredPois = useMemo(() => {
  const rawFeatures = isSearching 
    ? searchData?.features || [] 
    : bboxData?.features || [];

  return rawFeatures.map(mapFeatureToPoi).filter((poi: PoiMarker) => {
    const matchesType =
      filters.locationTypes.length === 0 ||
      filters.locationTypes.includes(poi.locationTypeKey);

    return matchesType;
  });
}, [isSearching, filters.locationTypes, bboxData, searchData]);



  const clusterOptions = useMemo(
    () => ({
      // Use default Leaflet.markercluster styles
    }),
    [],
  );

  const tileProps = useMemo(() => {
    switch (mapType) {
      case "satellite":
        return {
          tileUrl: SATELLITE_TILE_URL,
          tileAttribution: SATELLITE_ATTRIBUTION,
        };
      case "osm":
        return {
          tileUrl: OSM_TILE_URL,
          tileAttribution: OSM_ATTRIBUTION,
        };
      case "voyager":
      default:
        return {
          tileUrl: VOYAGER_TILE_URL,
          tileAttribution: VOYAGER_ATTRIBUTION,
        };
    }
  }, [mapType]);

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
          maxZoom={MAX_ZOOM}
          clusterOptions={clusterOptions}
          style={{ width: "100%", height: "100%" }}
          {...tileProps}
        >
          {/* FloatingActions must be INSIDE MapContainer to use useMap() */}
          <FloatingActions />

          {/* MapPanHandler pans/flies to selected marker */}
          <MapPanHandler
            marker={activeMarker}
            mode={mode}
            sidebarWidth={366}
            drawerHeight={320}
          />

          {/* Track map bounds */}
          <MapBoundsHandler onBoundsChange={setBounds} />

          {/* Fit map to search results */}
          <MapSearchHandler 
            results={filteredPois} 
            isSearching={isSearching} 
          />

          {/* User location marker */}
          {userLocation && (
            <CustomMarker
              position={[userLocation.lat, userLocation.lng]}
              tooltip={userLocation.name}
              icon={
                <PinIcon
                  color="#4285f4"
                  emoji="👤"
                />
              }
              iconSize={[32, 40]}
              iconAnchor={[16, 40]}
              popupAnchor={[0, -44]}
              onClick={() => setUserLocation(userLocation)}
            />
          )}

          {/* POI markers */}
          {filteredPois.map((poi: PoiMarker) => {
            const style = getStyleForType(poi.locationTypeKey);
            return (
              <CustomMarker
                key={poi.id}
                position={[poi.lat, poi.lng]}
                tooltip={poi.name}
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
