"use client";

import { useLeafletMap } from "./MapContext";
import { useMapStore } from "../hooks/useMapStore";
import { Popover, UnstyledButton, Text, Stack, Box, Button } from "@mantine/core";

export function FloatingActions() {
  const map = useLeafletMap();
  const mapType = useMapStore((s) => s.mapType);
  const setMapType = useMapStore((s) => s.setMapType);

  const setUserLocation = useMapStore((s) => s.setUserLocation);

  function handleZoomIn() {
    map.zoomIn();
  }

  function handleZoomOut() {
    map.zoomOut();
  }

  async function handleLocate() {
    map.locate({ setView: true, maxZoom: 18 });
    
    map.once("locationfound", async (e) => {
      const { lat, lng } = e.latlng;
      
      try {
        // Reverse geocode using OpenStreetMap Nominatim
        const response = await fetch(
          `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}&zoom=18&addressdetails=1`,
          {
            headers: {
              'Accept-Language': 'en-US,en;q=0.9',
              'User-Agent': 'Gemini-CLI-GIS-Project'
            }
          }
        );
        const data = await response.json();
        
        setUserLocation({
          id: "user-location",
          lat,
          lng,
          name: data.display_name.split(',')[0] || "Your Location",
          locationTypeKey: 0, // 0 for user/current location
          description: data.display_name || `Lat: ${lat.toFixed(4)}, Lng: ${lng.toFixed(4)}`
        });
      } catch (error) {
        console.error("Reverse geocoding failed:", error);
        setUserLocation({
          id: "user-location",
          lat,
          lng,
          name: "Your Location",
          locationTypeKey: 0,
          description: `Lat: ${lat.toFixed(4)}, Lng: ${lng.toFixed(4)}`
        });
      }
    });
  }

  const mapOptions = [
    { value: "voyager", label: "Voyager", emoji: "🏙️" },
    { value: "osm", label: "OpenStreetMap", emoji: "🗺️" },
    { value: "satellite", label: "Satellite", emoji: "🛰️" },
  ] as const;

  const currentType = mapOptions.find((opt) => opt.value === mapType) || mapOptions[0];

  return (
    <div
      style={{
        position: "absolute",
        bottom: 24,
        right: 16,
        zIndex: 1000,
        display: "flex",
        flexDirection: "column",
        gap: 8,
      }}
    >
      {/* Map Type Popover */}
      <Popover
        position="top"
        offset={12}
        withArrow
        shadow="md"
      >
        <Popover.Target>
          <Button
            aria-label="Select map type"
            style={{
              width: 44,
              height: 44,
              background: "#ffffff",
              borderRadius: 8,
              border: "none",
              boxShadow: "0 2px 6px rgba(0,0,0,0.3)",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              overflow: "hidden",
              padding: 0,
            }}
          >
            <div
              style={{
                width: "100%",
                height: "100%",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: "9px",
                fontWeight: "bold",
                color: "#4285f4",
                textTransform: "uppercase",
                lineHeight: 1,
                flexDirection: "column",
              }}
            >
              <div style={{ fontSize: "18px", marginBottom: "2px" }}>
                {currentType.emoji}
              </div>
              <span style={{ fontSize: "8px" }}>Layers</span>
            </div>
          </Button>
        </Popover.Target>

        <Popover.Dropdown p={8} style={{zIndex: 10000}}>
          <Stack gap={4}>
            {mapOptions.map((option) => (
              <UnstyledButton
                key={option.value}
                onClick={() => {
                  setMapType(option.value);
                }}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 12,
                  padding: "8px 12px",
                  borderRadius: 6,
                  backgroundColor: mapType === option.value ? "rgba(66, 133, 244, 0.1)" : "transparent",
                  transition: "background-color 0.2s ease",
                }}
              >
                <Box style={{ fontSize: 20 }}>{option.emoji}</Box>
                <Text
                  size="sm"
                  fw={500}
                  c={mapType === option.value ? "#4285f4" : "#666"}
                >
                  {option.label}
                </Text>
              </UnstyledButton>
            ))}
          </Stack>
        </Popover.Dropdown>
      </Popover>

      <div
        style={{
          display: "flex",
          flexDirection: "column",
          background: "#ffffff",
          borderRadius: 8,
          boxShadow: "0 2px 6px rgba(0,0,0,0.3)",
          overflow: "hidden",
        }}
      >

        <button
          type="button"
          onClick={handleZoomIn}
          aria-label="Zoom in"
          style={{
            width: 40,
            height: 40,
            border: "none",
            background: "none",
            borderBottom: "1px solid #f0f0f0",
            cursor: "pointer",
            fontSize: 20,
            color: "#666",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          +
        </button>
        <button
          type="button"
          onClick={handleZoomOut}
          aria-label="Zoom out"
          style={{
            width: 40,
            height: 40,
            border: "none",
            background: "none",
            cursor: "pointer",
            fontSize: 24,
            color: "#666",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          −
        </button>
      </div>

      <button
        type="button"
        onClick={handleLocate}
        aria-label="My location"
        style={{
          width: 40,
          height: 40,
          background: "#ffffff",
          borderRadius: "50%",
          border: "none",
          boxShadow: "0 2px 6px rgba(0,0,0,0.3)",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <svg
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke="#666"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z" />
          <circle cx="12" cy="10" r="3" />
        </svg>
      </button>
    </div>
  );
}
