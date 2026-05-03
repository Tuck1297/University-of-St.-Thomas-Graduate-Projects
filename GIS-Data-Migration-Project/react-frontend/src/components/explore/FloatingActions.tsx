"use client";

import { useLeafletMap } from "../core/ClusteredMap";

function ZoomInIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="#5f6368"
      strokeWidth="2.5"
      strokeLinecap="round"
      aria-hidden="true"
    >
      <line x1="12" y1="5" x2="12" y2="19" />
      <line x1="5" y1="12" x2="19" y2="12" />
    </svg>
  );
}

function ZoomOutIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="#5f6368"
      strokeWidth="2.5"
      strokeLinecap="round"
      aria-hidden="true"
    >
      <line x1="5" y1="12" x2="19" y2="12" />
    </svg>
  );
}

function LocationIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="#4285f4"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="12" cy="12" r="3" />
      <line x1="12" y1="2" x2="12" y2="6" />
      <line x1="12" y1="18" x2="12" y2="22" />
      <line x1="2" y1="12" x2="6" y2="12" />
      <line x1="18" y1="12" x2="22" y2="12" />
    </svg>
  );
}

const fabButtonStyle: React.CSSProperties = {
  width: 40,
  height: 40,
  borderRadius: "50%",
  border: "none",
  background: "#ffffff",
  boxShadow: "0 2px 8px rgba(0,0,0,0.18), 0 1px 3px rgba(0,0,0,0.1)",
  cursor: "pointer",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  transition: "box-shadow 0.15s ease, background 0.15s ease",
  padding: 0,
};

export function FloatingActions() {
  const map = useLeafletMap();

  function handleZoomIn() {
    map.zoomIn();
  }

  function handleZoomOut() {
    map.zoomOut();
  }

  function handleMyLocation() {
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        map.flyTo([pos.coords.latitude, pos.coords.longitude], 14, {
          animate: true,
          duration: 1,
        });
      },
      () => {
        // Silently fail if user denies or blocks geolocation
      },
    );
  }

  return (
    <div
      style={{
        position: "absolute",
        bottom: 32,
        right: 16,
        zIndex: 1000,
        display: "flex",
        flexDirection: "column",
        gap: 8,
      }}
      role="group"
      aria-label="Map controls"
    >
      <button
        type="button"
        style={fabButtonStyle}
        onClick={handleZoomIn}
        aria-label="Zoom in"
        title="Zoom in"
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLButtonElement).style.boxShadow =
            "0 4px 12px rgba(0,0,0,0.22), 0 2px 6px rgba(0,0,0,0.12)";
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLButtonElement).style.boxShadow =
            "0 2px 8px rgba(0,0,0,0.18), 0 1px 3px rgba(0,0,0,0.1)";
        }}
      >
        <ZoomInIcon />
      </button>

      <button
        type="button"
        style={fabButtonStyle}
        onClick={handleZoomOut}
        aria-label="Zoom out"
        title="Zoom out"
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLButtonElement).style.boxShadow =
            "0 4px 12px rgba(0,0,0,0.22), 0 2px 6px rgba(0,0,0,0.12)";
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLButtonElement).style.boxShadow =
            "0 2px 8px rgba(0,0,0,0.18), 0 1px 3px rgba(0,0,0,0.1)";
        }}
      >
        <ZoomOutIcon />
      </button>

      <button
        type="button"
        style={fabButtonStyle}
        onClick={handleMyLocation}
        aria-label="Go to my location"
        title="My location"
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLButtonElement).style.boxShadow =
            "0 4px 12px rgba(0,0,0,0.22), 0 2px 6px rgba(0,0,0,0.12)";
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLButtonElement).style.boxShadow =
            "0 2px 8px rgba(0,0,0,0.18), 0 1px 3px rgba(0,0,0,0.1)";
        }}
      >
        <LocationIcon />
      </button>
    </div>
  );
}
