"use client";

import { useLeafletMap } from "../core/MapContext";

export function FloatingActions() {
  const map = useLeafletMap();

  function handleZoomIn() {
    map.zoomIn();
  }

  function handleZoomOut() {
    map.zoomOut();
  }

  function handleLocate() {
    map.locate({ setView: true, maxZoom: 16 });
  }

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
