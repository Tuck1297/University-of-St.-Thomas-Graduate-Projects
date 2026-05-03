"use client";

import { useEffect } from "react";
import L from "leaflet";
import { useLeafletMap } from "./ClusteredMap";
import type { BaseMarker } from "../../types/map.types";

interface MapPanHandlerProps {
  marker: BaseMarker | null;
  mode: "sidebar" | "drawer";
  sidebarWidth?: number;
  drawerHeight?: number;
}

export default function MapPanHandler({
  marker,
  mode,
  sidebarWidth = 400,
  drawerHeight = 355,
}: MapPanHandlerProps) {
  const map = useLeafletMap();

  useEffect(() => {
    if (!marker) return;

    const latLng = L.latLng(marker.lat, marker.lng);
    const padding =
      mode === "sidebar"
        ? {
            paddingTopLeft: [sidebarWidth + 20, 20] as L.PointTuple,
            paddingBottomRight: [20, 20] as L.PointTuple,
          }
        : {
            paddingTopLeft: [20, 20] as L.PointTuple,
            paddingBottomRight: [20, drawerHeight + 20] as L.PointTuple,
          };

    map.flyTo(latLng, Math.max(map.getZoom(), 12), {
      animate: true,
      duration: 0.5,
      ...padding,
    });
  }, [marker, mode, sidebarWidth, drawerHeight, map]);

  return null;
}
