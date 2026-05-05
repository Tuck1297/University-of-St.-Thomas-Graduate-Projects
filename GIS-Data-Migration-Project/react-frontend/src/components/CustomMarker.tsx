"use client";

import { useEffect, useRef } from "react";
import L from "leaflet";
import { renderToString } from "react-dom/server";
import { useClusterGroup } from "./MapContext";

export interface CustomMarkerProps {
  position: [number, number];
  icon: React.ReactNode;
  iconSize?: [number, number];
  iconAnchor?: [number, number];
  popupAnchor?: [number, number];
  tooltip?: string;
  onClick?: () => void;
  children?: React.ReactNode;
}

export default function CustomMarker({
  position,
  icon,
  iconSize = [32, 32],
  iconAnchor = [16, 16],
  popupAnchor = [0, -16],
  tooltip,
  onClick,
  children,
}: CustomMarkerProps) {
  const clusterGroup = useClusterGroup();
  const markerRef = useRef<L.Marker | null>(null);

  useEffect(() => {
    const customIcon = L.divIcon({
      html: renderToString(<>{icon}</>),
      className: "custom-marker-icon",
      iconSize,
      iconAnchor,
      popupAnchor,
    });

    const marker = L.marker(position, { icon: customIcon });

    if (onClick) {
      marker.on("click", (e) => {
        L.DomEvent.stopPropagation(e);
        onClick();
      });
    }

    if (tooltip) {
      marker.bindTooltip(tooltip, {
        direction: "top",
        offset: [0, -iconSize[1] + iconAnchor[1] - 30],
        opacity: 0.8,
      });
    }

    if (children) {
      marker.bindPopup(renderToString(<>{children}</>));
    }

    clusterGroup.addLayer(marker);
    markerRef.current = marker;

    return () => {
      if (markerRef.current) {
        clusterGroup.removeLayer(markerRef.current);
      }
    };
  }, [position, icon, iconSize, iconAnchor, popupAnchor, tooltip, onClick, children, clusterGroup]);

  return null;
}
