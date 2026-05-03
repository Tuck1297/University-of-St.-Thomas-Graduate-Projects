"use client";

import { useEffect, useMemo, useRef } from "react";
import L from "leaflet";
import { renderToString } from "react-dom/server";
import { useClusterGroup } from "./ClusteredMap";

interface CustomMarkerProps {
  position: [number, number];
  popupContent?: React.ReactNode;
  children?: React.ReactNode;
  icon: React.ReactNode;
  iconSize?: [number, number];
  iconAnchor?: [number, number];
  popupAnchor?: [number, number];
  onClick?: () => void;
  /** Extra options spread into the Leaflet marker (e.g. { accountId }) */
  markerOptions?: Record<string, unknown>;
}

export default function CustomMarker({
  position,
  popupContent,
  children,
  icon,
  iconSize = [36, 36],
  iconAnchor,
  popupAnchor = [0, -18],
  onClick,
  markerOptions,
}: CustomMarkerProps) {
  const resolvedPopup = children ?? popupContent;
  const clusterGroup = useClusterGroup();
  const markerRef = useRef<L.Marker | null>(null);

  // Stabilize markerOptions to avoid unnecessary effect re-runs
  const serializedOptions = markerOptions
    ? JSON.stringify(markerOptions)
    : undefined;
  const stableOptions = useMemo(
    () =>
      serializedOptions
        ? (JSON.parse(serializedOptions) as Record<string, unknown>)
        : undefined,
    [serializedOptions],
  );

  useEffect(() => {
    const html = renderToString(icon as React.ReactElement);
    const divIcon = L.divIcon({
      html,
      className: "custom-marker-icon",
      iconSize,
      iconAnchor: iconAnchor ?? [iconSize[0] / 2, iconSize[1] / 2],
      popupAnchor,
    });

    const marker = L.marker(position, {
      icon: divIcon,
      ...(stableOptions as L.MarkerOptions),
    });

    if (onClick) {
      marker.on("click", onClick);
    }

    if (resolvedPopup) {
      const popupHtml = renderToString(resolvedPopup as React.ReactElement);
      marker.bindPopup(popupHtml);
    }

    clusterGroup.addLayer(marker);
    markerRef.current = marker;

    return () => {
      clusterGroup.removeLayer(marker);
      markerRef.current = null;
    };
  }, [
    position,
    icon,
    iconSize,
    iconAnchor,
    popupAnchor,
    onClick,
    resolvedPopup,
    clusterGroup,
    stableOptions,
  ]);

  return null;
}
