"use client";

import {
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { LeafletMapContext, ClusterGroupContext, type MarkerClusterGroup } from "./MapContext";

// Fix Leaflet default marker icons
delete (L.Icon.Default.prototype as unknown as Record<string, unknown>)
  ._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl:
    "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
});

interface MarkerClusterGroupOptions {
  chunkedLoading?: boolean;
  spiderfyOnMaxZoom?: boolean;
  showCoverageOnHover?: boolean;
  maxClusterRadius?: number;
  iconCreateFunction?: (cluster: unknown) => L.Icon | L.DivIcon;
  [key: string]: unknown;
}

const LWithCluster = L as unknown as typeof L & {
  markerClusterGroup: (
    options?: MarkerClusterGroupOptions,
  ) => MarkerClusterGroup;
};

let _clusterReady: Promise<void> | null = null;
function ensureMarkerCluster(): Promise<void> {
  if (_clusterReady) return _clusterReady;
  if (typeof LWithCluster.markerClusterGroup === "function") {
    _clusterReady = Promise.resolve();
    return _clusterReady;
  }
  _clusterReady = new Promise<void>((resolve, reject) => {
    // Load CSS
    const linkBase = document.createElement("link");
    linkBase.rel = "stylesheet";
    linkBase.href = "https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css";
    document.head.appendChild(linkBase);

    const linkDefault = document.createElement("link");
    linkDefault.rel = "stylesheet";
    linkDefault.href = "https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css";
    document.head.appendChild(linkDefault);

    // Load Script
    const script = document.createElement("script");
    script.src =
      "https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js";
    script.onload = () => resolve();
    script.onerror = () =>
      reject(new Error("Failed to load leaflet.markercluster"));
    document.head.appendChild(script);
  });
  return _clusterReady;
}

export interface ClusteredMapProps {
  center?: [number, number];
  zoom?: number;
  minZoom?: number;
  maxZoom?: number;
  children?: ReactNode;
  clusterOptions?: MarkerClusterGroupOptions;
  tileUrl?: string;
  tileAttribution?: string;
  className?: string;
  style?: React.CSSProperties;
  iconCreateFunction?: (cluster: unknown) => L.Icon | L.DivIcon;
}

export default function ClusteredMap({
  center = [44.9778, -93.265],
  zoom = 6,
  minZoom = 2,
  maxZoom = 21,
  children,
  clusterOptions = {},
  tileUrl = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
  tileAttribution = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  className,
  style,
  iconCreateFunction,
}: ClusteredMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const tileLayerRef = useRef<L.TileLayer | null>(null);
  const [map, setMap] = useState<L.Map | null>(null);
  const [clusterGroup, setClusterGroup] = useState<MarkerClusterGroup | null>(null);

  // Initialize Map
  useEffect(() => {
    const el = containerRef.current;
    if (!el || mapRef.current) return;

    let cancelled = false;

    void ensureMarkerCluster().then(() => {
      if (cancelled || mapRef.current) return;

      const m = L.map(el, {
        center,
        zoom,
        minZoom,
        maxZoom,
        scrollWheelZoom: true,
        zoomControl: false,
      });

      mapRef.current = m;
      setMap(m);

      const cg = LWithCluster.markerClusterGroup({
        chunkedLoading: true,
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        maxClusterRadius: 50,
        ...clusterOptions,
        ...(iconCreateFunction ? { iconCreateFunction } : {}),
      });
      m.addLayer(cg);
      setClusterGroup(cg);

      setTimeout(() => m.invalidateSize(), 100);
    });

    return () => {
      cancelled = true;
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Handle Tile Layer Updates
  useEffect(() => {
    if (!map) return;

    if (tileLayerRef.current) {
      tileLayerRef.current.remove();
    }

    tileLayerRef.current = L.tileLayer(tileUrl, {
      attribution: tileAttribution,
      maxZoom,
    }).addTo(map);

    return () => {
      if (tileLayerRef.current) {
        tileLayerRef.current.remove();
        tileLayerRef.current = null;
      }
    };
  }, [map, tileUrl, tileAttribution, maxZoom]);

  return (
    <>
      <div
        ref={containerRef}
        className={className}
        style={{ width: "100%", height: "100%", ...style }}
      />
      {map && clusterGroup && (
        <LeafletMapContext.Provider value={map}>
          <ClusterGroupContext.Provider value={clusterGroup}>
            {children}
          </ClusterGroupContext.Provider>
        </LeafletMapContext.Provider>
      )}
    </>
  );
}
