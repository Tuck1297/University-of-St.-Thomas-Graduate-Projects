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

const CLUSTER_CSS = `
.marker-cluster-small,.marker-cluster-medium,.marker-cluster-large{background-clip:padding-box;border-radius:20px}
.marker-cluster-small{background-color:rgba(181,226,140,.6)}
.marker-cluster-small div{background-color:rgba(110,204,57,.6)}
.marker-cluster-medium{background-color:rgba(241,211,87,.6)}
.marker-cluster-medium div{background-color:rgba(240,194,12,.6)}
.marker-cluster-large{background-color:rgba(253,156,115,.6)}
.marker-cluster-large div{background-color:rgba(241,128,23,.6)}
.marker-cluster{background-clip:padding-box;border-radius:20px}
.marker-cluster div{width:30px;height:30px;margin-left:5px;margin-top:5px;text-align:center;border-radius:15px;font:12px "Helvetica Neue",Arial,Helvetica,sans-serif}
.marker-cluster span{line-height:30px}
.leaflet-cluster-anim .leaflet-marker-icon,.leaflet-cluster-anim .leaflet-marker-shadow{transition:transform .3s ease-out,opacity .3s ease-in}
.leaflet-cluster-spider-leg{transition:stroke-dashoffset .3s ease-out,stroke-opacity .3s ease-in}
`;

export interface ClusteredMapProps {
  center?: [number, number];
  zoom?: number;
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
  children,
  clusterOptions = {},
  tileUrl = "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
  tileAttribution = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/">CARTO</a>',
  className,
  style,
  iconCreateFunction,
}: ClusteredMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const [map, setMap] = useState<L.Map | null>(null);
  const [clusterGroup, setClusterGroup] = useState<MarkerClusterGroup | null>(null);

  useEffect(() => {
    const id = "leaflet-markercluster-css";
    if (!document.getElementById(id)) {
      const styleEl = document.createElement("style");
      styleEl.id = id;
      styleEl.textContent = CLUSTER_CSS;
      document.head.appendChild(styleEl);
    }
  }, []);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || mapRef.current) return;

    let cancelled = false;

    void ensureMarkerCluster().then(() => {
      if (cancelled || mapRef.current) return;

      const m = L.map(el, {
        center,
        zoom,
        scrollWheelZoom: true,
        zoomControl: false,
      });

      L.tileLayer(tileUrl, { attribution: tileAttribution }).addTo(m);

      const cg = LWithCluster.markerClusterGroup({
        chunkedLoading: true,
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        maxClusterRadius: 50,
        ...clusterOptions,
        ...(iconCreateFunction ? { iconCreateFunction } : {}),
      });
      m.addLayer(cg);

      mapRef.current = m;
      setMap(m);
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
