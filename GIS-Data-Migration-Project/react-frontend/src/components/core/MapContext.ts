import { createContext, useContext } from "react";
import L from "leaflet";

export interface MarkerClusterGroup extends L.FeatureGroup {
  addLayer(layer: L.Layer): this;
  removeLayer(layer: L.Layer): this;
}

export const LeafletMapContext = createContext<L.Map | null>(null);
export const ClusterGroupContext = createContext<MarkerClusterGroup | null>(null);

export function useLeafletMap(): L.Map {
  const map = useContext(LeafletMapContext);
  if (!map) throw new Error("useLeafletMap must be used inside ClusteredMap");
  return map;
}

export function useClusterGroup(): MarkerClusterGroup {
  const cg = useContext(ClusterGroupContext);
  if (!cg) throw new Error("useClusterGroup must be used inside ClusteredMap");
  return cg;
}

export interface MarkerClusterLike {
  getChildCount(): number;
}
