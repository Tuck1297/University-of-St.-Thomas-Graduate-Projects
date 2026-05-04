export interface BaseMarker {
  id: string;
  lat: number;
  lng: number;
  name: string;
  locationTypeKey: number;
  description: string;
}

export interface PoiMarker extends BaseMarker {
  // These are now optional as they might not exist in the base list results
  category?: string;
  rating?: number;
  reviewCount?: number;
  priceLevel?: number;
}

export type MapMarker = PoiMarker;

export interface FilterState {
  search: string;
  locationTypes: number[]; // Rewired to use location_type_key
}

export interface PanelState {
  open: boolean;
  mode: "sidebar" | "drawer";
}
