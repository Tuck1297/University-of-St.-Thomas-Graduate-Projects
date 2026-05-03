export interface BaseMarker {
  id: string;
  lat: number;
  lng: number;
  name: string;
}

export interface SensorMarker extends BaseMarker {
  sensorType: "air" | "water" | "soil" | "weather";
  temperature: number;
  humidity: number;
  status: "online" | "offline" | "warning";
  lastReading: string;
}

export interface AccountMarker extends BaseMarker {
  company: string;
  address: string;
  pipelineValue: number;
  status: "active" | "at-risk" | "churned" | "prospect";
  owner: string;
  lastContact: string;
  industry: string;
}

export interface PoiMarker extends BaseMarker {
  category: "restaurant" | "hotel" | "gas" | "park" | "shop";
  rating: number;
  reviewCount: number;
  description: string;
  priceLevel: 1 | 2 | 3 | 4;
}

export type MapMarker = SensorMarker | AccountMarker | PoiMarker;

export interface FilterState {
  search: string;
  categories: string[];
  statuses: string[];
}

export interface PanelState {
  open: boolean;
  mode: "sidebar" | "drawer";
}
