import { create } from "zustand";
import type { BaseMarker, FilterState, PanelState } from "../types/map.types";

interface MapStore {
  selectedMarker: BaseMarker | null;
  setSelectedMarker: (marker: BaseMarker | null) => void;

  userLocation: BaseMarker | null;
  setUserLocation: (marker: BaseMarker | null) => void;

  filters: FilterState;
  setFilters: (filters: Partial<FilterState>) => void;
  resetFilters: () => void;

  panel: PanelState;
  setPanel: (panel: Partial<PanelState>) => void;

  mapType: "voyager" | "osm" | "satellite";
  setMapType: (type: "voyager" | "osm" | "satellite") => void;
}

const defaultFilters: FilterState = {
  search: "",
  locationTypes: [],
};

export const useMapStore = create<MapStore>((set) => ({
  selectedMarker: null,
  setSelectedMarker: (marker) =>
    set({ 
      selectedMarker: marker, 
      userLocation: null, // Clear user location if a POI is selected
      panel: { open: !!marker, mode: "sidebar" } 
    }),

  userLocation: null,
  setUserLocation: (marker) =>
    set({ 
      userLocation: marker, 
      selectedMarker: null, // Clear selected POI if user location is set
      panel: { open: !!marker, mode: "sidebar" } 
    }),

  filters: defaultFilters,
  setFilters: (partial) =>
    set((state) => ({ filters: { ...state.filters, ...partial } })),
  resetFilters: () => set({ filters: defaultFilters }),

  panel: { open: false, mode: "sidebar" },
  setPanel: (partial) =>
    set((state) => ({ panel: { ...state.panel, ...partial } })),

  mapType: "voyager",
  setMapType: (type) => set({ mapType: type }),
}));
