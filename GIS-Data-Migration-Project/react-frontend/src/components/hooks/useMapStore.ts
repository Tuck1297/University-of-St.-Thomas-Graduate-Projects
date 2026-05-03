import { create } from "zustand";
import type { BaseMarker, FilterState, PanelState } from "../types/map.types";

interface MapStore {
  selectedMarker: BaseMarker | null;
  setSelectedMarker: (marker: BaseMarker | null) => void;

  filters: FilterState;
  setFilters: (filters: Partial<FilterState>) => void;
  resetFilters: () => void;

  panel: PanelState;
  setPanel: (panel: Partial<PanelState>) => void;
}

const defaultFilters: FilterState = {
  search: "",
  categories: [],
  statuses: [],
};

export const useMapStore = create<MapStore>((set) => ({
  selectedMarker: null,
  setSelectedMarker: (marker) =>
    set({ selectedMarker: marker, panel: { open: !!marker, mode: "sidebar" } }),

  filters: defaultFilters,
  setFilters: (partial) =>
    set((state) => ({ filters: { ...state.filters, ...partial } })),
  resetFilters: () => set({ filters: defaultFilters }),

  panel: { open: false, mode: "sidebar" },
  setPanel: (partial) =>
    set((state) => ({ panel: { ...state.panel, ...partial } })),
}));
