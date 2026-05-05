export interface MarkerStyle {
  color: string;
  emoji: string;
  label: string;
}

const DEFAULT_STYLE: MarkerStyle = {
  color: "#9aa0a6",
  emoji: "📍",
  label: "Location",
};

/**
 * Mapping of location_type_key to visual styles.
 * Grouped for maintainability.
 */
export const TYPE_MAPPING: Record<number, MarkerStyle> = {
  // Parks & Outdoors (Green/Brown)
  1: { color: "#34a853", emoji: "🏞️", label: "National Park" },
  2: { color: "#34a853", emoji: "🌳", label: "State Park" },
  3: { color: "#8ab34f", emoji: "🪁", label: "Recreation Area" },
  4: { color: "#2e7d32", emoji: "🌲", label: "National Forest" },
  5: { color: "#2e7d32", emoji: "🌲", label: "State Forest" },
  45: { color: "#34a853", emoji: "🌳", label: "Park" },
  60: { color: "#34a853", emoji: "🌳", label: "State Park" },
  61: { color: "#2e7d32", emoji: "🌲", label: "Woods" },
  88: { color: "#34a853", emoji: "🌳", label: "City Park" },
  113: { color: "#2e7d32", emoji: "🛡️", label: "Nature Preserve" },
  120: { color: "#34a853", emoji: "🌳", label: "Park" },
  121: { color: "#8ab34f", emoji: "🏔️", label: "Natural Feature" },
  142: { color: "#8ab34f", emoji: "🪁", label: "State Recreation Area" },

  // Camping & Lodging (Orange/Blue)
  6: { color: "#fbbc04", emoji: "⛺", label: "Campground" },
  7: { color: "#fbbc04", emoji: "📍", label: "Campsite" },
  54: { color: "#4285f4", emoji: "🏢", label: "Facility" },
  81: { color: "#4285f4", emoji: "🏨", label: "Resort Hotel" },
  85: { color: "#a52a2a", emoji: "🛖", label: "Camping Cabin" },
  97: { color: "#4285f4", emoji: "🛌", label: "Lodging" },
  108: { color: "#fbbc04", emoji: "🚐", label: "RV Park" },
  112: { color: "#fbbc04", emoji: "⛺", label: "Campground" },
  134: { color: "#fbbc04", emoji: "👦", label: "Children's Camp" },
  139: { color: "#fbbc04", emoji: "⛺", label: "Campground" },
  140: { color: "#fbbc04", emoji: "🏇", label: "Horse Campground" },
  143: { color: "#fbbc04", emoji: "⛺", label: "Wall Tent" },
  145: { color: "#fbbc04", emoji: "👥", label: "Group Camp" },
  147: { color: "#fbbc04", emoji: "⛺", label: "Tipi" },

  // Historical & Cultural (Purple)
  9: { color: "#9334e6", emoji: "🏛️", label: "National Historic Site" },
  13: { color: "#9334e6", emoji: "⚔️", label: "National Military Park" },
  14: { color: "#9334e6", emoji: "🏛️", label: "National Historical Park" },
  19: { color: "#9334e6", emoji: "🎗️", label: "Memorial" },
  46: { color: "#9334e6", emoji: "🎗️", label: "National Memorial" },
  62: { color: "#9334e6", emoji: "🏛️", label: "Historical Landmark" },
  71: { color: "#9334e6", emoji: "🖼️", label: "Museum" },
  78: { color: "#9334e6", emoji: "📜", label: "History Museum" },
  79: { color: "#9334e6", emoji: "🗿", label: "Cultural Landmark" },
  86: { color: "#9334e6", emoji: "🗿", label: "Monument" },
  91: { color: "#9334e6", emoji: "📜", label: "Historical Place" },
  127: { color: "#9334e6", emoji: "⛪", label: "Church" },

  // Food & Drink (Red)
  57: { color: "#ea4335", emoji: "🍺", label: "Bar" },
  77: { color: "#ea4335", emoji: "🍔", label: "Fast Food" },
  115: { color: "#ea4335", emoji: "🍕", label: "Pizza" },
  125: { color: "#ea4335", emoji: "🍻", label: "Bar & Grill" },
  126: { color: "#ea4335", emoji: "🍽️", label: "Restaurant" },
  136: { color: "#ea4335", emoji: "🍱", label: "Food Court" },
  137: { color: "#ea4335", emoji: "🍕", label: "Food" },

  // Attractions & Sports (Blue/Yellow)
  55: { color: "#4285f4", emoji: "⚽", label: "Sports Activity" },
  59: { color: "#fbbc04", emoji: "🏎️", label: "Off-Roading Area" },
  69: { color: "#4285f4", emoji: "🏖️", label: "Beach" },
  80: { color: "#f4b400", emoji: "🎡", label: "Tourist Attraction" },
  83: { color: "#f4b400", emoji: "🎢", label: "Amusement Park" },
  92: { color: "#4285f4", emoji: "🧗", label: "Adventure Sports" },
  96: { color: "#4285f4", emoji: "🏊", label: "Swimming Pool" },
  104: { color: "#f4b400", emoji: "🎰", label: "Casino" },
  107: { color: "#4285f4", emoji: "🌊", label: "Water Park" },
  114: { color: "#4285f4", emoji: "⛷️", label: "Ski Resort" },
  116: { color: "#34a853", emoji: "🛝", label: "Playground" },
  119: { color: "#34a853", emoji: "⛳", label: "Miniature Golf" },
  122: { color: "#f4b400", emoji: "🎮", label: "Amusement Center" },
  131: { color: "#4285f4", emoji: "🏟️", label: "Arena" },
  132: { color: "#34a853", emoji: "🦁", label: "Zoo" },

  // Transport & Info (Grey)
  8: { color: "#70757a", emoji: "ℹ️", label: "Visitor Center" },
  24: { color: "#70757a", emoji: "🛣️", label: "Parkway" },
  64: { color: "#70757a", emoji: "🌉", label: "Bridge" },
  66: { color: "#70757a", emoji: "🚻", label: "Rest Stop" },
  67: { color: "#70757a", emoji: "ℹ️", label: "Tourist Info" },
  99: { color: "#70757a", emoji: "ℹ️", label: "Visitor Center" },
  130: { color: "#70757a", emoji: "🅿️", label: "Parking" },
  141: { color: "#70757a", emoji: "⛲", label: "State Wayside" },

  // Water Features (Cyan)
  11: { color: "#00acc1", emoji: "🛶", label: "Wild & Scenic River" },
  22: { color: "#00acc1", emoji: "🌊", label: "National River" },
  94: { color: "#00acc1", emoji: "🌊", label: "River" },
  109: { color: "#00acc1", emoji: "💧", label: "Lake" },
};

/**
 * Returns the style for a given location type key.
 * Falls back to DEFAULT_STYLE if the key is unknown.
 */
export function getStyleForType(typeKey: number): MarkerStyle {
  return TYPE_MAPPING[typeKey] || DEFAULT_STYLE;
}
