"use client";

import ResponsivePanel from "../core/ResponsivePanel";
import { useResponsiveMode } from "../hooks/useResponsiveMode";
import { useMapStore } from "../hooks/useMapStore";
import type { PoiMarker } from "../types/map.types";

const CATEGORY_COLORS: Record<PoiMarker["category"], string> = {
  restaurant: "#ea4335",
  hotel: "#4285f4",
  gas: "#34a853",
  park: "#8ab34f",
  shop: "#9334e6",
};

const CATEGORY_LABELS: Record<PoiMarker["category"], string> = {
  restaurant: "Restaurant",
  hotel: "Hotel",
  gas: "Gas Station",
  park: "Park",
  shop: "Shop",
};

const CATEGORY_EMOJIS: Record<PoiMarker["category"], string> = {
  restaurant: "🍽",
  hotel: "🏨",
  gas: "⛽",
  park: "🌳",
  shop: "🛍",
};

function StarRating({
  rating,
  reviewCount,
}: {
  rating: number;
  reviewCount: number;
}) {
  const fullStars = Math.floor(rating);
  const hasHalf = rating - fullStars >= 0.5;
  const emptyStars = 5 - fullStars - (hasHalf ? 1 : 0);

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
      <span
        style={{ color: "#f5a623", fontSize: 14, letterSpacing: 1 }}
        aria-hidden="true"
      >
        {"★".repeat(fullStars)}
        {hasHalf ? "½" : ""}
        {"☆".repeat(emptyStars)}
      </span>
      <span
        style={{ fontSize: 13, color: "#5f6368" }}
        aria-label={`${rating} out of 5 stars, ${reviewCount} reviews`}
      >
        {rating.toFixed(1)} ({reviewCount.toLocaleString()})
      </span>
    </div>
  );
}

function PriceLevel({ level }: { level: 1 | 2 | 3 | 4 }) {
  return (
    <span
      style={{ fontSize: 13, color: "#5f6368" }}
      aria-label={`Price level: ${level} out of 4`}
    >
      {"$".repeat(level)}
      <span style={{ color: "#d0d0d0" }}>{"$".repeat(4 - level)}</span>
    </span>
  );
}

function CardContent({ poi }: { poi: PoiMarker }) {
  const color = CATEGORY_COLORS[poi.category];
  const label = CATEGORY_LABELS[poi.category];
  const emoji = CATEGORY_EMOJIS[poi.category];

  return (
    <div>
      {/* Name */}
      <h2
        style={{
          margin: "0 0 8px",
          fontSize: 18,
          fontWeight: 600,
          color: "#202124",
          lineHeight: 1.3,
          paddingRight: 32,
        }}
      >
        {poi.name}
      </h2>

      {/* Category badge + price */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          marginBottom: 8,
        }}
      >
        <span
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 4,
            padding: "3px 10px",
            borderRadius: 12,
            background: color + "18",
            border: `1px solid ${color}40`,
            color: color,
            fontSize: 12,
            fontWeight: 600,
          }}
          aria-label={`Category: ${label}`}
        >
          <span aria-hidden="true">{emoji}</span>
          {label}
        </span>
        <PriceLevel level={poi.priceLevel} />
      </div>

      {/* Rating */}
      <div style={{ marginBottom: 12 }}>
        <StarRating rating={poi.rating} reviewCount={poi.reviewCount} />
      </div>

      {/* Divider */}
      <div style={{ height: 1, background: "#f1f3f4", marginBottom: 12 }} />

      {/* Description */}
      <p
        style={{
          margin: 0,
          fontSize: 14,
          color: "#5f6368",
          lineHeight: 1.5,
        }}
      >
        {poi.description}
      </p>

      {/* Placeholder for more detail */}
      <div
        style={{
          marginTop: 16,
          padding: "12px",
          borderRadius: 8,
          background: "#f8f9fa",
          fontSize: 13,
          color: "#9aa0a6",
          textAlign: "center",
        }}
      >
        More details coming soon
      </div>
    </div>
  );
}

export function LocationCard() {
  const selectedMarker = useMapStore((s) => s.selectedMarker);
  const setSelectedMarker = useMapStore((s) => s.setSelectedMarker);
  const mode = useResponsiveMode();

  const isPoiMarker = (m: typeof selectedMarker): m is PoiMarker =>
    m !== null && "category" in m && "rating" in m;

  const poi = isPoiMarker(selectedMarker) ? selectedMarker : null;
  const isOpen = poi !== null;

  function handleClose() {
    setSelectedMarker(null);
  }

  if (mode === "drawer") {
    return (
      <ResponsivePanel
        open={isOpen}
        onClose={handleClose}
        snapPoints={["148px", "320px", 1]}
        drawerClassName="explore-location-drawer"
        drawerTitle={poi ? `Details for ${poi.name}` : "Location details"}
      >
        {poi && <CardContent poi={poi} />}
      </ResponsivePanel>
    );
  }

  // Desktop: floating card on the left side with slide-in animation
  return (
    <div
      role="complementary"
      aria-label={poi ? `Details for ${poi.name}` : "Location details"}
      style={{
        position: "absolute",
        top: 80,
        left: 16,
        bottom: 16,
        width: 350,
        zIndex: 999,
        background: "#ffffff",
        borderRadius: 12,
        boxShadow: isOpen
          ? "0 2px 8px rgba(0,0,0,0.12), 0 4px 24px rgba(0,0,0,0.1)"
          : "none",
        overflowY: "auto",
        padding: 20,
        transform: isOpen ? "translateX(0)" : "translateX(-110%)",
        opacity: isOpen ? 1 : 0,
        transition:
          "transform 0.3s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease",
        pointerEvents: isOpen ? "auto" : "none",
      }}
    >
      {/* Close button */}
      <button
        type="button"
        onClick={handleClose}
        aria-label="Close location details"
        style={{
          position: "absolute",
          top: 12,
          right: 12,
          width: 28,
          height: 28,
          borderRadius: "50%",
          border: "none",
          background: "#f1f3f4",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: 14,
          color: "#5f6368",
          lineHeight: 1,
          padding: 0,
          transition: "background 0.15s ease",
          zIndex: 1,
        }}
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLButtonElement).style.background = "#e0e0e0";
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLButtonElement).style.background = "#f1f3f4";
        }}
      >
        &times;
      </button>

      {poi && <CardContent poi={poi} />}
    </div>
  );
}
