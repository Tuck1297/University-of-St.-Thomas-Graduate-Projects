"use client";

import ResponsivePanel from "../core/ResponsivePanel";
import { useResponsiveMode } from "../hooks/useResponsiveMode";
import { useMapStore } from "../hooks/useMapStore";
import { useLocationDetails } from "../../hooks/useApi";
import type { PoiMarker } from "../../types/map.types";
import type { 
  LocationDetails, 
  MnDnrAttributes, 
  GoogleAttributes, 
  NpsAttributes,
  LocationMedia,
  LocationAddress
} from "../../types/location.types";

const CATEGORY_COLORS: Record<string, string> = {
  restaurant: "#ea4335",
  hotel: "#4285f4",
  gas: "#34a853",
  park: "#8ab34f",
  shop: "#9334e6",
  other: "#9aa0a6",
};

const CATEGORY_LABELS: Record<string, string> = {
  restaurant: "Restaurant",
  hotel: "Hotel",
  gas: "Gas Station",
  park: "Park",
  shop: "Shop",
  other: "Location",
};

const CATEGORY_EMOJIS: Record<string, string> = {
  restaurant: "🍽",
  hotel: "🏨",
  gas: "⛽",
  park: "🌳",
  shop: "🛍",
  other: "📍",
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

function PriceLevel({ level }: { level: number }) {
  const cappedLevel = Math.min(Math.max(level, 0), 4);
  return (
    <span
      style={{ fontSize: 13, color: "#5f6368" }}
      aria-label={`Price level: ${cappedLevel} out of 4`}
    >
      {"$".repeat(cappedLevel)}
      <span style={{ color: "#d0d0d0" }}>{"$".repeat(4 - cappedLevel)}</span>
    </span>
  );
}

function MediaGallery({ media }: { media: LocationMedia[] }) {
  const photos = media.filter(m => m.media_type === "Photo" || m.url.match(/\.(jpg|jpeg|png|webp|avif)$/i));
  
  if (photos.length === 0) return null;

  return (
    <div style={{ marginTop: 16 }}>
      <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 8px" }}>Media</h3>
      <div style={{ display: "flex", gap: 8, overflowX: "auto", paddingBottom: 8, scrollbarWidth: "none" }}>
        {photos.map((item, i) => (
          <img 
            key={i} 
            src={item.url} 
            alt={item.altText || item.title || "Location photo"}
            style={{ height: 120, borderRadius: 8, objectFit: "cover", flexShrink: 0 }}
          />
        ))}
      </div>
    </div>
  );
}

function FormatAddress({ address }: { address: LocationAddress }) {
  const lines = [
    address.address_line_1,
    address.address_line_2,
    address.address_line_3,
    [address.city, address.state_abbre, address.postal_code].filter(Boolean).join(", ")
  ].filter(Boolean);

  return (
    <div style={{ fontSize: 13, color: "#5f6368", lineHeight: 1.4 }}>
      {lines.map((l, i) => <div key={i}>{l}</div>)}
    </div>
  );
}

function CardContent({ poi, details, isLoading }: { poi: PoiMarker; details?: LocationDetails | null; isLoading?: boolean }) {
  const category = poi.category || "other";
  const color = CATEGORY_COLORS[category] || CATEGORY_COLORS.other;
  const label = CATEGORY_LABELS[category] || CATEGORY_LABELS.other;
  const emoji = CATEGORY_EMOJIS[category] || CATEGORY_EMOJIS.other;

  // Enriched data from details
  const description = details?.description || poi.description;
  
  // Extract attributes safely
  const attrs = details?.attributes;
  const isGoogle = details?.data_source_name === "Google Places API";
  const isDnr = details?.data_source_name === "MN DNR";
  const isNps = details?.data_source_name === "NPS";

  const googleAttrs = isGoogle ? attrs as GoogleAttributes : null;
  const dnrAttrs = isDnr ? attrs as MnDnrAttributes : null;
  const npsAttrs = isNps ? attrs as NpsAttributes : null;

  const rating = googleAttrs?.rating || poi.rating || 0;
  const reviewCount = googleAttrs?.user_rating_count || poi.reviewCount || 0;
  const priceLevel = googleAttrs?.price_level || poi.priceLevel || 0;

  return (
    <div>
      {/* Name */}
      <h2
        style={{
          margin: "0 0 4px",
          fontSize: 20,
          fontWeight: 600,
          color: "#202124",
          lineHeight: 1.2,
          paddingRight: 32,
        }}
      >
        {details?.name || poi.name}
      </h2>

      {/* Location Type / Data Source */}
      <div style={{ fontSize: 12, color: "#70757a", marginBottom: 12 }}>
        {details?.location_type} • {details?.data_source_name}
      </div>

      {/* Category badge + price */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          marginBottom: 12,
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
        {priceLevel > 0 && <PriceLevel level={priceLevel} />}
      </div>

      {/* Rating */}
      {(rating > 0 || reviewCount > 0) && (
        <div style={{ marginBottom: 12 }}>
          <StarRating rating={rating} reviewCount={reviewCount} />
        </div>
      )}

      {/* Divider */}
      <div style={{ height: 1, background: "#f1f3f4", marginBottom: 16 }} />

      {/* Media Gallery */}
      {details?.media && <MediaGallery media={details.media} />}

      {/* Description */}
      {description && (
        <div style={{ marginTop: 16 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 4px" }}>About</h3>
          <p
            style={{
              margin: 0,
              fontSize: 14,
              color: "#3c4043",
              lineHeight: 1.5,
              whiteSpace: "pre-wrap"
            }}
          >
            {description}
          </p>
        </div>
      )}

      {/* AI Summary */}
      {details?.ai_generative_description && (
        <div style={{ marginTop: 16, padding: 12, background: "#f8f9ff", borderRadius: 8, border: "1px solid #e8eaff" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 4 }}>
             <svg width="16" height="16" viewBox="0 0 24 24" fill="#4285f4"><path d="M12 2L14.5 9H22L16 14L18.5 21L12 17L5.5 21L8 14L2 9H9.5L12 2Z"/></svg>
             <span style={{ fontSize: 12, fontWeight: 600, color: "#4285f4" }}>AI Summary</span>
          </div>
          <p style={{ margin: 0, fontSize: 13, color: "#3c4043", lineHeight: 1.5 }}>
            {details.ai_generative_description}
          </p>
        </div>
      )}

      {/* Contact & Address */}
      <div style={{ marginTop: 20, display: "flex", flexDirection: "column", gap: 12 }}>
        {details?.addresses?.map((addr, i) => (
          <div key={i} style={{ display: "flex", gap: 12 }}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#70757a" strokeWidth="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg>
            <FormatAddress address={addr} />
          </div>
        ))}

        {details?.phone_numbers?.map((ph, i) => (
          <div key={i} style={{ display: "flex", gap: 12, alignItems: "center" }}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#70757a" strokeWidth="2"><path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07 19.5 19.5 0 01-6-6 19.79 19.79 0 01-3.07-8.67A2 2 0 014.11 2h3a2 2 0 012 1.72 12.84 12.84 0 00.7 2.81 2 2 0 01-.45 2.11L8.09 9.91a16 16 0 006 6l2.27-2.27a2 2 0 012.11-.45 12.84 12.84 0 002.81.7A2 2 0 0122 16.92z"/></svg>
            <span style={{ fontSize: 13, color: "#1a73e8" }}>{ph.phone_number}</span>
          </div>
        ))}

        {details?.websites?.map((web, i) => (
          <div key={i} style={{ display: "flex", gap: 12, alignItems: "center" }}>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#70757a" strokeWidth="2"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/></svg>
            <a href={web.url} target="_blank" rel="noreferrer" style={{ fontSize: 13, color: "#1a73e8", textDecoration: "none" }}>{web.url}</a>
          </div>
        ))}
      </div>

      {/* Source Specific Features (DNR) */}
      {dnrAttrs && (
        <div style={{ marginTop: 20 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 8px" }}>Park Features</h3>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
            {dnrAttrs.swimming_beach && <FeatureItem label="Swimming Beach" icon="🏖️" />}
            {dnrAttrs.fishing_pier && <FeatureItem label="Fishing Pier" icon="🎣" />}
            {dnrAttrs.boat_rental && <FeatureItem label="Boat Rental" icon="🚣" />}
            {dnrAttrs.hiking_trails && <FeatureItem label="Hiking Trails" icon="🥾" />}
            {dnrAttrs.camper_cabins && <FeatureItem label="Camper Cabins" icon="🏠" />}
            {dnrAttrs.nature_programs && <FeatureItem label="Nature Programs" icon="🎓" />}
          </div>
        </div>
      )}

      {/* NPS Activities */}
      {npsAttrs?.activities && (
        <div style={{ marginTop: 20 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 8px" }}>Activities</h3>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {npsAttrs.activities.map((act, i) => (
              <span key={i} style={{ fontSize: 12, padding: "4px 8px", background: "#f1f3f4", borderRadius: 4, color: "#3c4043" }}>{act}</span>
            ))}
          </div>
        </div>
      )}

      {isLoading && (
        <div style={{ marginTop: 16, textAlign: "center", color: "#70757a", fontSize: 13 }}>
          Updating with latest details...
        </div>
      )}
    </div>
  );
}

function FeatureItem({ label, icon }: { label: string; icon: string }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, color: "#3c4043" }}>
      <span>{icon}</span>
      <span>{label}</span>
    </div>
  );
}

export function LocationCard() {
  const selectedMarker = useMapStore((s) => s.selectedMarker);
  const setSelectedMarker = useMapStore((s) => s.setSelectedMarker);
  const mode = useResponsiveMode();

  const poi = selectedMarker as PoiMarker | null;
  const isOpen = poi !== null;

  const { data: details, isLoading } = useLocationDetails(
    poi ? Number(poi.id) : null
  );

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
        drawerTitle={details?.name || poi?.name || "Location details"}
      >
        {poi && <CardContent poi={poi} details={details} isLoading={isLoading} />}
      </ResponsivePanel>
    );
  }

  return (
    <div
      role="complementary"
      aria-label={details?.name || poi?.name || "Location details"}
      style={{
        position: "absolute",
        top: 80,
        left: 16,
        bottom: 16,
        width: 366,
        zIndex: 999,
        background: "#ffffff",
        borderRadius: 12,
        boxShadow: isOpen
          ? "0 2px 8px rgba(0,0,0,0.12), 0 4px 24px rgba(0,0,0,0.1)"
          : "none",
        overflowY: "auto",
        padding: 24,
        transform: isOpen ? "translateX(0)" : "translateX(-115%)",
        opacity: isOpen ? 1 : 0,
        transition: "transform 0.3s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease",
        pointerEvents: isOpen ? "auto" : "none",
      }}
    >
      <button
        type="button"
        onClick={handleClose}
        aria-label="Close"
        style={{
          position: "absolute",
          top: 16,
          right: 16,
          width: 32,
          height: 32,
          borderRadius: "50%",
          border: "none",
          background: "#f1f3f4",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: 20,
          color: "#5f6368",
          padding: 0,
          zIndex: 1,
        }}
      >
        &times;
      </button>

      {poi && <CardContent poi={poi} details={details} isLoading={isLoading} />}
    </div>
  );
}
