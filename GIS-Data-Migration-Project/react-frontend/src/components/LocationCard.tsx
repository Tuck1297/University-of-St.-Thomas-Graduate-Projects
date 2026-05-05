"use client";

import ResponsivePanel from "./ResponsivePanel";
import { useResponsiveMode } from "../hooks/useResponsiveMode";
import { useMapStore } from "../hooks/useMapStore";
import { useLocationDetails } from "../hooks/useApi";
import type { PoiMarker } from "../types/map.types";
import type {
  LocationDetails,
  MnDnrAttributes,
  GoogleAttributes,
  NpsAttributes,
  LocationMedia,
  LocationAddress,
  RidbAttributes,
} from "../types/location.types";
import { getStyleForType } from "../types/locationTypeMapping";
import { Carousel } from "@mantine/carousel";
import { 
  HiOutlineMapPin, 
  HiOutlinePhone, 
  HiOutlineEnvelope, 
  HiOutlineGlobeAlt,
  HiOutlineXMark
} from "react-icons/hi2";
import { MdOutlineAutoAwesome } from "react-icons/md";

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
  const photos = media.filter(
    (m) =>
      m.media_type === "Photo" || m.url.match(/\.(jpg|jpeg|png|webp|avif)$/i),
  );

  if (photos.length === 0) return null;

  return (
    <div style={{ marginTop: 16 }}>
      <h3
        style={{
          fontSize: 14,
          fontWeight: 600,
          color: "#202124",
          margin: "0 0 8px",
        }}
      >
        Media
      </h3>
      <Carousel
        withIndicators
        height={200}
        mb={10}
        slideGap="xs"
        slideSize="33.333333%"
        emblaOptions={{
          loop: true,
          dragFree: false,
          align: "center",
        }}
      >
        {photos.map((item, i) => {
          let imageUrl = item.url;
          const googleMapsKey = import.meta.env.VITE_GOOGLE_MAPS_KEY;

          if (imageUrl.includes("places.googleapis.com") && googleMapsKey) {
            const separator = imageUrl.includes("?") ? "&" : "?";
            imageUrl = `${imageUrl}${separator}key=${googleMapsKey}&maxHeightPx=400`;
          }

          return (
            <Carousel.Slide key={i}>
              <img
                src={imageUrl}
                alt={item.altText || item.title || "Location photo"}
                style={{
                  height: 190,
                  borderRadius: 8,
                  objectFit: "cover",
                  flexShrink: 0,
                }}
              />
            </Carousel.Slide>
          );
        })}
      </Carousel>
    </div>
  );
}

function FormatAddress({ address }: { address: LocationAddress }) {
  const lines = [
    address.address_line_1,
    address.address_line_2,
    address.address_line_3,
    [address.city, address.state_abbre, address.postal_code]
      .filter(Boolean)
      .join(", "),
  ].filter(Boolean);

  return (
    <div style={{ fontSize: 13, color: "#5f6368", lineHeight: 1.4 }}>
      {lines.map((l, i) => (
        <div key={i}>{l}</div>
      ))}
    </div>
  );
}

function CardContent({
  poi,
  details,
  isLoading,
}: {
  poi: PoiMarker;
  details?: LocationDetails | null;
  isLoading?: boolean;
}) {
  const style = getStyleForType(poi.locationTypeKey);
  const color = style.color;
  const label = style.label;
  const emoji = style.emoji;

  // Enriched data from details
  const description = details?.description || poi.description;

  // Extract attributes safely. If attributes are a JSON string, parse them into an object.
  let attrs: null | undefined | string | GoogleAttributes | MnDnrAttributes | NpsAttributes | RidbAttributes = details?.attributes;
  if (typeof attrs === "string") {
    try {
      attrs = JSON.parse(attrs);
    } catch (e: Error | unknown) {
      console.log("Failed to parse attributes JSON for location ID", poi.id, ":", e);
      // Fallback: try to repair common issues (single quotes) and parse again
      try {
        const repaired = (attrs as string).replace(/\b(undefined|null)\b/g, 'null').replace(/'/g, '"');
        attrs = JSON.parse(repaired);
      } catch (e2: Error | unknown) {
        console.log("Failed to parse repaired attributes JSON for location ID", poi.id, ":", e2);
        // If parsing fails, leave attrs as the original string
        // and let downstream code handle it safely.
      }
    }
  }
  const isGoogle = details?.data_source_name === "Google Places API";
  const isDnr =
    details?.data_source_name === "MN DNR" ||
    details?.data_source_name === "MN Combined Master Record";
  const isNps = details?.data_source_name === "NPS";
  const isRidb = details?.data_source_name === "RIDB";

  const googleAttrs = isGoogle ? (attrs as GoogleAttributes) : null;
  const dnrAttrs = isDnr ? (attrs as MnDnrAttributes) : null;
  const npsAttrs = isNps ? (attrs as NpsAttributes) : null;
  const ridbAttrs = isRidb ? (attrs as RidbAttributes) : null;

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
        {poi.id === "user-location"
          ? "Current Location • OpenStreetMap"
          : `${details?.location_type || "Location"} • ${details?.data_source_name || "Database"}`}
      </div>

      {/* Style badge + price */}
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
          <h3
            style={{
              fontSize: 14,
              fontWeight: 600,
              color: "#202124",
              margin: "0 0 4px",
            }}
          >
            About
          </h3>
          <p
            style={{
              margin: 0,
              fontSize: 14,
              color: "#3c4043",
              lineHeight: 1.5,
              whiteSpace: "pre-wrap",
            }}
          >
            <div dangerouslySetInnerHTML={{ __html: description }}></div>
          </p>
        </div>
      )}

      {/* AI Summary */}
      {details?.ai_generative_description && (
        <div
          style={{
            marginTop: 16,
            padding: 12,
            background: "#f8f9ff",
            borderRadius: 8,
            border: "1px solid #e8eaff",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              marginBottom: 4,
            }}
          >
            <MdOutlineAutoAwesome size={16} color="#4285f4" />
            <span style={{ fontSize: 12, fontWeight: 600, color: "#4285f4" }}>
              AI Summary
            </span>
          </div>
          <p
            style={{
              margin: 0,
              fontSize: 13,
              color: "#3c4043",
              lineHeight: 1.5,
            }}
          >
            {details.ai_generative_description}
          </p>
        </div>
      )}

      {/* Operating Hours */}
      {details?.operating_hours && details.operating_hours.length > 0 && (
        <div style={{ marginTop: 16 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 4px" }}>Hours</h3>
          {details.operating_hours.map((h, i) => (
            <div key={i} style={{ fontSize: 13, color: "#3c4043" }}>
              <span style={{ fontWeight: 500 }}>{h.name}:</span> {h.description}
            </div>
          ))}
        </div>
      )}

      {/* Weather Info */}
      {details?.weather_info && (
        <div style={{ marginTop: 16 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 4px" }}>Weather</h3>
          <p style={{ margin: 0, fontSize: 13, color: "#3c4043", lineHeight: 1.4 }}>{details.weather_info}</p>
        </div>
      )}

      {/* Directions Info */}
      {details?.directions_info && (
        <div style={{ marginTop: 16 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 4px" }}>Directions</h3>
          <p style={{ margin: 0, fontSize: 13, color: "#3c4043", lineHeight: 1.4 }}>
            <div dangerouslySetInnerHTML={{ __html: details.directions_info }}></div>
          </p>
        </div>
      )}

      {/* Contact & Address */}
      <div
        style={{
          marginTop: 20,
          display: "flex",
          flexDirection: "column",
          gap: 12,
        }}
      >
        {details?.addresses?.map((addr, i) => (
          <div key={i} style={{ display: "flex", gap: 12 }}>
            <HiOutlineMapPin size={20} color="#70757a" style={{ flexShrink: 0 }} />
            <FormatAddress address={addr} />
          </div>
        ))}

        {details?.phone_numbers?.map((ph, i) => (
          <div
            key={i}
            style={{ display: "flex", gap: 12, alignItems: "center" }}
          >
            <HiOutlinePhone size={20} color="#70757a" style={{ flexShrink: 0 }} />
            <a
              href={`tel:${ph.phone_number}`}
              style={{
                fontSize: 14,
                color: "#1a73e8",
                textDecoration: "none",
                fontWeight: 500,
              }}
              onMouseEnter={(e) => (e.currentTarget.style.textDecoration = "underline")}
              onMouseLeave={(e) => (e.currentTarget.style.textDecoration = "none")}
            >
              {ph.phone_number}
            </a>
          </div>
        ))}

        {details?.email_addresses?.map((email, i) => (
          <div
            key={i}
            style={{ display: "flex", gap: 12, alignItems: "center" }}
          >
            <HiOutlineEnvelope size={20} color="#70757a" style={{ flexShrink: 0 }} />
            <a
              href={`mailto:${email.email_address}`}
              style={{
                fontSize: 14,
                color: "#1a73e8",
                textDecoration: "none",
                fontWeight: 500,
                wordBreak: "break-all",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.textDecoration = "underline")}
              onMouseLeave={(e) => (e.currentTarget.style.textDecoration = "none")}
            >
              {email.email_address}
            </a>
          </div>
        ))}

        {details?.websites?.map((web, i) => (
          <div
            key={i}
            style={{ display: "flex", gap: 12, alignItems: "center" }}
          >
            <HiOutlineGlobeAlt size={20} color="#70757a" style={{ flexShrink: 0 }} />
            <a
              href={web.url}
              target="_blank"
              rel="noreferrer"
              style={{
                fontSize: 14,
                color: "#1a73e8",
                textDecoration: "none",
                fontWeight: 500,
                wordBreak: "break-all",
                display: "-webkit-box",
                WebkitLineClamp: 1,
                WebkitBoxOrient: "vertical",
                overflow: "hidden",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.textDecoration = "underline")}
              onMouseLeave={(e) => (e.currentTarget.style.textDecoration = "none")}
            >
              {web.url.replace(/^https?:\/\//, "")}
            </a>
          </div>
        ))}
      </div>

      {/* Source Specific Features (DNR) */}
      {dnrAttrs && (
        <div style={{ marginTop: 24 }}>
          {/* Camping Section */}
          {(dnrAttrs.site_drive_in || dnrAttrs.max_rv_length || dnrAttrs["30_amp_hookup_sites"] || dnrAttrs["50_amp_hookup_sites"] || dnrAttrs.horse_campsites || dnrAttrs.backpack_sites || dnrAttrs.group_campsites || dnrAttrs.camper_cabins) && (
            <div style={{ marginBottom: 20 }}>
              <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 10px", display: "flex", alignItems: "center", gap: 6 }}>
                <span>⛺</span> Camping
              </h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                {dnrAttrs.site_drive_in && <FeatureItem label="Drive-in Sites" icon="🚗" />}
                {dnrAttrs.max_rv_length && <FeatureItem label={`RV limit: ${dnrAttrs.max_rv_length}ft`} icon="🚐" />}
                {dnrAttrs["30_amp_hookup_sites"] && <FeatureItem label="30 Amp Hookups" icon="🔌" />}
                {dnrAttrs["50_amp_hookup_sites"] && <FeatureItem label="50 Amp Hookups" icon="⚡" />}
                {dnrAttrs.rv_pull_through_sites && <FeatureItem label="Pull-through" icon="↕️" />}
                {dnrAttrs.horse_campsites && <FeatureItem label="Horse Camp" icon="🏇" />}
                {dnrAttrs.backpack_sites && <FeatureItem label="Backpack Sites" icon="🎒" />}
                {dnrAttrs.group_campsites && <FeatureItem label="Group Camp" icon="👥" />}
                {dnrAttrs.camper_cabins && <FeatureItem label="Camper Cabins" icon="🏠" />}
              </div>
              {/* Camping Seasons */}
              <div style={{ marginTop: 8, fontSize: 12, color: "#70757a", background: "#f8f9fa", padding: "4px 8px", borderRadius: 4, display: "inline-block" }}>
                Seasons: {[
                  dnrAttrs.spring_camping && "Spring",
                  dnrAttrs.summer_camping && "Summer",
                  dnrAttrs.fall_camping && "Fall",
                  dnrAttrs.winter_camping && "Winter"
                ].filter(Boolean).join(", ") || "Contact Park"}
              </div>
            </div>
          )}

          {/* Facilities Section */}
          {(dnrAttrs.has_showers || dnrAttrs.has_flush_toilets || dnrAttrs.has_dump_station || dnrAttrs.picnic_shelter) && (
            <div style={{ marginBottom: 20 }}>
              <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 10px", display: "flex", alignItems: "center", gap: 6 }}>
                <span>🏢</span> Facilities
              </h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                {dnrAttrs.has_showers && <FeatureItem label="Showers" icon="🚿" />}
                {dnrAttrs.has_flush_toilets && <FeatureItem label="Flush Toilets" icon="🚻" />}
                {dnrAttrs.has_dump_station && <FeatureItem label="Dump Station" icon="🚮" />}
                {dnrAttrs.picnic_shelter && <FeatureItem label="Picnic Shelter" icon="⛱️" />}
              </div>
            </div>
          )}

          {/* Activities Section */}
          {(dnrAttrs.hiking_trails || dnrAttrs.paved_trails || dnrAttrs.groomed_cross_country_ski_trails || dnrAttrs.swimming_beach || dnrAttrs.fishing_pier || dnrAttrs.boat_rental || dnrAttrs.showshoe_rental || dnrAttrs.nature_programs) && (
            <div style={{ marginBottom: 20 }}>
              <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 10px", display: "flex", alignItems: "center", gap: 6 }}>
                <span>🚶</span> Activities
              </h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                {dnrAttrs.hiking_trails && <FeatureItem label="Hiking Trails" icon="🥾" />}
                {dnrAttrs.paved_trails && <FeatureItem label="Paved Trails" icon="🚲" />}
                {dnrAttrs.groomed_cross_country_ski_trails && <FeatureItem label="Ski Trails" icon="🎿" />}
                {dnrAttrs.swimming_beach && <FeatureItem label="Beach" icon="🏖️" />}
                {dnrAttrs.fishing_pier && <FeatureItem label="Fishing" icon="🎣" />}
                {dnrAttrs.boat_rental && <FeatureItem label="Boat Rental" icon="🚣" />}
                {dnrAttrs.showshoe_rental && <FeatureItem label="Snowshoe Rental" icon="❄️" />}
                {dnrAttrs.nature_programs && <FeatureItem label="Nature Programs" icon="🎓" />}
              </div>
            </div>
          )}

          {/* Accessibility Section */}
          {(dnrAttrs.site_accessible_drive_in || dnrAttrs.has_accessible_showers || dnrAttrs.has_accessible_flush_toilets || dnrAttrs.accessible_camper_cabins || dnrAttrs.accessible_fishing_pier || dnrAttrs.accessible_picnic_shelter || dnrAttrs.accessible_track_chair) && (
            <div style={{ marginBottom: 20 }}>
              <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 10px", display: "flex", alignItems: "center", gap: 6 }}>
                <span>♿</span> Accessibility
              </h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                {dnrAttrs.site_accessible_drive_in && <FeatureItem label="Accessible Sites" icon="♿" />}
                {dnrAttrs.has_accessible_showers && <FeatureItem label="Showers" icon="♿" />}
                {dnrAttrs.has_accessible_flush_toilets && <FeatureItem label="Toilets" icon="♿" />}
                {dnrAttrs.accessible_camper_cabins && <FeatureItem label="Cabins" icon="♿" />}
                {dnrAttrs.accessible_fishing_pier && <FeatureItem label="Fishing Pier" icon="♿" />}
                {dnrAttrs.accessible_picnic_shelter && <FeatureItem label="Picnic Shelter" icon="♿" />}
                {dnrAttrs.accessible_track_chair && <FeatureItem label="Track Chair" icon="♿" />}
              </div>
            </div>
          )}

          {/* Stats & Reference Section */}
          {(dnrAttrs.gis_acres || dnrAttrs.legislative_id || dnrAttrs.program_project || dnrAttrs.guid) && (
            <div style={{ 
              marginTop: 16, 
              padding: 12,
              background: "#f8f9fa",
              borderRadius: 8,
              fontSize: 12, 
              color: "#5f6368",
              border: "1px solid #eef0f2"
            }}>
              <h4 style={{ margin: "0 0 8px", fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "#70757a" }}>Reference Info</h4>
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                {dnrAttrs.gis_acres && (
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ fontWeight: 600 }}>Land Area:</span>
                    <span>{dnrAttrs.gis_acres.toLocaleString(undefined, { maximumFractionDigits: 2 })} acres</span>
                  </div>
                )}
                {dnrAttrs.legislative_id && (
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ fontWeight: 600 }}>Designation:</span>
                    <span>{dnrAttrs.legislative_id}</span>
                  </div>
                )}
                {dnrAttrs.program_project && (
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span style={{ fontWeight: 600 }}>Project ID:</span>
                    <span style={{ fontFamily: "monospace" }}>{dnrAttrs.program_project}</span>
                  </div>
                )}
                {dnrAttrs.guid && (
                  <div style={{ display: "flex", flexDirection: "column", marginTop: 4 }}>
                    <span style={{ fontWeight: 600 }}>DNR Global ID:</span>
                    <span style={{ fontFamily: "monospace", fontSize: 10, color: "#9aa0a6" }}>{dnrAttrs.guid}</span>
                  </div>
                )}
                {dnrAttrs.county && (
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4, paddingTop: 4, borderTop: "1px dashed #dadce0" }}>
                    <span style={{ fontWeight: 600 }}>County:</span>
                    <span>{dnrAttrs.county}</span>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {/* RIDB Specific Features */}
      {ridbAttrs && (
        <div style={{ marginTop: 20 }}>
          <h3 style={{ fontSize: 14, fontWeight: 600, color: "#202124", margin: "0 0 8px" }}>Facility Info</h3>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
            {ridbAttrs.reservable && <FeatureItem label="Reservable" icon="📅" />}
            {ridbAttrs.accessible && <FeatureItem label="Accessible" icon="♿" />}
            {ridbAttrs.facility_type && <FeatureItem label={ridbAttrs.facility_type} icon="🏢" />}
            {ridbAttrs.stay_limit && <FeatureItem label={`Stay limit: ${ridbAttrs.stay_limit}`} icon="⏱️" />}
          </div>
        </div>
      )}

      {/* NPS Activities */}
      {npsAttrs?.activities && (
        <div style={{ marginTop: 20 }}>
          <h3
            style={{
              fontSize: 14,
              fontWeight: 600,
              color: "#202124",
              margin: "0 0 8px",
            }}
          >
            Activities
          </h3>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {npsAttrs.activities.map((act, i) => (
              <span
                key={i}
                style={{
                  fontSize: 12,
                  padding: "4px 8px",
                  background: "#f1f3f4",
                  borderRadius: 4,
                  color: "#3c4043",
                }}
              >
                {act}
              </span>
            ))}
          </div>
        </div>
      )}

      {isLoading && (
        <div
          style={{
            marginTop: 16,
            textAlign: "center",
            color: "#70757a",
            fontSize: 13,
          }}
        >
          Updating with latest details...
        </div>
      )}
    </div>
  );
}

function FeatureItem({ label, icon }: { label: string; icon: string }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 6,
        fontSize: 13,
        color: "#3c4043",
      }}
    >
      <span>{icon}</span>
      <span>{label}</span>
    </div>
  );
}

export function LocationCard() {
  const selectedMarker = useMapStore((s) => s.selectedMarker);
  const setSelectedMarker = useMapStore((s) => s.setSelectedMarker);
  const userLocation = useMapStore((s) => s.userLocation);
  const setUserLocation = useMapStore((s) => s.setUserLocation);
  const mode = useResponsiveMode();

  const activeMarker = (selectedMarker || userLocation) as PoiMarker | null;
  const isOpen = activeMarker !== null;
  const isUserLocation = activeMarker?.id === "user-location";

  const { data: details, isLoading } = useLocationDetails(
    activeMarker && !isUserLocation ? Number(activeMarker.id) : null,
  );

  function handleClose() {
    setSelectedMarker(null);
    setUserLocation(null);
  }

  if (mode === "drawer") {
    return (
      <ResponsivePanel
        open={isOpen}
        onClose={handleClose}
        snapPoints={["148px", "320px", 1]}
        drawerClassName="explore-location-drawer"
        drawerTitle={details?.name || activeMarker?.name || "Location details"}
      >
        {activeMarker && (
          <CardContent
            poi={activeMarker}
            details={details}
            isLoading={isLoading}
          />
        )}
      </ResponsivePanel>
    );
  }

  return (
    <div
      role="complementary"
      aria-label={details?.name || activeMarker?.name || "Location details"}
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
        transition:
          "transform 0.3s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease",
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
        <HiOutlineXMark size={20} />
      </button>

      {activeMarker && (
        <CardContent
          poi={activeMarker}
          details={details}
          isLoading={isLoading}
        />
      )}
    </div>
  );
}
