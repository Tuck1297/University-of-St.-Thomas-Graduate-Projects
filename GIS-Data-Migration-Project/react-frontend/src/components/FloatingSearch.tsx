"use client";

import { useState, useMemo } from "react";
import { useMapStore } from "../hooks/useMapStore";
import { TYPE_MAPPING } from "../types/locationTypeMapping";
import { useDebouncedCallback } from "@mantine/hooks";

export function FloatingSearch() {
  const [showFilters, setShowFilters] = useState(false);
  const search = useMapStore((s) => s.filters.search);
  const [searchValue, setSearchValue] = useState(search);
  const [prevSearch, setPrevSearch] = useState(search);
  const locationTypes = useMapStore((s) => s.filters.locationTypes);
  const setFilters = useMapStore((s) => s.setFilters);

  // Sync local value with store during render (recommended over useEffect)
  if (search !== prevSearch) {
    setPrevSearch(search);
    setSearchValue(search);
  }

  const debouncedSetFilters = useDebouncedCallback((value: string) => {
    setFilters({ search: value });
  }, 300);

  // Group unique types for cleaner filtering
  const filterOptions = useMemo(() => {
    const seen = new Set<string>();
    return Object.entries(TYPE_MAPPING)
      .map(([key, style]) => ({ value: Number(key), ...style }))
      .filter((opt) => {
        if (seen.has(opt.label)) return false;
        seen.add(opt.label);
        return true;
      })
      .slice(0, 12); // Limit to top/representative types for the UI
  }, []);

  function handleSearchChange(e: React.ChangeEvent<HTMLInputElement>) {
    const value = e.target.value;
    setSearchValue(value);
    debouncedSetFilters(value);
  }

  function handleTypeClick(typeKey: number) {
    const isSelected = locationTypes.includes(typeKey);
    if (isSelected) {
      setFilters({ locationTypes: locationTypes.filter((t) => t !== typeKey) });
    } else {
      setFilters({ locationTypes: [...locationTypes, typeKey] });
    }
  }

  return (
    <div
      style={{
        position: "absolute",
        top: 16,
        left: "50%",
        transform: "translateX(-50%)",
        zIndex: 1000,
        width: "min(500px, 90vw)",
        display: "flex",
        flexDirection: "column",
        gap: 8,
      }}
    >
      {/* Search pill */}
      <div
        style={{
          background: "#ffffff",
          borderRadius: 24,
          boxShadow: "0 2px 8px rgba(0,0,0,0.18), 0 1px 3px rgba(0,0,0,0.1)",
          display: "flex",
          alignItems: "center",
          padding: "0 8px 0 16px",
          height: 48,
          gap: 8,
        }}
      >
        {/* Magnifying glass icon */}
        <svg
          width="18"
          height="18"
          viewBox="0 0 24 24"
          fill="none"
          stroke="#9aa0a6"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          style={{ flexShrink: 0 }}
          aria-hidden="true"
        >
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>

        {/* Text input */}
        <input
          type="text"
          value={searchValue}
          onChange={handleSearchChange}
          placeholder="Search places..."
          style={{
            flex: 1,
            border: "none",
            outline: "none",
            background: "transparent",
            fontSize: 15,
            color: "#202124",
            lineHeight: "1.4",
          }}
          aria-label="Search places"
        />

        {/* Filter toggle button */}
        <button
          type="button"
          onClick={() => setShowFilters((prev) => !prev)}
          aria-label={showFilters ? "Hide type filters" : "Show type filters"}
          aria-pressed={showFilters}
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            width: 36,
            height: 36,
            borderRadius: "50%",
            border: "none",
            background: showFilters ? "#e8f0fe" : "transparent",
            cursor: "pointer",
            flexShrink: 0,
            transition: "background 0.15s ease",
            padding: 0,
          }}
        >
          <svg
            width="18"
            height="18"
            viewBox="0 0 24 24"
            fill="none"
            stroke={showFilters ? "#4285f4" : "#5f6368"}
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <line x1="4" y1="6" x2="20" y2="6" />
            <line x1="8" y1="12" x2="16" y2="12" />
            <line x1="11" y1="18" x2="13" y2="18" />
          </svg>
        </button>
      </div>

      {/* Type chips row */}
      {showFilters && (
        <div
          style={{
            background: "#ffffff",
            borderRadius: 24,
            boxShadow: "0 2px 8px rgba(0,0,0,0.18), 0 1px 3px rgba(0,0,0,0.1)",
            padding: "8px 12px",
            display: "flex",
            gap: 6,
            overflowX: "auto",
          }}
          role="group"
          aria-label="Filter by location type"
        >
          {filterOptions.map((type) => {
            const isActive = locationTypes.includes(type.value);
            return (
              <button
                key={type.value}
                type="button"
                onClick={() => handleTypeClick(type.value)}
                aria-pressed={isActive}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                  padding: "5px 12px",
                  borderRadius: 16,
                  border: `1.5px solid ${isActive ? type.color : "#e0e0e0"}`,
                  background: isActive ? type.color + "18" : "#ffffff",
                  color: isActive ? type.color : "#5f6368",
                  fontSize: 13,
                  fontWeight: isActive ? 600 : 400,
                  cursor: "pointer",
                  whiteSpace: "nowrap",
                  flexShrink: 0,
                  transition: "all 0.15s ease",
                }}
              >
                <span style={{ fontSize: "14px" }} aria-hidden="true">
                  {type.emoji}
                </span>
                {type.label}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
