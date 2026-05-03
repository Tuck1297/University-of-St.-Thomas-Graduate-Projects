"use client";

import { useState, useSyncExternalStore } from "react";
import { useMediaQuery } from "@mantine/hooks";
import { Drawer } from "vaul";

const emptySubscribe = () => () => {};

/** Height of the drag-handle bar (padding + bar height). */
const HANDLE_HEIGHT = 20;

/**
 * Convert a snap-point value to a pixel number.
 *  - "148px" → 148
 *  - 1       → window.innerHeight  (fraction of viewport)
 *  - 0.5     → window.innerHeight * 0.5
 */
function snapToPixels(snap: string | number): number {
  if (typeof snap === "string") {
    return parseFloat(snap);
  }
  // fraction (0–1) of viewport height
  return typeof window !== "undefined"
    ? Math.round(window.innerHeight * snap)
    : 600;
}

interface ResponsivePanelProps {
  open: boolean;
  onClose: () => void;
  children: React.ReactNode;
  snapPoints?: (string | number)[];
  sidebarWidth?: number;
  sidebarClassName?: string;
  drawerClassName?: string;
  /** Accessible title for the drawer (visually hidden) */
  drawerTitle?: string;
}

export default function ResponsivePanel({
  open,
  onClose,
  children,
  snapPoints = ["148px", "355px", 1],
  sidebarWidth = 400,
  sidebarClassName = "",
  drawerClassName = "",
  drawerTitle = "Panel",
}: ResponsivePanelProps) {
  const isClient = useSyncExternalStore(
    emptySubscribe,
    () => true,
    () => false,
  );
  const isMobile = useMediaQuery("(max-width: 768px)");
  const mode = isClient ? (isMobile ? "drawer" : "sidebar") : "sidebar";

  // Track active snap so we can constrain the scrollable area height
  const [activeSnap, setActiveSnap] = useState<number | string | null>(
    snapPoints[0],
  );

  // Reset snap when drawer opens
  const [trackedOpen, setTrackedOpen] = useState(open);
  if (trackedOpen !== open) {
    setTrackedOpen(open);
    if (open) {
      setActiveSnap(snapPoints[0]);
    }
  }

  if (mode === "drawer") {
    // Compute the max-height for the scrollable content area based on the
    // current snap point, minus the drag handle height.
    const snapPx = activeSnap !== null ? snapToPixels(activeSnap) : 0;
    const contentMaxHeight = Math.max(0, snapPx - HANDLE_HEIGHT);

    return (
      <Drawer.Root
        modal={false}
        open={open}
        onOpenChange={(isOpen) => !isOpen && onClose()}
        snapPoints={snapPoints}
        activeSnapPoint={activeSnap}
        setActiveSnapPoint={setActiveSnap}
      >
        <Drawer.Portal>
          <Drawer.Content
            className={drawerClassName}
            style={{
              position: "fixed",
              bottom: 0,
              left: 0,
              right: 0,
              height: "100dvh",
              zIndex: 1000,
              background: "white",
              borderTopLeftRadius: 12,
              borderTopRightRadius: 12,
              boxShadow: "0 -4px 16px rgba(0,0,0,0.15)",
              outline: "none",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <Drawer.Title
              style={{
                position: "absolute",
                width: 1,
                height: 1,
                padding: 0,
                margin: -1,
                overflow: "hidden",
                clip: "rect(0,0,0,0)",
                whiteSpace: "nowrap",
                borderWidth: 0,
              }}
            >
              {drawerTitle}
            </Drawer.Title>
            {/* Drag handle — not scrollable */}
            <div
              style={{
                display: "flex",
                justifyContent: "center",
                padding: "8px 0 4px",
                flexShrink: 0,
              }}
            >
              <div
                style={{
                  width: 40,
                  height: 4,
                  borderRadius: 2,
                  background: "#ccc",
                }}
              />
            </div>
            {/* Content area — exactly fills the visible snap region.
                Children with height:100% fill this and scroll internally.
                Children without internal scroll get overflow-y:auto here. */}
            <div
              data-vaul-no-drag
              style={{
                height: contentMaxHeight,
                flexShrink: 0,
                overflowY: "auto",
                WebkitOverflowScrolling: "touch",
                touchAction: "pan-y",
                padding: "0 16px 16px",
              }}
            >
              {children}
            </div>
          </Drawer.Content>
        </Drawer.Portal>
      </Drawer.Root>
    );
  }

  // Desktop: always render for slide animation, hide with translateX
  return (
    <div
      className={sidebarClassName}
      style={{
        position: "absolute",
        top: 0,
        left: 0,
        bottom: 0,
        width: sidebarWidth,
        zIndex: 1000,
        background: "white",
        boxShadow: open ? "4px 0 16px rgba(0,0,0,0.1)" : "none",
        overflow: "auto",
        transform: open ? "translateX(0)" : "translateX(-100%)",
        transition: "transform 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
        pointerEvents: open ? "auto" : "none",
      }}
    >
      <div style={{ padding: 16 }}>{children}</div>
    </div>
  );
}
