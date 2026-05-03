"use client";

import { useSyncExternalStore } from "react";
import { useMediaQuery } from "@mantine/hooks";

const emptySubscribe = () => () => {};

export function useResponsiveMode() {
  const isClient = useSyncExternalStore(
    emptySubscribe,
    () => true,
    () => false,
  );
  const isMobile = useMediaQuery("(max-width: 768px)");
  return isClient ? (isMobile ? "drawer" : "sidebar") : "sidebar";
}
