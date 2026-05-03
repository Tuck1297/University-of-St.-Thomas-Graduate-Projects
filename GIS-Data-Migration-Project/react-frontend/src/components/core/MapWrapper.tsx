"use client";

import React, { Suspense, lazy } from "react";
import type { ComponentProps } from "react";
import type { default as ClusteredMapType } from "./ClusteredMap";

const ClusteredMap = lazy(() => import("./ClusteredMap"));

export type MapWrapperProps = ComponentProps<typeof ClusteredMapType>;

export default function MapWrapper(props: MapWrapperProps) {
  return (
    <Suspense
      fallback={
        <div
          style={{
            width: "100%",
            height: "100%",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            background: "#f0f0f0",
          }}
        >
          Loading map...
        </div>
      }
    >
      <ClusteredMap {...props} />
    </Suspense>
  );
}
