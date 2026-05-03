import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import "@mantine/core/styles.css";
import { MantineProvider } from "@mantine/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "leaflet/dist/leaflet.css";
import "leaflet-gesture-handling/dist/leaflet-gesture-handling.css";
// import { GestureHandling } from "leaflet-gesture-handling";
// import L from "leaflet";
import { Route, Switch } from "wouter";
import Home from "./pages/Home.tsx";
import { MapPage } from "./pages/Map.tsx";

// Register gesture handling
// L.Map.addInitHook("addHandler", "gestureHandling", GestureHandling);

const queryClient = new QueryClient();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MantineProvider>
      <QueryClientProvider client={queryClient}></QueryClientProvider>
      <Switch>
        <Route path="/" component={Home} />
        <Route path="/map" component={MapPage} />
        <Route>404 Not Found</Route>
      </Switch>
    </MantineProvider>
  </StrictMode>,
);
