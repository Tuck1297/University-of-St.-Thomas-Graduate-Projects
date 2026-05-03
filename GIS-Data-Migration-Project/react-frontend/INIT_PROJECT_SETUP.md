# GIS Data Migration - React Frontend Setup Guide

This guide outlines the steps to set up a minimum React project with the following tech stack:
- **Framework**: Vite (React + TypeScript)
- **State Management/Data Fetching**: TanStack Query (React Query) & Ky
- **UI Framework**: Mantine
- **Mapping**: Leaflet, React-Leaflet, React-Leaflet-Cluster, Leaflet-Gesture-Handling
- **Routing**: Wouter

## 1. Initialize Project

Run the following command to create a new Vite project:

```bash
npm create vite@latest . -- --template react-ts
```

## 2. Install Dependencies

Install the core libraries:

```bash
npm install @tanstack/react-query ky @mantine/core @mantine/hooks wouter leaflet react-leaflet react-leaflet-cluster leaflet-gesture-handling
```

Install Leaflet types (if using TypeScript):

```bash
npm install -D @types/leaflet
```

## 3. Configuration

### Mantine Setup
Mantine requires PostCSS for its styling. Ensure you have the necessary PostCSS configuration if it's not automatically handled by Vite (usually Vite handles `.css` files fine, but Mantine v7 prefers its own CSS imports).

In `src/main.tsx`:
```tsx
import '@mantine/core/styles.css';
import { MantineProvider } from '@mantine/core';

// ... inside render
<MantineProvider>
  <App />
</MantineProvider>
```

### TanStack Query Setup
In `src/main.tsx`:
```tsx
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const queryClient = new QueryClient();

// ... wrap App
<QueryClientProvider client={queryClient}>
  <MantineProvider>
    <App />
  </MantineProvider>
</QueryClientProvider>
```

### Leaflet & Gesture Handling
You must import Leaflet's CSS for the map to render correctly.

In `src/main.tsx` or your Map component:
```tsx
import 'leaflet/dist/leaflet.css';
import 'leaflet-gesture-handling/dist/leaflet-gesture-handling.css';
import { GestureHandling } from 'leaflet-gesture-handling';
import L from 'leaflet';

// Register gesture handling
L.Map.addInitHook('addHandler', 'gestureHandling', GestureHandling);
```

### Routing with Wouter
In `src/App.tsx`:
```tsx
import { Route, Switch } from 'wouter';

function App() {
  return (
    <Switch>
      <Route path="/" component={Home} />
      <Route path="/map" component={MapPage} />
      <Route>404 Not Found</Route>
    </Switch>
  );
}
```

## 4. Basic Map Component Example

```tsx
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import MarkerClusterGroup from 'react-leaflet-cluster';

export function Map() {
  return (
    <MapContainer 
      center={[44.95, -93.09]} 
      zoom={13} 
      style={{ height: '500px', width: '100%' }}
      gestureHandling={true}
    >
      <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
      <MarkerClusterGroup>
        <Marker position={[44.95, -93.09]}>
          <Popup>St. Paul, MN</Popup>
        </Marker>
      </MarkerClusterGroup>
    </MapContainer>
  );
}
```

## 5. Fetching Data with Ky and TanStack Query

```tsx
import { useQuery } from '@tanstack/react-query';
import ky from 'ky';

const fetchData = async () => {
  return await ky.get('https://api.example.com/data').json();
};

function DataComponent() {
  const { data, isLoading } = useQuery({ queryKey: ['gisData'], queryFn: fetchData });
  // ...
}
```