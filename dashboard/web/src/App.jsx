import { useDeferredValue, useEffect, useState } from "react";
import DeckGL from "@deck.gl/react";
import { ScatterplotLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

const API_BASE_URL = import.meta.env.VITE_LIVE_API_URL || "http://127.0.0.1:8000";
const MAP_STYLE = "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json";

// CTA L-line brand colors (used when city=chicago and route_type=1)
const CTA_LINE_COLORS = {
  red:  [198,  12,  36, 235],
  blue: [  0, 161, 222, 235],
  brn:  [ 98,  54,  27, 235],
  g:    [  0, 155,  58, 235],
  org:  [249,  70,  28, 235],
  p:    [ 82,  35, 152, 235],
  pink: [226, 126, 166, 235],
  y:    [249, 227,   0, 245],
};

function statusColor(status) {
  switch (status) {
    case "IN_TRANSIT_TO":
      return [255, 166, 43, 220];
    case "STOPPED_AT":
      return [237, 91, 91, 230];
    case "INCOMING_AT":
      return [43, 130, 255, 230];
    default:
      return [27, 198, 174, 210];
  }
}

function displayStatus(vehicle) {
  if (vehicle.current_status === "IN_TRANSIT_TO") {
    return "In transit";
  }
  if (vehicle.current_status === "STOPPED_AT") {
    return vehicle.city === "chicago" && vehicle.route_type === 3 ? "Delayed" : "Stopped";
  }
  if (vehicle.current_status === "INCOMING_AT") {
    return "Approaching stop";
  }

  if (vehicle.city === "chicago" && vehicle.route_type === 3) {
    return "Reporting live";
  }

  return "Unknown";
}

function vehicleColor(vehicle) {
  // Chicago L-trains: use authentic CTA line brand colors
  if (vehicle.city === "chicago" && vehicle.route_type === 1) {
    const color = CTA_LINE_COLORS[vehicle.route_id?.toLowerCase()];
    return color ?? [120, 120, 120, 220];
  }
  // Chicago buses: orange
  if (vehicle.city === "chicago" && vehicle.route_type === 3) {
    return [255, 131, 43, 220];
  }
  // Boston / other: status-based color
  return statusColor(vehicle.current_status);
}

function vehicleRadius(vehicle) {
  if (vehicle.city === "chicago" && vehicle.route_type === 1) {
    return 66;
  }
  if (vehicle.city === "chicago" && vehicle.route_type === 3) {
    return 56;
  }
  if (vehicle.route_type === 1 || vehicle.route_type === 2) {
    return 58;
  }
  return 46;
}

function routeTypeLabel(routeType) {
  const labels = {
    0: "Light rail",
    1: "Subway",
    2: "Commuter rail",
    3: "Bus",
    4: "Ferry",
  };
  return labels[routeType] || "Transit";
}

function cityViewState(city) {
  if (!city) {
    return {
      latitude: 42.3601,
      longitude: -71.0589,
      zoom: 11.2,
      bearing: 0,
      pitch: 35,
    };
  }

  return {
    latitude: city.latitude,
    longitude: city.longitude,
    zoom: city.zoom,
    bearing: 0,
    pitch: 35,
  };
}

function App() {
  const [cities, setCities] = useState([]);
  const [selectedCity, setSelectedCity] = useState("boston");
  const [vehicleMap, setVehicleMap] = useState({});
  const [routeFilter, setRouteFilter] = useState("");
  const [health, setHealth] = useState(null);
  const [connectionState, setConnectionState] = useState("connecting");
  const [errorMessage, setErrorMessage] = useState("");
  const [mapViewState, setMapViewState] = useState(cityViewState(null));

  useEffect(() => {
    async function loadCities() {
      const response = await fetch(`${API_BASE_URL}/api/live/cities`);
      if (!response.ok) {
        throw new Error("Unable to load live cities");
      }
      const payload = await response.json();
      setCities(payload);
    }

    loadCities().catch((error) => {
      setErrorMessage(error.message);
    });
  }, []);

  useEffect(() => {
    async function loadSnapshot() {
      setErrorMessage("");
      const response = await fetch(`${API_BASE_URL}/api/live/${selectedCity}/vehicles`);
      if (!response.ok) {
        throw new Error("Unable to load live snapshot");
      }
      const payload = await response.json();

      const nextVehicleMap = {};
      for (const vehicle of payload.vehicles) {
        nextVehicleMap[vehicle.vehicle_id] = vehicle;
      }
      setVehicleMap(nextVehicleMap);
    }

    async function loadHealth() {
      const response = await fetch(`${API_BASE_URL}/api/live/${selectedCity}/health`);
      if (!response.ok) {
        return;
      }
      const payload = await response.json();
      setHealth(payload);
    }

    loadSnapshot().catch((error) => {
      setErrorMessage(error.message);
    });
    loadHealth();
    setRouteFilter("");
  }, [selectedCity]);

  useEffect(() => {
    const wsUrl = API_BASE_URL.replace(/^http/, "ws");
    const socket = new WebSocket(`${wsUrl}/ws/live/${selectedCity}`);

    socket.onopen = () => {
      setConnectionState("live");
    };

    socket.onclose = () => {
      setConnectionState("reconnecting");
    };

    socket.onerror = () => {
      setConnectionState("error");
    };

    socket.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (payload.type === "vehicle_update" && payload.vehicle) {
        setVehicleMap((current) => ({
          ...current,
          [payload.vehicle.vehicle_id]: payload.vehicle,
        }));
      }
    };

    return () => {
      socket.close();
    };
  }, [selectedCity]);

  const vehicles = Object.values(vehicleMap);
  const routeOptions = Array.from(
    new Set(vehicles.map((vehicle) => vehicle.route_id).filter(Boolean)),
  ).sort();

  const filteredVehicles = vehicles.filter((vehicle) => {
    if (!routeFilter) {
      return true;
    }
    return vehicle.route_id === routeFilter;
  });
  const deferredVehicles = useDeferredValue(filteredVehicles);

  const cityConfig = cities.find((city) => city.slug === selectedCity);

  useEffect(() => {
    setMapViewState(cityViewState(cityConfig));
  }, [selectedCity, cityConfig]);

  const layer = new ScatterplotLayer({
    id: "vehicle-positions",
    data: deferredVehicles,
    getPosition: (vehicle) => [vehicle.longitude, vehicle.latitude],
    getFillColor: (vehicle) => vehicleColor(vehicle),
    getLineColor: [16, 24, 39, 220],
    getRadius: (vehicle) => vehicleRadius(vehicle),
    radiusMinPixels: 5,
    radiusMaxPixels: 16,
    pickable: true,
    stroked: true,
    filled: true,
    opacity: 0.95,
  });

  return (
    <div className="app-shell">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Realtime Transit Command View</p>
          <h1>Live transit — Boston &amp; Chicago</h1>
          <p className="hero-copy">
            Multi-city live vehicle tracking. Boston shows MBTA buses, subway, and
            commuter rail. Chicago shows CTA buses (orange) and L-trains colored by
            line. Switch cities above to pan the map and update the live feed.
          </p>
        </div>
        <div className={`connection-pill connection-${connectionState}`}>
          <span className="pill-dot" />
          {connectionState === "live" ? "Live stream connected" : connectionState}
        </div>
      </section>

      <section className="workspace">
        <aside className="control-panel">
          <div className="panel-block">
            <label htmlFor="city-select">City</label>
            <select
              id="city-select"
              value={selectedCity}
              onChange={(event) => setSelectedCity(event.target.value)}
            >
              {cities.map((city) => (
                <option key={city.slug} value={city.slug} disabled={!city.supports_live}>
                  {city.display_name}
                  {!city.supports_live ? " (live coming soon)" : ""}
                </option>
              ))}
            </select>
          </div>

          <div className="panel-block">
            <label htmlFor="route-filter">Route filter</label>
            <select
              id="route-filter"
              value={routeFilter}
              onChange={(event) => setRouteFilter(event.target.value)}
            >
              <option value="">All routes</option>
              {routeOptions.map((routeId) => (
                <option key={routeId} value={routeId}>
                  {routeId}
                </option>
              ))}
            </select>
          </div>

          <div className="stats-grid">
            <article className="stat-card">
              <span className="stat-label">Vehicles on map</span>
              <strong>{deferredVehicles.length}</strong>
            </article>
            <article className="stat-card">
              <span className="stat-label">Routes active</span>
              <strong>{routeOptions.length}</strong>
            </article>
            <article className="stat-card">
              <span className="stat-label">Last backend update</span>
              <strong>{health?.last_upsert_at ? new Date(health.last_upsert_at).toLocaleTimeString() : "Waiting"}</strong>
            </article>
          </div>

          <div className="panel-block">
            <h2>Live feed notes</h2>
            <ul className="compact-list">
              <li>Basemap is MapLibre over a real street style.</li>
              <li>Vehicle state is city-aware so Chicago can slot in later.</li>
              <li>OSM overlays can be added as additional map layers.</li>
            </ul>
          </div>

          {errorMessage ? (
            <div className="error-banner">{errorMessage}</div>
          ) : null}
        </aside>

        <main className="map-panel">
          <div className="map-frame">
            <DeckGL
              viewState={mapViewState}
              onViewStateChange={({ viewState }) => setMapViewState(viewState)}
              controller={true}
              layers={[layer]}
              getTooltip={({ object }) =>
                object
                  ? {
                      html: `
                        <div class="tooltip-title">${object.label || object.vehicle_id}</div>
                        <div>Route: ${object.route_label || object.route_id || "Unknown"}</div>
                        <div>Mode: ${routeTypeLabel(object.route_type)}</div>
                        <div>Status: ${displayStatus(object)}</div>
                        <div>Updated: ${object.updated_at ? new Date(object.updated_at).toLocaleTimeString() : "Unknown"}</div>
                      `,
                    }
                  : null
              }
            >
              <Map
                reuseMaps={true}
                mapLib={maplibregl}
                mapStyle={MAP_STYLE}
                attributionControl={true}
              />
            </DeckGL>
          </div>

          <div className="vehicle-table-shell">
            <div className="table-header">
              <h2>Live vehicles</h2>
              <span>{deferredVehicles.length} rows</span>
            </div>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Vehicle</th>
                    <th>Route</th>
                    <th>Status</th>
                    <th>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {deferredVehicles.slice(0, 25).map((vehicle) => (
                    <tr key={vehicle.vehicle_id}>
                      <td>{vehicle.label || vehicle.vehicle_id}</td>
                      <td>{vehicle.route_label || vehicle.route_id || "Unknown"}</td>
                      <td>{displayStatus(vehicle)}</td>
                      <td>{vehicle.updated_at ? new Date(vehicle.updated_at).toLocaleTimeString() : "Unknown"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </main>
      </section>
    </div>
  );
}

export default App;
