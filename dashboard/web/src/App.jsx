import { useDeferredValue, useEffect, useState } from "react";
import DeckGL from "@deck.gl/react";
import { ScatterplotLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

const API_BASE_URL = import.meta.env.VITE_LIVE_API_URL || "http://127.0.0.1:8000";
const MAP_STYLE = "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json";

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

function App() {
  const [cities, setCities] = useState([]);
  const [selectedCity, setSelectedCity] = useState("boston");
  const [vehicleMap, setVehicleMap] = useState({});
  const [routeFilter, setRouteFilter] = useState("");
  const [health, setHealth] = useState(null);
  const [connectionState, setConnectionState] = useState("connecting");
  const [errorMessage, setErrorMessage] = useState("");

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
  const viewState = cityConfig
    ? {
        latitude: cityConfig.latitude,
        longitude: cityConfig.longitude,
        zoom: cityConfig.zoom,
        bearing: 0,
        pitch: 35,
      }
    : {
        latitude: 42.3601,
        longitude: -71.0589,
        zoom: 11.2,
        bearing: 0,
        pitch: 35,
      };

  const layer = new ScatterplotLayer({
    id: "vehicle-positions",
    data: deferredVehicles,
    getPosition: (vehicle) => [vehicle.longitude, vehicle.latitude],
    getFillColor: (vehicle) => statusColor(vehicle.current_status),
    getLineColor: [16, 24, 39, 220],
    getRadius: 42,
    radiusMinPixels: 4,
    radiusMaxPixels: 12,
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
          <h1>Boston live vehicles over a real city map</h1>
          <p className="hero-copy">
            Kafka and Flink can sit upstream later. For now, this frontend is already
            wired for live position updates, city-aware APIs, and future OSM overlays.
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
              initialViewState={viewState}
              controller={true}
              layers={[layer]}
              getTooltip={({ object }) =>
                object
                  ? {
                      html: `
                        <div class="tooltip-title">${object.label || object.vehicle_id}</div>
                        <div>Route: ${object.route_label || object.route_id || "Unknown"}</div>
                        <div>Mode: ${routeTypeLabel(object.route_type)}</div>
                        <div>Status: ${object.current_status || "Unknown"}</div>
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
                      <td>{vehicle.current_status || "Unknown"}</td>
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
