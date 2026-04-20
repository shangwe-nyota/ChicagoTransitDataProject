import { startTransition, useDeferredValue, useEffect, useState } from "react";
import DeckGL from "@deck.gl/react";
import { PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

const API_BASE_URL = import.meta.env.VITE_LIVE_API_URL || "http://127.0.0.1:8000";
const MAP_STYLE = "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json";

const CTA_LINE_COLORS = {
  red: [198, 12, 36, 235],
  blue: [0, 161, 222, 235],
  brn: [98, 54, 27, 235],
  g: [0, 155, 58, 235],
  org: [249, 70, 28, 235],
  p: [82, 35, 152, 235],
  pink: [226, 126, 166, 235],
  y: [249, 227, 0, 245],
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
  if (vehicle.city === "chicago" && vehicle.route_type === 1) {
    return CTA_LINE_COLORS[vehicle.route_id?.toLowerCase()] ?? [120, 120, 120, 220];
  }
  if (vehicle.city === "chicago" && vehicle.route_type === 3) {
    return [255, 131, 43, 220];
  }
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

function formatNumber(value) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) {
    return "0";
  }
  return numeric.toLocaleString();
}

function formatCompact(value) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) {
    return "0";
  }
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(numeric);
}

function formatTime(value) {
  if (!value) {
    return "Waiting";
  }
  return new Date(value).toLocaleTimeString();
}

function routeLabel(route) {
  if (!route) {
    return "Unknown route";
  }
  if (route.route_short_name && route.route_long_name) {
    return `${route.route_short_name} — ${route.route_long_name}`;
  }
  return route.route_short_name || route.route_long_name || route.route_id || "Unknown route";
}

function batchStopColor(stop, lens, maxValue) {
  const score = lens === "poi" ? (stop.poi_count_within_400m ?? 0) : (stop.trip_count ?? 0);
  const ratio = maxValue > 0 ? score / maxValue : 0;

  if (lens === "poi") {
    if (ratio > 0.75) return [22, 163, 74, 235];
    if (ratio > 0.45) return [74, 222, 128, 230];
    if (ratio > 0.2) return [251, 191, 36, 220];
    return [56, 189, 248, 210];
  }

  if (ratio > 0.75) return [244, 114, 54, 235];
  if (ratio > 0.45) return [251, 191, 36, 225];
  if (ratio > 0.2) return [52, 211, 153, 220];
  return [96, 165, 250, 210];
}

function batchStopRadius(stop, lens, maxValue) {
  const score = lens === "poi" ? (stop.poi_count_within_400m ?? 0) : (stop.trip_count ?? 0);
  const ratio = maxValue > 0 ? score / maxValue : 0;
  return Math.max(42, Math.min(180, 42 + ratio * 120));
}

function rowValue(row, lens) {
  return lens === "poi" ? row.poi_count_within_400m ?? 0 : row.trip_count ?? 0;
}

function ModeToggle({ mode, setMode }) {
  return (
    <div className="mode-switch">
      <button
        type="button"
        className={mode === "live" ? "mode-chip active" : "mode-chip"}
        onClick={() => setMode("live")}
      >
        Live Ops
      </button>
      <button
        type="button"
        className={mode === "batch" ? "mode-chip active" : "mode-chip"}
        onClick={() => setMode("batch")}
      >
        Batch Atlas
      </button>
    </div>
  );
}

function StatCard({ label, value, accent = "" }) {
  return (
    <article className={`stat-card ${accent}`}>
      <span className="stat-label">{label}</span>
      <strong>{value}</strong>
    </article>
  );
}

function InsightTable({ title, caption, rows, columns }) {
  return (
    <section className="insight-card">
      <div className="insight-header">
        <h3>{title}</h3>
        <p>{caption}</p>
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.key}>{column.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={`${title}-${index}-${row.stop_id || row.route_id || row.highway || row.city}`}>
                {columns.map((column) => (
                  <td key={column.key}>{column.render ? column.render(row) : row[column.key]}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function CoverageCard({ rows }) {
  return (
    <section className="insight-card">
      <div className="insight-header">
        <h3>Transit coverage by road type</h3>
        <p>How much of each road class sits within reach of a transit stop.</p>
      </div>
      <div className="coverage-stack">
        {rows.map((row) => (
          <div className="coverage-row" key={row.highway}>
            <div className="coverage-meta">
              <strong>{row.highway}</strong>
              <span>{row.coverage_pct}% coverage</span>
            </div>
            <div className="coverage-bar-shell">
              <div className="coverage-bar-fill" style={{ width: `${Math.min(Number(row.coverage_pct || 0), 100)}%` }} />
            </div>
            <small>
              {formatNumber(row.road_segments_near_transit)} / {formatNumber(row.total_road_segments)} segments
            </small>
          </div>
        ))}
      </div>
    </section>
  );
}

function ComparisonCard({ city }) {
  return (
    <article className="comparison-card">
      <div className="comparison-heading">
        <p>{city.display_name}</p>
        <span>{formatCompact(city.total_stop_events)} stop events</span>
      </div>
      <div className="comparison-grid">
        <div>
          <span>Stops</span>
          <strong>{formatNumber(city.total_stops)}</strong>
        </div>
        <div>
          <span>Routes</span>
          <strong>{formatNumber(city.total_routes)}</strong>
        </div>
        <div>
          <span>Avg POIs / stop</span>
          <strong>{Number(city.avg_poi_access_per_stop || 0).toFixed(1)}</strong>
        </div>
        <div>
          <span>POI-ready stops</span>
          <strong>{formatNumber(city.stops_with_poi_context)}</strong>
        </div>
      </div>
      <div className="comparison-notes">
        <div>
          <span>Busiest stop</span>
          <strong>{city.busiest_stop?.stop_name || "N/A"}</strong>
        </div>
        <div>
          <span>Most active route</span>
          <strong>{routeLabel(city.busiest_route)}</strong>
        </div>
      </div>
    </article>
  );
}

function LiveWorkspace({
  cities,
  selectedCity,
  setSelectedCity,
  routeFilter,
  setRouteFilter,
  health,
  connectionState,
  errorMessage,
  mapViewState,
  setMapViewState,
  vehicles,
}) {
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
    <section className="workspace">
      <aside className="control-panel">
        <div className="panel-block">
          <label htmlFor="city-select-live">City</label>
          <select
            id="city-select-live"
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
          <StatCard label="Vehicles on map" value={formatNumber(deferredVehicles.length)} />
          <StatCard label="Routes active" value={formatNumber(routeOptions.length)} />
          <StatCard label="Last backend update" value={health?.last_upsert_at ? formatTime(health.last_upsert_at) : "Waiting"} />
        </div>

        <div className="panel-block">
          <h2>Live feed notes</h2>
          <ul className="compact-list">
            <li>MapLibre basemap with deck.gl vehicle rendering.</li>
            <li>Boston shows MBTA buses, rail, and subway.</li>
            <li>Chicago currently looks strongest as a CTA bus live mode.</li>
          </ul>
        </div>

        {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}
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
            <Map reuseMaps={true} mapLib={maplibregl} mapStyle={MAP_STYLE} attributionControl={true} />
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
  );
}

function BatchWorkspace({
  cities,
  selectedCity,
  setSelectedCity,
  batchLens,
  setBatchLens,
  batchDashboard,
  batchComparison,
  batchRoutes,
  selectedBatchRouteId,
  setSelectedBatchRouteId,
  batchRouteDetail,
  batchLoading,
  batchRouteLoading,
  batchError,
  batchMapViewState,
  setBatchMapViewState,
}) {
  const topStopsForLens =
    batchLens === "poi"
      ? batchDashboard?.top_stops_by_poi ?? []
      : batchDashboard?.top_stops_by_activity ?? [];
  const lensMaxValue = topStopsForLens.reduce((currentMax, row) => Math.max(currentMax, rowValue(row, batchLens)), 0);
  const batchLayer = new ScatterplotLayer({
    id: "batch-stops",
    data: topStopsForLens.slice(0, 250),
    getPosition: (row) => [row.stop_lon, row.stop_lat],
    getFillColor: (row) => batchStopColor(row, batchLens, lensMaxValue),
    getLineColor: [10, 20, 30, 150],
    getRadius: (row) => batchStopRadius(row, batchLens, lensMaxValue),
    radiusMinPixels: 4,
    radiusMaxPixels: 22,
    pickable: true,
    stroked: true,
    filled: true,
    opacity: 0.88,
  });
  const routeStopLayer = new ScatterplotLayer({
    id: "route-stops",
    data: batchRouteDetail?.stops ?? [],
    getPosition: (row) => [row.stop_lon, row.stop_lat],
    getFillColor: [248, 250, 252, 180],
    getLineColor: [15, 23, 42, 255],
    getRadius: 54,
    radiusMinPixels: 5,
    radiusMaxPixels: 15,
    pickable: true,
    stroked: true,
    filled: true,
  });
  const routePathLayer = new PathLayer({
    id: "route-paths",
    data: batchRouteDetail?.paths ?? [],
    getPath: (row) => row.path,
    getColor: [255, 196, 61, 220],
    getWidth: 5,
    widthMinPixels: 2,
    widthMaxPixels: 8,
    pickable: false,
  });

  const selectedRoute = batchRoutes.find((route) => route.route_id === selectedBatchRouteId) ?? null;
  const mapLayers = [batchLayer];
  if ((batchRouteDetail?.paths ?? []).length > 0) {
    mapLayers.push(routePathLayer);
  }
  if ((batchRouteDetail?.stops ?? []).length > 0) {
    mapLayers.push(routeStopLayer);
  }

  return (
    <section className="workspace batch-workspace">
      <aside className="control-panel">
        <div className="panel-block">
          <label htmlFor="city-select-batch">City</label>
          <select
            id="city-select-batch"
            value={selectedCity}
            onChange={(event) => setSelectedCity(event.target.value)}
          >
            {cities.map((city) => (
              <option key={city.slug} value={city.slug}>
                {city.display_name}
              </option>
            ))}
          </select>
        </div>

        <div className="panel-block">
          <label htmlFor="lens-select">Atlas lens</label>
          <select
            id="lens-select"
            value={batchLens}
            onChange={(event) => setBatchLens(event.target.value)}
          >
            <option value="activity">Scheduled activity hotspots</option>
            <option value="poi">Amenity access hotspots</option>
          </select>
        </div>

        <div className="panel-block">
          <label htmlFor="route-spotlight">Route spotlight</label>
          <select
            id="route-spotlight"
            value={selectedBatchRouteId}
            onChange={(event) => setSelectedBatchRouteId(event.target.value)}
          >
            <option value="">Select a route</option>
            {batchRoutes.map((route) => (
              <option key={route.route_id} value={route.route_id}>
                {routeLabel(route)}
              </option>
            ))}
          </select>
        </div>

        <div className="stats-grid">
          <StatCard label="Stops in batch graph" value={formatNumber(batchDashboard?.overview?.total_stops)} accent="warm" />
          <StatCard label="Routes modeled" value={formatNumber(batchDashboard?.overview?.total_routes)} accent="cool" />
          <StatCard label="Scheduled stop events" value={formatCompact(batchDashboard?.overview?.total_stop_events)} accent="gold" />
          <StatCard label="Avg POIs per stop" value={Number(batchDashboard?.overview?.avg_poi_access_per_stop || 0).toFixed(1)} accent="green" />
        </div>

        <div className="panel-block">
          <h2>Snapshot</h2>
          <div className="snapshot-stack">
            <div className="snapshot-card">
              <span>Busiest stop</span>
              <strong>{batchDashboard?.overview?.busiest_stop?.stop_name || "Loading"}</strong>
              <small>{formatCompact(batchDashboard?.overview?.busiest_stop?.trip_count)} stop events</small>
            </div>
            <div className="snapshot-card">
              <span>Most connected route</span>
              <strong>{routeLabel(batchDashboard?.overview?.busiest_route)}</strong>
              <small>{formatCompact(batchDashboard?.overview?.busiest_route?.stop_event_count)} stop events</small>
            </div>
            <div className="snapshot-card">
              <span>POI leader</span>
              <strong>{batchDashboard?.overview?.poi_leader?.stop_name || "Loading"}</strong>
              <small>{formatNumber(batchDashboard?.overview?.poi_leader?.poi_count_within_400m)} amenities within 400m</small>
            </div>
          </div>
        </div>

        {batchError ? <div className="error-banner">{batchError}</div> : null}
        {batchLoading ? <div className="status-banner">Loading batch snapshot from Snowflake…</div> : null}
      </aside>

      <main className="map-panel">
        <div className="map-frame batch-frame">
          <DeckGL
            viewState={batchMapViewState}
            onViewStateChange={({ viewState }) => setBatchMapViewState(viewState)}
            controller={true}
            layers={mapLayers}
            getTooltip={({ object }) =>
              object
                ? {
                    html: `
                      <div class="tooltip-title">${object.stop_name || object.route_long_name || object.shape_id || "Batch feature"}</div>
                      ${object.trip_count ? `<div>Scheduled stop events: ${formatNumber(object.trip_count)}</div>` : ""}
                      ${object.poi_count_within_400m ? `<div>Amenities within 400m: ${formatNumber(object.poi_count_within_400m)}</div>` : ""}
                      ${object.food_poi_count_within_400m ? `<div>Food nearby: ${formatNumber(object.food_poi_count_within_400m)}</div>` : ""}
                    `,
                  }
                : null
            }
          >
            <Map reuseMaps={true} mapLib={maplibregl} mapStyle={MAP_STYLE} attributionControl={true} />
          </DeckGL>
          <div className="map-overlay-card">
            <p className="eyebrow">Snowflake-backed batch atlas</p>
            <h2>{batchLens === "poi" ? "Amenity access hot spots" : "Scheduled activity hot spots"}</h2>
            <p>
              {batchLens === "poi"
                ? "Stops are scaled by nearby amenities, with the selected route drawn as a corridor overlay."
                : "Stops are scaled by scheduled stop events so you can see where transit intensity concentrates."}
            </p>
            <div className="map-meta">
              <span>Batch refresh</span>
              <strong>{formatTime(batchDashboard?.generated_at)}</strong>
            </div>
          </div>
        </div>

        <div className="insight-grid">
          <InsightTable
            title="Top stops by scheduled activity"
            caption="Where scheduled service intensity peaks."
            rows={(batchDashboard?.top_stops_by_activity ?? []).slice(0, 10)}
            columns={[
              { key: "stop_name", label: "Stop" },
              { key: "trip_count", label: "Stop events", render: (row) => formatNumber(row.trip_count) },
              { key: "poi_count_within_400m", label: "POIs nearby", render: (row) => formatNumber(row.poi_count_within_400m || 0) },
            ]}
          />

          <InsightTable
            title="Top stops by amenity access"
            caption="Stops with the richest surrounding urban context."
            rows={(batchDashboard?.top_stops_by_poi ?? []).slice(0, 10)}
            columns={[
              { key: "stop_name", label: "Stop" },
              { key: "poi_count_within_400m", label: "POIs / 400m", render: (row) => formatNumber(row.poi_count_within_400m) },
              { key: "food_poi_count_within_400m", label: "Food nearby", render: (row) => formatNumber(row.food_poi_count_within_400m || 0) },
            ]}
          />

          <InsightTable
            title="Most active routes"
            caption="Routes ranked by total scheduled stop events."
            rows={(batchDashboard?.top_routes_by_activity ?? []).slice(0, 10)}
            columns={[
              { key: "route_short_name", label: "Route", render: (row) => row.route_short_name || row.route_id },
              { key: "stop_event_count", label: "Stop events", render: (row) => formatNumber(row.stop_event_count) },
              { key: "distinct_stop_count", label: "Stops", render: (row) => formatNumber(row.distinct_stop_count) },
            ]}
          />

          <InsightTable
            title="Routes with the strongest POI reach"
            caption="Routes whose stop footprint touches the most urban amenities."
            rows={(batchDashboard?.top_routes_by_poi ?? []).slice(0, 10)}
            columns={[
              { key: "route_short_name", label: "Route", render: (row) => row.route_short_name || row.route_id },
              { key: "total_poi_access", label: "Total POI reach", render: (row) => formatNumber(row.total_poi_access) },
              { key: "avg_poi_access_per_stop", label: "Avg / stop", render: (row) => Number(row.avg_poi_access_per_stop || 0).toFixed(1) },
            ]}
          />

          <CoverageCard rows={batchDashboard?.road_coverage ?? []} />

          <section className="insight-card route-card">
            <div className="insight-header">
              <h3>Route spotlight</h3>
              <p>{selectedRoute ? routeLabel(selectedRoute) : "Choose a route to inspect corridor shape and stop context."}</p>
            </div>
            {batchRouteLoading ? (
              <div className="status-banner">Loading route corridor…</div>
            ) : batchRouteDetail?.summary ? (
              <>
                <div className="route-summary-grid">
                  <div>
                    <span>Stop events</span>
                    <strong>{formatCompact(batchRouteDetail.summary.stop_event_count)}</strong>
                  </div>
                  <div>
                    <span>Distinct trips</span>
                    <strong>{formatNumber(batchRouteDetail.summary.distinct_trip_count)}</strong>
                  </div>
                  <div>
                    <span>Total POI reach</span>
                    <strong>{formatNumber(batchRouteDetail.summary.total_poi_access || 0)}</strong>
                  </div>
                  <div>
                    <span>Avg POIs / stop</span>
                    <strong>{Number(batchRouteDetail.summary.avg_poi_access_per_stop || 0).toFixed(1)}</strong>
                  </div>
                </div>
                <div className="route-stop-list">
                  {(batchRouteDetail.stops ?? []).slice(0, 8).map((stop) => (
                    <div className="route-stop-row" key={`${stop.route_id}-${stop.stop_id}`}>
                      <div>
                        <strong>{stop.stop_name}</strong>
                        <span>{formatNumber(stop.trip_count)} stop events</span>
                      </div>
                      <div className="route-stop-badges">
                        <span>{formatNumber(stop.poi_count_within_400m || 0)} POIs</span>
                        <span>{formatNumber(stop.food_poi_count_within_400m || 0)} food</span>
                      </div>
                    </div>
                  ))}
                </div>
              </>
            ) : (
              <div className="status-banner">No route selected yet.</div>
            )}
          </section>

          <section className="insight-card comparison-card-shell">
            <div className="insight-header">
              <h3>Boston vs Chicago</h3>
              <p>Quick cross-city comparison using the same GTFS + OSM metrics.</p>
            </div>
            <div className="comparison-shell">
              {(batchComparison?.cities ?? []).map((city) => (
                <ComparisonCard key={city.city} city={city} />
              ))}
            </div>
          </section>
        </div>
      </main>
    </section>
  );
}

function App() {
  const [mode, setMode] = useState("live");
  const [cities, setCities] = useState([]);
  const [batchCities, setBatchCities] = useState([]);
  const [selectedCity, setSelectedCity] = useState("boston");

  const [vehicleMap, setVehicleMap] = useState({});
  const [routeFilter, setRouteFilter] = useState("");
  const [health, setHealth] = useState(null);
  const [connectionState, setConnectionState] = useState("connecting");
  const [errorMessage, setErrorMessage] = useState("");
  const [liveMapViewState, setLiveMapViewState] = useState(cityViewState(null));

  const [batchLens, setBatchLens] = useState("activity");
  const [batchDashboard, setBatchDashboard] = useState(null);
  const [batchComparison, setBatchComparison] = useState(null);
  const [batchRoutes, setBatchRoutes] = useState([]);
  const [batchDashboardCache, setBatchDashboardCache] = useState({});
  const [batchRoutesCache, setBatchRoutesCache] = useState({});
  const [batchRouteDetailCache, setBatchRouteDetailCache] = useState({});
  const [selectedBatchRouteId, setSelectedBatchRouteId] = useState("");
  const [batchRouteDetail, setBatchRouteDetail] = useState(null);
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchRouteLoading, setBatchRouteLoading] = useState(false);
  const [batchError, setBatchError] = useState("");
  const [batchMapViewState, setBatchMapViewState] = useState(cityViewState(null));

  useEffect(() => {
    async function loadCities() {
      const [liveResponse, batchResponse] = await Promise.all([
        fetch(`${API_BASE_URL}/api/live/cities`),
        fetch(`${API_BASE_URL}/api/batch/cities`),
      ]);
      if (!liveResponse.ok) {
        throw new Error("Unable to load live cities");
      }
      if (!batchResponse.ok) {
        throw new Error("Unable to load batch cities");
      }
      const [livePayload, batchPayload] = await Promise.all([
        liveResponse.json(),
        batchResponse.json(),
      ]);
      startTransition(() => {
        setCities(livePayload);
        setBatchCities(batchPayload);
      });
    }

    loadCities().catch((error) => {
      setErrorMessage(error.message);
      setBatchError(error.message);
    });
  }, []);

  useEffect(() => {
    const cityConfig = cities.find((city) => city.slug === selectedCity);
    setLiveMapViewState(cityViewState(cityConfig));
  }, [selectedCity, cities]);

  useEffect(() => {
    const cityConfig =
      cities.find((city) => city.slug === selectedCity) ||
      batchCities.find((city) => city.slug === selectedCity);
    setBatchMapViewState(cityViewState(cityConfig));
  }, [selectedCity, cities, batchCities]);

  useEffect(() => {
    if (mode !== "live") {
      return undefined;
    }

    let cancelled = false;

    async function loadSnapshot() {
      setErrorMessage("");
      const [vehiclesResponse, healthResponse] = await Promise.all([
        fetch(`${API_BASE_URL}/api/live/${selectedCity}/vehicles`),
        fetch(`${API_BASE_URL}/api/live/${selectedCity}/health`),
      ]);
      if (!vehiclesResponse.ok) {
        throw new Error("Unable to load live snapshot");
      }
      const vehiclePayload = await vehiclesResponse.json();
      const nextVehicleMap = {};
      for (const vehicle of vehiclePayload.vehicles) {
        nextVehicleMap[vehicle.vehicle_id] = vehicle;
      }
      const healthPayload = healthResponse.ok ? await healthResponse.json() : null;
      if (!cancelled) {
        startTransition(() => {
          setVehicleMap(nextVehicleMap);
          setHealth(healthPayload);
          setRouteFilter("");
        });
      }
    }

    loadSnapshot().catch((error) => {
      if (!cancelled) {
        setErrorMessage(error.message);
      }
    });

    return () => {
      cancelled = true;
    };
  }, [mode, selectedCity]);

  useEffect(() => {
    if (mode !== "live") {
      return undefined;
    }

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
  }, [mode, selectedCity]);

  useEffect(() => {
    if (mode !== "batch") {
      return undefined;
    }

    let cancelled = false;

    async function loadBatchData() {
      const cachedDashboard = batchDashboardCache[selectedCity];
      const cachedRoutes = batchRoutesCache[selectedCity];
      const needsDashboard = !cachedDashboard;
      const needsRoutes = !cachedRoutes;
      const needsComparison = !batchComparison;

      if (!needsDashboard && !needsRoutes && !needsComparison) {
        startTransition(() => {
          setBatchError("");
          setBatchDashboard(cachedDashboard);
          setBatchRoutes(cachedRoutes);
          setSelectedBatchRouteId((current) => {
            if (current && cachedRoutes.some((route) => route.route_id === current)) {
              return current;
            }
            return cachedRoutes[0]?.route_id || "";
          });
        });
        return;
      }

      setBatchLoading(true);
      setBatchError("");
      const [dashboardPayload, routesPayload, comparisonPayload] = await Promise.all([
        needsDashboard
          ? fetch(`${API_BASE_URL}/api/batch/${selectedCity}/dashboard`).then((response) => {
              if (!response.ok) {
                throw new Error("Unable to load batch dashboard");
              }
              return response.json();
            })
          : Promise.resolve(cachedDashboard),
        needsRoutes
          ? fetch(`${API_BASE_URL}/api/batch/${selectedCity}/routes?limit=500`).then((response) => {
              if (!response.ok) {
                throw new Error("Unable to load route catalog");
              }
              return response.json();
            })
          : Promise.resolve({ routes: cachedRoutes }),
        needsComparison
          ? fetch(`${API_BASE_URL}/api/batch/comparison`).then((response) => {
              if (!response.ok) {
                throw new Error("Unable to load city comparison");
              }
              return response.json();
            })
          : Promise.resolve(batchComparison),
      ]);

      if (!cancelled) {
        startTransition(() => {
          setBatchDashboard(dashboardPayload);
          setBatchRoutes(routesPayload.routes);
          if (needsDashboard) {
            setBatchDashboardCache((current) => ({ ...current, [selectedCity]: dashboardPayload }));
          }
          if (needsRoutes) {
            setBatchRoutesCache((current) => ({ ...current, [selectedCity]: routesPayload.routes }));
          }
          if (needsComparison) {
            setBatchComparison(comparisonPayload);
          }
          setSelectedBatchRouteId((current) => {
            if (current && routesPayload.routes.some((route) => route.route_id === current)) {
              return current;
            }
            return routesPayload.routes[0]?.route_id || "";
          });
        });
      }
    }

    loadBatchData()
      .catch((error) => {
        if (!cancelled) {
          setBatchError(error.message);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setBatchLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [mode, selectedCity]);

  useEffect(() => {
    if (mode !== "batch" || !selectedBatchRouteId) {
      return undefined;
    }

    let cancelled = false;
    const routeCacheKey = `${selectedCity}:${selectedBatchRouteId}`;
    const cachedRouteDetail = batchRouteDetailCache[routeCacheKey];

    if (cachedRouteDetail) {
      startTransition(() => {
        setBatchRouteDetail(cachedRouteDetail);
        setBatchError("");
      });
      return undefined;
    }

    async function loadRouteDetail() {
      setBatchRouteLoading(true);
      setBatchRouteDetail(null);
      const response = await fetch(`${API_BASE_URL}/api/batch/${selectedCity}/routes/${selectedBatchRouteId}`);
      if (!response.ok) {
        throw new Error("Unable to load route detail");
      }
      const payload = await response.json();
      if (!cancelled) {
        startTransition(() => {
          setBatchRouteDetail(payload);
          setBatchRouteDetailCache((current) => ({ ...current, [routeCacheKey]: payload }));
        });
      }
    }

    loadRouteDetail()
      .catch((error) => {
        if (!cancelled) {
          setBatchError(error.message);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setBatchRouteLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [mode, selectedCity, selectedBatchRouteId, batchRouteDetailCache]);

  const vehicles = Object.values(vehicleMap);
  const batchHeroCity = batchCities.find((city) => city.slug === selectedCity) || cities.find((city) => city.slug === selectedCity);

  return (
    <div className="app-shell">
      <section className="hero-panel">
        <div>
          <ModeToggle mode={mode} setMode={setMode} />
          <p className="eyebrow">{mode === "live" ? "Realtime Transit Command View" : "Snowflake Batch Atlas"}</p>
          <h1>
            {mode === "live"
              ? "Live transit across Boston and Chicago"
              : `Batch transit intelligence for ${batchHeroCity?.display_name || "both cities"}`}
          </h1>
          <p className="hero-copy">
            {mode === "live"
              ? "Track vehicles on a live basemap, switch cities instantly, and keep the streaming story front and center for the presentation."
              : "Explore GTFS + OSM analytics through a warehouse-backed map experience: busiest stops, route intensity, amenity access, corridor context, and city comparison in one place."}
          </p>
        </div>
        <div className={`connection-pill ${mode === "live" ? `connection-${connectionState}` : "connection-batch"}`}>
          <span className="pill-dot" />
          {mode === "live"
            ? connectionState === "live"
              ? "Live stream connected"
              : connectionState
            : batchLoading
              ? "Refreshing warehouse snapshot"
              : "Batch snapshot ready"}
        </div>
      </section>

      {mode === "live" ? (
        <LiveWorkspace
          cities={cities}
          selectedCity={selectedCity}
          setSelectedCity={setSelectedCity}
          routeFilter={routeFilter}
          setRouteFilter={setRouteFilter}
          health={health}
          connectionState={connectionState}
          errorMessage={errorMessage}
          mapViewState={liveMapViewState}
          setMapViewState={setLiveMapViewState}
          vehicles={vehicles}
        />
      ) : (
        <BatchWorkspace
          cities={batchCities}
          selectedCity={selectedCity}
          setSelectedCity={setSelectedCity}
          batchLens={batchLens}
          setBatchLens={setBatchLens}
          batchDashboard={batchDashboard}
          batchComparison={batchComparison}
          batchRoutes={batchRoutes}
          selectedBatchRouteId={selectedBatchRouteId}
          setSelectedBatchRouteId={setSelectedBatchRouteId}
          batchRouteDetail={batchRouteDetail}
          batchLoading={batchLoading}
          batchRouteLoading={batchRouteLoading}
          batchError={batchError}
          batchMapViewState={batchMapViewState}
          setBatchMapViewState={setBatchMapViewState}
        />
      )}
    </div>
  );
}

export default App;
