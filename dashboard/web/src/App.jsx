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

const BATCH_ACTIVITY_SCALE = ["#67b7ff", "#56d39d", "#f7c948", "#f97316"];
const BATCH_ACCESS_SCALE = ["#7dd3fc", "#38bdf8", "#34d399", "#16a34a"];
const AMENITY_GROUP_COLORS = {
  food: "#f97316",
  education: "#8b5cf6",
  healthcare: "#ef4444",
  grocery: "#14b8a6",
  civic: "#60a5fa",
  park: "#22c55e",
  entertainment: "#f59e0b",
  other: "#94a3b8",
};
const ROUTE_MODE_COLORS = {
  0: "#f59e0b",
  1: "#ef4444",
  2: "#60a5fa",
  3: "#22c55e",
  4: "#14b8a6",
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

function routeViewStateFromDetail(detail, fallbackCity) {
  const stopPoints = (detail?.stops ?? [])
    .filter((stop) => Number.isFinite(stop.stop_lat) && Number.isFinite(stop.stop_lon))
    .map((stop) => [Number(stop.stop_lon), Number(stop.stop_lat)]);

  const pathPoints = (detail?.paths ?? []).flatMap((path) => path.path || []);
  const allPoints = [...pathPoints, ...stopPoints].filter(
    (point) => Array.isArray(point) && Number.isFinite(point[0]) && Number.isFinite(point[1]),
  );

  if (allPoints.length === 0) {
    return cityViewState(fallbackCity);
  }

  const lons = allPoints.map((point) => point[0]);
  const lats = allPoints.map((point) => point[1]);
  const minLon = Math.min(...lons);
  const maxLon = Math.max(...lons);
  const minLat = Math.min(...lats);
  const maxLat = Math.max(...lats);
  const span = Math.max(maxLon - minLon, maxLat - minLat);

  let zoom = 12.3;
  if (span > 0.7) zoom = 9.6;
  else if (span > 0.45) zoom = 10.3;
  else if (span > 0.2) zoom = 11;
  else if (span > 0.08) zoom = 11.8;
  else if (span > 0.03) zoom = 12.8;

  return {
    latitude: (minLat + maxLat) / 2,
    longitude: (minLon + maxLon) / 2,
    zoom,
    bearing: 0,
    pitch: 30,
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

function formatPercent(value) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) {
    return "0%";
  }
  return `${numeric.toFixed(0)}%`;
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

function amenityGroupLabel(group) {
  const labels = {
    food: "Food and cafes",
    education: "Education",
    healthcare: "Healthcare",
    grocery: "Groceries",
    civic: "Civic places",
    park: "Parks",
    entertainment: "Entertainment",
    other: "Other amenities",
  };
  return labels[group] || group || "Other amenities";
}

function batchLensTitle(lens) {
  return lens === "access" ? "Neighborhood access" : "Service intensity";
}

function batchLensDescription(lens) {
  return lens === "access"
    ? "Highlights stops with the richest mix of daily destinations within a short walk."
    : "Highlights where scheduled service concentrates most heavily across the network.";
}

function buildConicGradient(rows, valueKey, colorForRow) {
  const total = rows.reduce((sum, row) => sum + Number(row[valueKey] || 0), 0);
  if (!total) {
    return "conic-gradient(#334155 0turn, #334155 1turn)";
  }

  let start = 0;
  const segments = rows.map((row) => {
    const portion = Number(row[valueKey] || 0) / total;
    const end = start + portion;
    const color = colorForRow(row);
    const segment = `${color} ${start}turn ${end}turn`;
    start = end;
    return segment;
  });
  return `conic-gradient(${segments.join(", ")})`;
}

function batchStopColor(stop, lens, maxValue) {
  const score = lens === "access" ? (stop.poi_count_within_400m ?? 0) : (stop.trip_count ?? 0);
  const ratio = maxValue > 0 ? score / maxValue : 0;

  if (lens === "access") {
    if (ratio > 0.75) return [22, 163, 74, 235];
    if (ratio > 0.45) return [52, 211, 153, 230];
    if (ratio > 0.2) return [56, 189, 248, 220];
    return [125, 211, 252, 210];
  }

  if (ratio > 0.75) return [244, 114, 54, 235];
  if (ratio > 0.45) return [251, 191, 36, 225];
  if (ratio > 0.2) return [52, 211, 153, 220];
  return [96, 165, 250, 210];
}

function batchStopRadius(stop, lens, maxValue) {
  const score = lens === "access" ? (stop.poi_count_within_400m ?? 0) : (stop.trip_count ?? 0);
  const ratio = maxValue > 0 ? score / maxValue : 0;
  return Math.max(42, Math.min(180, 42 + ratio * 120));
}

function rowValue(row, lens) {
  return lens === "access" ? row.poi_count_within_400m ?? 0 : row.trip_count ?? 0;
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

function LegendCard({ title, caption, items }) {
  return (
    <section className="insight-card">
      <div className="insight-header">
        <h3>{title}</h3>
        <p>{caption}</p>
      </div>
      <div className="legend-grid">
        {items.map((item) => (
          <div className="legend-item" key={item.label}>
            <span className="legend-swatch" style={{ background: item.color }} />
            <div>
              <strong>{item.label}</strong>
              <small>{item.description}</small>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function BarChartCard({ title, caption, rows, labelKey, valueKey, formatLabel, formatValue, color }) {
  const maxValue = rows.reduce((currentMax, row) => Math.max(currentMax, Number(row[valueKey] || 0)), 0);

  return (
    <section className="insight-card">
      <div className="insight-header">
        <h3>{title}</h3>
        <p>{caption}</p>
      </div>
      <div className="bar-chart">
        {rows.map((row, index) => {
          const value = Number(row[valueKey] || 0);
          const width = maxValue > 0 ? (value / maxValue) * 100 : 0;
          return (
            <div className="bar-row" key={`${title}-${index}-${row.stop_id || row.route_id || row.poi_group || row.route_type}`}>
              <div className="bar-copy">
                <strong>{formatLabel ? formatLabel(row[labelKey], row) : row[labelKey]}</strong>
                <span>{formatValue ? formatValue(value, row) : formatNumber(value)}</span>
              </div>
              <div className="bar-track">
                <div className="bar-fill" style={{ width: `${width}%`, background: color(row) }} />
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function DonutBreakdownCard({ title, caption, rows, valueKey, labelForRow, colorForRow }) {
  const total = rows.reduce((sum, row) => sum + Number(row[valueKey] || 0), 0);
  const chartBackground = buildConicGradient(rows, valueKey, colorForRow);

  return (
    <section className="insight-card donut-card">
      <div className="insight-header">
        <h3>{title}</h3>
        <p>{caption}</p>
      </div>
      <div className="donut-layout">
        <div className="donut-shell" style={{ background: chartBackground }}>
          <div className="donut-center">
            <span>Total</span>
            <strong>{formatCompact(total)}</strong>
          </div>
        </div>
        <div className="donut-legend">
          {rows.map((row) => (
            <div className="donut-row" key={`${title}-${labelForRow(row)}`}>
              <span className="legend-swatch" style={{ background: colorForRow(row) }} />
              <div>
                <strong>{labelForRow(row)}</strong>
                <small>{formatNumber(row[valueKey])}</small>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function RouteSpotlightMap({ detail, fallbackCity }) {
  const routeMapViewState = routeViewStateFromDetail(detail, fallbackCity);
  const pathLayer = new PathLayer({
    id: "route-spotlight-paths",
    data: detail?.paths ?? [],
    getPath: (row) => row.path,
    getColor: [255, 196, 61, 235],
    getWidth: 6,
    widthMinPixels: 3,
    widthMaxPixels: 8,
    pickable: false,
  });
  const stopLayer = new ScatterplotLayer({
    id: "route-spotlight-stops",
    data: detail?.stops ?? [],
    getPosition: (row) => [row.stop_lon, row.stop_lat],
    getFillColor: (row) => (row.poi_count_within_400m ? [125, 211, 252, 220] : [248, 250, 252, 200]),
    getLineColor: [15, 23, 42, 255],
    getRadius: (row) => Math.max(40, Math.min(120, 28 + Math.sqrt(Number(row.trip_count || 0)))),
    radiusMinPixels: 5,
    radiusMaxPixels: 16,
    pickable: true,
    stroked: true,
    filled: true,
  });

  return (
    <div className="route-map-shell">
      <DeckGL
        viewState={routeMapViewState}
        controller={true}
        layers={[pathLayer, stopLayer]}
        getTooltip={({ object }) =>
          object
            ? {
                html: `
                  <div class="tooltip-title">${object.stop_name || "Route corridor"}</div>
                  ${object.trip_count ? `<div>Scheduled stop events: ${formatNumber(object.trip_count)}</div>` : ""}
                  ${object.poi_count_within_400m ? `<div>Nearby amenities: ${formatNumber(object.poi_count_within_400m)}</div>` : ""}
                `,
              }
            : null
        }
      >
        <Map reuseMaps={true} mapLib={maplibregl} mapStyle={MAP_STYLE} attributionControl={false} />
      </DeckGL>
    </div>
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
          <span>Avg amenities / stop</span>
          <strong>{Number(city.avg_poi_access_per_stop || 0).toFixed(1)}</strong>
        </div>
        <div>
          <span>Stops with access context</span>
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
  cityMetadata,
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
  batchRoutePreview,
  batchLoading,
  batchRouteLoading,
  batchError,
  batchMapViewState,
  setBatchMapViewState,
}) {
  const topStopsForLens =
    batchLens === "access"
      ? batchDashboard?.top_stops_by_poi ?? []
      : batchDashboard?.top_stops_by_activity ?? [];
  const topRoutesForLens =
    batchLens === "access"
      ? batchDashboard?.top_routes_by_poi ?? []
      : batchDashboard?.top_routes_by_activity ?? [];
  const amenityMix = (batchDashboard?.amenity_mix ?? []).slice(0, 6);
  const routeModeMix = (batchDashboard?.route_mode_mix ?? []).slice(0, 5);
  const topAmenityStops = (batchDashboard?.top_stops_by_poi ?? []).slice(0, 8);
  const topActivityStops = (batchDashboard?.top_stops_by_activity ?? []).slice(0, 8);
  const lensLegend =
    batchLens === "access"
      ? [
          { color: BATCH_ACCESS_SCALE[0], label: "Lower access", description: "Fewer nearby daily destinations around the stop." },
          { color: BATCH_ACCESS_SCALE[2], label: "Balanced access", description: "A healthy mix of food, services, and everyday destinations." },
          { color: BATCH_ACCESS_SCALE[3], label: "Rich access", description: "Dense clusters of amenities within a short walk." },
        ]
      : [
          { color: BATCH_ACTIVITY_SCALE[0], label: "Lower service", description: "Stops with lighter scheduled intensity." },
          { color: BATCH_ACTIVITY_SCALE[2], label: "Busy corridor", description: "Strong scheduled frequency and route overlap." },
          { color: BATCH_ACTIVITY_SCALE[3], label: "Network hotspot", description: "The highest service concentration in the city." },
        ];
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
    data: batchRoutePreview?.stops ?? batchRouteDetail?.stops ?? [],
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
  const mapLayers = selectedRoute ? [] : [batchLayer];
  if ((batchRouteDetail?.paths ?? []).length > 0) {
    mapLayers.push(routePathLayer);
  }
  if (((batchRoutePreview?.stops ?? batchRouteDetail?.stops) ?? []).length > 0) {
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
            <option value="activity">Service intensity</option>
            <option value="access">Neighborhood access</option>
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
          <StatCard label="Avg amenities per stop" value={Number(batchDashboard?.overview?.avg_poi_access_per_stop || 0).toFixed(1)} accent="green" />
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
              <span>Top amenity stop</span>
              <strong>{batchDashboard?.overview?.poi_leader?.stop_name || "Loading"}</strong>
              <small>{formatNumber(batchDashboard?.overview?.poi_leader?.poi_count_within_400m)} amenities within 400m</small>
            </div>
          </div>
        </div>

        {batchError ? <div className="error-banner">{batchError}</div> : null}
        {batchLoading ? <div className="status-banner">Refreshing the daily batch snapshot…</div> : null}
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
                      ${object.poi_count_within_400m ? `<div>Nearby amenities: ${formatNumber(object.poi_count_within_400m)}</div>` : ""}
                      ${object.food_poi_count_within_400m ? `<div>Food access: ${formatNumber(object.food_poi_count_within_400m)}</div>` : ""}
                    `,
                  }
                : null
            }
          >
            <Map reuseMaps={true} mapLib={maplibregl} mapStyle={MAP_STYLE} attributionControl={true} />
          </DeckGL>
          <div className="map-overlay-card">
            <p className="eyebrow">Snowflake-backed batch atlas</p>
            <h2>{batchLensTitle(batchLens)}</h2>
            <p>
              {selectedRoute
                ? `${batchLensDescription(batchLens)} The highlighted corridor shows how ${routeLabel(selectedRoute)} threads through that urban context.`
                : batchLensDescription(batchLens)}
            </p>
            <div className="map-meta">
              <span>Batch refresh</span>
              <strong>{formatTime(batchDashboard?.generated_at)}</strong>
            </div>
            <div className="map-meta">
              <span>Current route spotlight</span>
              <strong>{selectedRoute ? routeLabel(selectedRoute) : "Choose a route"}</strong>
            </div>
          </div>
        </div>

        <div className="insight-grid">
          <LegendCard
            title={`${batchLensTitle(batchLens)} legend`}
            caption="Color and marker size now have one meaning at a time, and this legend stays aligned with the active lens."
            items={lensLegend}
          />

          <BarChartCard
            title="Top stops by service intensity"
            caption="The busiest stops by scheduled stop events in the current city."
            rows={topActivityStops}
            labelKey="stop_name"
            valueKey="trip_count"
            formatValue={(value) => `${formatCompact(value)} stop events`}
            color={() => BATCH_ACTIVITY_SCALE[3]}
          />

          <BarChartCard
            title="Top stops by neighborhood access"
            caption="Stops with the strongest nearby mix of destinations and daily essentials."
            rows={topAmenityStops}
            labelKey="stop_name"
            valueKey="poi_count_within_400m"
            formatValue={(value, row) => `${formatNumber(value)} amenities, ${formatNumber(row.food_poi_count_within_400m || 0)} food`}
            color={() => BATCH_ACCESS_SCALE[3]}
          />

          <BarChartCard
            title={batchLens === "access" ? "Routes with the strongest neighborhood access" : "Routes with the most service intensity"}
            caption={batchLens === "access"
              ? "Which routes touch the richest urban context across their stop footprint."
              : "Which routes carry the most scheduled stop activity overall."}
            rows={topRoutesForLens.slice(0, 8)}
            labelKey="route_short_name"
            valueKey={batchLens === "access" ? "total_poi_access" : "stop_event_count"}
            formatLabel={(_, row) => row.route_short_name || row.route_id}
            formatValue={(value, row) =>
              batchLens === "access"
                ? `${formatNumber(value)} amenity touches, ${Number(row.avg_poi_access_per_stop || 0).toFixed(1)} avg per stop`
                : `${formatCompact(value)} stop events across ${formatNumber(row.distinct_stop_count)} stops`}
            color={() => batchLens === "access" ? BATCH_ACCESS_SCALE[2] : BATCH_ACTIVITY_SCALE[2]}
          />

          <DonutBreakdownCard
            title="Amenity mix around transit"
            caption="What kinds of nearby places are shaping the batch access story in this city."
            rows={amenityMix}
            valueKey="amenity_count"
            labelForRow={(row) => amenityGroupLabel(row.poi_group)}
            colorForRow={(row) => AMENITY_GROUP_COLORS[row.poi_group] || AMENITY_GROUP_COLORS.other}
          />

          <DonutBreakdownCard
            title="Network mode mix"
            caption="How the city's modeled routes split across transit modes."
            rows={routeModeMix}
            valueKey="route_count"
            labelForRow={(row) => routeTypeLabel(row.route_type)}
            colorForRow={(row) => ROUTE_MODE_COLORS[row.route_type] || ROUTE_MODE_COLORS[3]}
          />

          <CoverageCard rows={batchDashboard?.road_coverage ?? []} />

          <section className="insight-card route-card">
            <div className="insight-header">
              <h3>Route spotlight</h3>
              <p>{selectedRoute ? routeLabel(selectedRoute) : "Choose a route to inspect corridor shape, service intensity, and nearby destinations."}</p>
            </div>
            {batchRouteLoading ? (
              <div className="status-banner">Loading route corridor…</div>
            ) : batchRoutePreview?.summary || batchRouteDetail?.summary ? (
              <>
                <div className="route-summary-grid">
                  <div>
                    <span>Stop events</span>
                    <strong>{formatCompact((batchRouteDetail?.summary || batchRoutePreview?.summary)?.stop_event_count)}</strong>
                  </div>
                  <div>
                    <span>Distinct trips</span>
                    <strong>{formatNumber((batchRouteDetail?.summary || batchRoutePreview?.summary)?.distinct_trip_count)}</strong>
                  </div>
                  <div>
                    <span>Total access score</span>
                    <strong>{formatNumber((batchRouteDetail?.summary || batchRoutePreview?.summary)?.total_poi_access || 0)}</strong>
                  </div>
                  <div>
                    <span>Avg amenities / stop</span>
                    <strong>{Number((batchRouteDetail?.summary || batchRoutePreview?.summary)?.avg_poi_access_per_stop || 0).toFixed(1)}</strong>
                  </div>
                </div>
                <p className="route-story-copy">
                  {selectedRoute
                    ? `${routeLabel(selectedRoute)} blends ${formatCompact((batchRouteDetail?.summary || batchRoutePreview?.summary)?.stop_event_count)} scheduled stop events with ${formatNumber((batchRouteDetail?.summary || batchRoutePreview?.summary)?.total_poi_access || 0)} nearby amenities across its corridor.`
                    : "Select a route to read its corridor story."}
                </p>
                <RouteSpotlightMap detail={batchRouteDetail || batchRoutePreview} fallbackCity={cityMetadata.find((city) => city.slug === selectedCity) || null} />
                <div className="route-analytics-grid">
                  <BarChartCard
                    title="Busiest stops on this route"
                    caption="The stops where this route contributes the most scheduled activity."
                    rows={((batchRouteDetail?.stops ?? batchRoutePreview?.stops) ?? []).slice(0, 8)}
                    labelKey="stop_name"
                    valueKey="trip_count"
                    formatValue={(value) => `${formatNumber(value)} stop events`}
                    color={() => BATCH_ACTIVITY_SCALE[3]}
                  />
                  <BarChartCard
                    title="Best access points on this route"
                    caption="Where this route connects riders to the strongest nearby mix of amenities."
                    rows={[...((batchRouteDetail?.stops ?? batchRoutePreview?.stops) ?? [])]
                      .sort((left, right) => Number(right.poi_count_within_400m || 0) - Number(left.poi_count_within_400m || 0))
                      .slice(0, 8)}
                    labelKey="stop_name"
                    valueKey="poi_count_within_400m"
                    formatValue={(value, row) => `${formatNumber(value)} amenities, ${formatNumber(row.critical_service_poi_count_within_400m || 0)} essential`}
                    color={() => BATCH_ACCESS_SCALE[3]}
                  />
                </div>
                <div className="route-stop-list">
                  {((batchRouteDetail?.stops ?? batchRoutePreview?.stops) ?? []).slice(0, 8).map((stop) => (
                    <div className="route-stop-row" key={`${stop.route_id}-${stop.stop_id}`}>
                      <div>
                        <strong>{stop.stop_name}</strong>
                        <span>{formatNumber(stop.trip_count)} stop events</span>
                      </div>
                      <div className="route-stop-badges">
                        <span>{formatNumber(stop.poi_count_within_400m || 0)} amenities</span>
                        <span>{formatNumber(stop.food_poi_count_within_400m || 0)} food</span>
                        <span>{formatNumber(stop.critical_service_poi_count_within_400m || 0)} essential services</span>
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
              <p>A side-by-side read on service intensity and neighborhood access using the same batch metrics.</p>
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
  const [batchRoutePreviewCache, setBatchRoutePreviewCache] = useState({});
  const [batchRouteDetailCache, setBatchRouteDetailCache] = useState({});
  const [batchBootstrapLoaded, setBatchBootstrapLoaded] = useState(false);
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
        setBatchCities(batchPayload || []);
        setBatchBootstrapLoaded(true);
      });
    }

    loadCities().catch((error) => {
        setErrorMessage(error.message);
        setBatchError(error.message);
        setBatchBootstrapLoaded(true);
      });
  }, []);

  useEffect(() => {
    if (batchCities.length === 0) {
      return undefined;
    }

    let cancelled = false;

    async function warmBatchCaches() {
      const cityTargets = [selectedCity, ...batchCities.map((city) => city.slug)].filter(
        (value, index, array) => value && array.indexOf(value) === index,
      );

      const comparisonPromise = batchComparison
        ? Promise.resolve(batchComparison)
        : fetch(`${API_BASE_URL}/api/batch/comparison`).then((response) => {
            if (!response.ok) {
              throw new Error("Unable to load city comparison");
            }
            return response.json();
          });

      const comparisonPayload = await comparisonPromise;
      const cityPayloads = await Promise.all(
        cityTargets.map(async (citySlug) => {
          const dashboardPromise = batchDashboardCache[citySlug]
            ? Promise.resolve(batchDashboardCache[citySlug])
            : fetch(`${API_BASE_URL}/api/batch/${citySlug}/dashboard`).then((response) => {
                if (!response.ok) {
                  throw new Error(`Unable to load batch dashboard for ${citySlug}`);
                }
                return response.json();
              });
          const routesPromise = batchRoutesCache[citySlug]
            ? Promise.resolve({ routes: batchRoutesCache[citySlug] })
            : fetch(`${API_BASE_URL}/api/batch/${citySlug}/routes?limit=500`).then((response) => {
                if (!response.ok) {
                  throw new Error(`Unable to load route catalog for ${citySlug}`);
                }
                return response.json();
              });

          const [dashboardPayload, routesPayload] = await Promise.all([dashboardPromise, routesPromise]);
          return {
            citySlug,
            dashboardPayload,
            routesPayload: routesPayload.routes,
          };
        }),
      );

      if (!cancelled) {
        const nextDashboardCache = {};
        const nextRoutesCache = {};
        cityPayloads.forEach(({ citySlug, dashboardPayload, routesPayload }) => {
          nextDashboardCache[citySlug] = dashboardPayload;
          nextRoutesCache[citySlug] = routesPayload;
        });

        startTransition(() => {
          setBatchComparison(comparisonPayload);
          setBatchDashboardCache((current) => ({ ...current, ...nextDashboardCache }));
          setBatchRoutesCache((current) => ({ ...current, ...nextRoutesCache }));

          const selectedDashboard = nextDashboardCache[selectedCity] || batchDashboardCache[selectedCity];
          const selectedRoutes = nextRoutesCache[selectedCity] || batchRoutesCache[selectedCity] || [];
          if (selectedDashboard) {
            setBatchDashboard(selectedDashboard);
          }
          setBatchRoutes(selectedRoutes);
          setSelectedBatchRouteId((current) => {
            if (current && selectedRoutes.some((route) => route.route_id === current)) {
              return current;
            }
            return selectedRoutes[0]?.route_id || "";
          });
        });
      }
    }

    warmBatchCaches().catch(() => undefined);

    return () => {
      cancelled = true;
    };
  }, [batchCities]);

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
    if (mode !== "batch" || !selectedBatchRouteId) {
      return;
    }

    const cityConfig = cities.find((city) => city.slug === selectedCity) || null;
    const activeRouteDetail =
      batchRouteDetailCache[`${selectedCity}:${selectedBatchRouteId}`] ||
      batchRoutePreviewCache[selectedCity]?.[selectedBatchRouteId] ||
      batchRouteDetail;

    if (!activeRouteDetail) {
      return;
    }

    startTransition(() => {
      setBatchMapViewState(routeViewStateFromDetail(activeRouteDetail, cityConfig));
    });
  }, [mode, selectedCity, selectedBatchRouteId, batchRouteDetail, batchRouteDetailCache, batchRoutePreviewCache, cities]);

  useEffect(() => {
    const cachedDashboard = batchDashboardCache[selectedCity];
    const cachedRoutes = batchRoutesCache[selectedCity];
    const routePreviewCache = batchRoutePreviewCache[selectedCity] || {};
    if (!cachedDashboard && !cachedRoutes) {
      return;
    }

    startTransition(() => {
      if (cachedDashboard) {
        setBatchDashboard(cachedDashboard);
      }
      if (cachedRoutes) {
        setBatchRoutes(cachedRoutes);
        setSelectedBatchRouteId((current) => {
          if (current && cachedRoutes.some((route) => route.route_id === current)) {
            return current;
          }
          return cachedRoutes[0]?.route_id || "";
        });
        setBatchRouteDetail((current) => {
          const activeRouteId =
            (current?.summary?.route_id && cachedRoutes.some((route) => route.route_id === current.summary.route_id))
              ? current.summary.route_id
              : cachedRoutes[0]?.route_id;
          return activeRouteId ? routePreviewCache[activeRouteId] || current : current;
        });
      }
    });
  }, [selectedCity, batchDashboardCache, batchRoutesCache, batchRoutePreviewCache]);

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
    if (!batchBootstrapLoaded) {
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

      setBatchLoading(mode === "batch");
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
  }, [batchBootstrapLoaded, mode, selectedCity, batchComparison, batchDashboardCache, batchRoutesCache]);

  useEffect(() => {
    if (!selectedBatchRouteId) {
      return undefined;
    }

    let cancelled = false;
    const routeCacheKey = `${selectedCity}:${selectedBatchRouteId}`;
    const previewRouteDetail = batchRoutePreviewCache[selectedCity]?.[selectedBatchRouteId];
    const cachedRouteDetail = batchRouteDetailCache[routeCacheKey];

    if (previewRouteDetail) {
      startTransition(() => {
        setBatchRouteDetail(previewRouteDetail);
        setBatchError("");
      });
    }

    if (cachedRouteDetail && !cachedRouteDetail.is_preview) {
      startTransition(() => {
        setBatchRouteDetail(cachedRouteDetail);
        setBatchError("");
      });
      return undefined;
    }

    async function loadRouteDetail() {
      setBatchRouteLoading(true);
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
  }, [selectedCity, selectedBatchRouteId, batchRouteDetailCache, batchRoutePreviewCache]);

  useEffect(() => {
    if (!batchBootstrapLoaded) {
      return undefined;
    }

    const prefetchTargets = Object.entries(batchRoutesCache).flatMap(([citySlug, routes]) =>
      (routes || [])
        .slice(0, 4)
        .map((route) => ({ citySlug, routeId: route.route_id }))
        .filter(({ citySlug: routeCity, routeId }) => !batchRouteDetailCache[`${routeCity}:${routeId}`]),
    );

    if (prefetchTargets.length === 0) {
      return undefined;
    }

    let cancelled = false;

    async function prefetchRouteDetails() {
      const settled = await Promise.all(
        prefetchTargets.map(async ({ citySlug, routeId }) => {
          const response = await fetch(`${API_BASE_URL}/api/batch/${citySlug}/routes/${routeId}`);
          if (!response.ok) {
            return null;
          }
          const payload = await response.json();
          return { citySlug, routeId, payload };
        }),
      );

      if (!cancelled) {
        const nextCacheEntries = {};
        settled.forEach((item) => {
          if (!item) {
            return;
          }
          nextCacheEntries[`${item.citySlug}:${item.routeId}`] = item.payload;
        });
        if (Object.keys(nextCacheEntries).length > 0) {
          startTransition(() => {
            setBatchRouteDetailCache((current) => ({ ...current, ...nextCacheEntries }));
          });
        }
      }
    }

    prefetchRouteDetails().catch(() => undefined);

    return () => {
      cancelled = true;
    };
  }, [batchBootstrapLoaded, batchRoutesCache, batchRouteDetailCache]);

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
            : !batchBootstrapLoaded || batchLoading
              ? "Staging batch snapshot"
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
          cityMetadata={cities}
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
          batchRoutePreview={batchRoutePreviewCache[selectedCity]?.[selectedBatchRouteId] || null}
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
