# Live Architecture

## Current Realtime System

The live dashboard is no longer Boston-only in architecture, even though Boston remains the primary presentation city.

The current shared live serving path is:

1. city-specific agency realtime feed
2. city-specific producer job
3. Kafka raw topic
4. Flink latest-state job
5. Kafka latest topic
6. Redis latest-state store
7. FastAPI snapshot + websocket layer in `dashboard/live_api.py`
8. React + deck.gl + MapLibre dashboard in `dashboard/web/`

The direct fallback path still exists:

1. city-specific agency realtime feed
2. city-specific poller writes directly to Redis
3. FastAPI
4. React dashboard

That fallback exists so a demo can still run if Kafka or Flink has issues.

## City Coverage Right Now

### Boston

Boston is the strongest live city right now.

Implemented pieces:

- MBTA client in `src/live/mbta.py`
- direct poller:
  - `jobs/realtime/mbta_poll_to_redis.py`
- Kafka producer:
  - `jobs/realtime/mbta_poll_to_kafka.py`
- dashboard support through the shared API and frontend

Boston live includes multiple modes when the source feed provides them, such as:

- buses
- subway
- commuter rail
- light rail
- shuttles when MBTA exposes them that way

Important operational note:

- overnight Boston counts can be legitimately small
- observed on `April 19, 2026` around `2:30 AM America/Chicago`:
  - direct MBTA feed returned about `15` vehicles

This means sparse Boston maps late at night do not necessarily indicate a bug.

### Chicago

Chicago is now wired into the same live stack, but with one important limitation.

Implemented pieces:

- CTA client in `src/live/cta.py`
- direct poller:
  - `jobs/realtime/cta_poll_to_redis.py`
- Kafka producer:
  - `jobs/realtime/cta_poll_to_kafka.py`
- dashboard support through the shared API and frontend

What works now:

- CTA bus polling
- normalization into `LiveVehicleState`
- Kafka -> Flink -> Redis -> FastAPI -> React path
- direct Redis fallback path

What is blocked:

- CTA train support depends on a valid train key
- current configured CTA train key returns:
  - `CTA Train Tracker error 101: Invalid API key`

So operationally, Chicago should currently be treated as:

- live buses working
- live trains credential-blocked

## Why Redis Sits In The Middle

Redis is the latest-state store for live vehicles.

For each city, the backend stores:

- one latest record per vehicle
- one index of active vehicle ids
- one metadata hash
- one pub/sub update channel

Example key patterns:

- `transit:live:boston:vehicle:{vehicle_id}`
- `transit:live:boston:vehicles:index`
- `transit:live:boston:metadata`
- `transit:live:boston:updates`

- `transit:live:chicago:vehicle:{vehicle_id}`
- `transit:live:chicago:vehicles:index`
- `transit:live:chicago:metadata`
- `transit:live:chicago:updates`

This design keeps the frontend simple:

- `GET /api/live/{city}/vehicles` for the initial snapshot
- `GET /api/live/{city}/health` for count + freshness checks
- `WS /ws/live/{city}` for pushed updates

Redis is not the historical warehouse. It is the latest-state serving layer.

## Shared Live Contract

The live system is held together by `LiveVehicleState` in `src/live/models.py`.

That contract is shared across:

- MBTA normalization
- CTA normalization
- Kafka payloads
- Flink output payloads
- Redis values
- FastAPI responses
- frontend rendering

That stability is intentional. The serving contract should stay the same even if upstream processing changes.

## Kafka And Flink

Kafka topics are city-aware but follow one simple naming convention:

- raw:
  - `transit.live.raw.{city}.vehicles`
- latest:
  - `transit.live.latest.{city}.vehicles`

Topic naming is centralized in `src/live/topics.py`.

The Flink job in `jobs/realtime/flink_vehicle_latest_job.py`:

- consumes raw vehicle events
- keys by `city + vehicle_id`
- keeps the latest event by timestamp
- emits the latest state to the latest topic

Then `jobs/realtime/kafka_latest_to_redis.py`:

- consumes the latest topic
- validates into `LiveVehicleState`
- upserts into Redis

## Frontend Behavior

The frontend in `dashboard/web/src/App.jsx` now:

- loads supported cities from the API
- lets the user switch between Boston and Chicago
- loads a snapshot from FastAPI
- subscribes to websocket updates
- keeps user zoom and pan stable during live data updates
- uses MapLibre for a real street basemap

Important UX note:

- the city switcher changes the map and the subscribed live feed
- the app can now rely on `bash scripts/live.sh all` to feed both Boston and Chicago at the same time

Chicago status wording was also improved:

- CTA buses now usually show `Reporting live` instead of `Unknown`
- delayed buses show `Delayed`

## Local Runbook

The preferred command is:

```bash
bash scripts/live.sh all
```

This now starts:

- Redis
- Kafka
- topics for Boston and Chicago
- per-city Flink latest-state jobs
- per-city Kafka latest-to-Redis consumers
- per-city upstream producers
- FastAPI
- React dashboard

The useful URLs are:

- UI:
  - `http://127.0.0.1:5173`
- API:
  - `http://127.0.0.1:8000`
- Boston health:
  - `http://127.0.0.1:8000/api/live/boston/health`
- Chicago health:
  - `http://127.0.0.1:8000/api/live/chicago/health`

Other useful commands:

```bash
bash scripts/live.sh status
bash scripts/live.sh logs
bash scripts/live.sh down
```

City-specific start options still exist:

```bash
bash scripts/live.sh all boston
bash scripts/live.sh all chicago
```

## Known Limitations

- Boston overnight feed volume can be low.
- Chicago trains are blocked by credentials.
- Chicago buses will often feel less dramatic than Boston multi-mode traffic because bus motion is smaller and there is no train layer yet.
- the live dashboard is now much stronger than the batch dashboard path; the batch dashboard migration is still future work.

## Immediate Next Work

The agreed next steps after the current live branch are:

1. verify Boston live during daytime when the feed is fuller
2. improve live UI colors and legends
3. build Boston GTFS + OSM batch support
4. finish Chicago GTFS + OSM batch support
5. move older batch visuals into the new dashboard
6. add tests and validation jobs
7. add a presentation runbook
