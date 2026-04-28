# Live Architecture

## Current Realtime System

The live dashboard is currently shared across Boston and Chicago.

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

