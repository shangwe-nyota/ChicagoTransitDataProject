# Live Dashboard Architecture

## Current Boston Live Slice

The new live dashboard path is:

1. MBTA realtime vehicle feed
2. `jobs/realtime/mbta_poll_to_redis.py`
3. Redis latest-state store
4. FastAPI snapshot + websocket layer in `dashboard/live_api.py`
5. React + deck.gl + MapLibre dashboard in `dashboard/web/`

This was chosen to get a presentation-quality live map working quickly while keeping the serving contract stable.

## Why Redis Sits In The Middle

Redis is the latest-state store for live vehicles.

For each city, the backend stores one latest record per vehicle and publishes updates to a city-specific channel:

- `transit:live:boston:vehicle:{vehicle_id}`
- `transit:live:boston:vehicles:index`
- `transit:live:boston:updates`

That design keeps the frontend simple:

- `GET /api/live/{city}/vehicles` for the initial snapshot
- `WS /ws/live/{city}` for pushed updates

## How Kafka + Flink Fit Next

The intended production-style realtime architecture is:

1. GTFS-RT or agency realtime source
2. Kafka ingestion topic
3. Flink job for normalization and latest-state logic
4. Redis latest-state store
5. FastAPI serving layer
6. React + deck.gl dashboard

When Kafka + Flink are added, the frontend and FastAPI contract do not need to change.

The only thing that changes is the upstream writer into Redis:

- today: MBTA poller writes `LiveVehicleState` records directly
- later: Flink output writer writes the same `LiveVehicleState` shape into Redis

## City Modularity

The live layer is city-aware from the start:

- city configs live in `src/live/config.py`
- REST routes are `GET /api/live/{city}/...`
- websocket routes are `WS /ws/live/{city}`
- Redis keys are namespaced by city

Boston is enabled for live now.
Chicago is already reserved in the API and config so it can be added later without reworking the frontend contract.

## Local Commands

Set `LIVE_CITY=boston` in `.env` to keep the default city explicit for local runs.

The simplest way to run the Boston live stack now is:

```bash
bash scripts/live.sh all
```

That starts:

- Redis
- Kafka
- city topics
- the Flink latest-state job
- the Kafka-to-Redis updater
- the MBTA-to-Kafka producer
- the FastAPI backend
- the React dashboard

To stop that full stack:

```bash
bash scripts/live.sh down
```

To check status:

```bash
bash scripts/live.sh status
```

If you want more control, you can still start layers separately:

```bash
bash scripts/live.sh infra
bash scripts/live.sh stream
bash scripts/live.sh app
```

The direct MBTA-to-Redis poller remains available as a fallback when you do not want Kafka + Flink in the loop:

```bash
bash scripts/run_mbta_live_poller.sh
```
