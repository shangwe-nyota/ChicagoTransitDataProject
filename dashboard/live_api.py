from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from src.batch.service import SnowflakeBatchService
from src.common.config import get_batch_city_config
from src.live.config import CITY_CONFIGS, get_city_config
from src.live.models import LiveCityResponse, LiveVehiclesResponse
from src.live.redis_store import RedisLiveStateStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = RedisLiveStateStore()
    batch_service = SnowflakeBatchService()
    app.state.live_store = store
    app.state.batch_service = batch_service
    yield
    await store.close()


app = FastAPI(
    title="Transit Data Project Live API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_store(app_: FastAPI) -> RedisLiveStateStore:
    return app_.state.live_store


def get_batch_service(app_: FastAPI) -> SnowflakeBatchService:
    return app_.state.batch_service


@app.get("/api/live/cities", response_model=list[LiveCityResponse])
async def list_live_cities():
    return [
        LiveCityResponse(**city.__dict__)
        for city in CITY_CONFIGS.values()
    ]


@app.get("/api/batch/cities")
async def list_batch_cities():
    service = get_batch_service(app)
    return service.list_batch_cities()


@app.get("/api/batch/comparison")
async def get_batch_comparison():
    service = get_batch_service(app)
    return await asyncio.to_thread(service.get_city_comparison)


@app.get("/api/batch/{city}/dashboard")
async def get_batch_dashboard(city: str, stop_limit: int = 200, route_limit: int = 25):
    try:
        get_batch_city_config(city)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    service = get_batch_service(app)
    return await asyncio.to_thread(
        service.get_city_dashboard,
        city,
        stop_limit,
        route_limit,
    )


@app.get("/api/batch/{city}/routes")
async def list_batch_routes(city: str, limit: int = 500):
    try:
        get_batch_city_config(city)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    service = get_batch_service(app)
    return {
        "city": city,
        "routes": await asyncio.to_thread(service.list_routes, city, limit),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/batch/{city}/routes/{route_id}")
async def get_batch_route_detail(city: str, route_id: str, stop_limit: int = 80):
    try:
        get_batch_city_config(city)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    service = get_batch_service(app)
    try:
        return await asyncio.to_thread(service.get_route_detail, city, route_id, stop_limit)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/live/{city}/vehicles", response_model=LiveVehiclesResponse)
async def get_live_vehicles(city: str, route_id: str | None = None):
    try:
        city_config = get_city_config(city)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    store = get_store(app)
    vehicles = await store.list_vehicles(city_config.slug, route_id=route_id)
    return LiveVehiclesResponse(
        city=city_config.slug,
        vehicles=vehicles,
        vehicle_count=len(vehicles),
        generated_at=datetime.now(timezone.utc),
    )


@app.get("/api/live/{city}/health")
async def get_live_health(city: str):
    try:
        city_config = get_city_config(city)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    store = get_store(app)
    metadata = await store.get_metadata(city_config.slug)
    vehicles = await store.list_vehicles(city_config.slug)
    redis_ok = await store.ping()
    return {
        "city": city_config.slug,
        "redis_ok": redis_ok,
        "vehicle_count": len(vehicles),
        "last_upsert_at": metadata.get("last_upsert_at"),
        "last_vehicle_id": metadata.get("last_vehicle_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.websocket("/ws/live/{city}")
async def websocket_live_updates(websocket: WebSocket, city: str):
    try:
        city_config = get_city_config(city)
    except KeyError:
        await websocket.close(code=4404)
        return

    store = get_store(app)
    pubsub = await store.subscribe(city_config.slug)
    await websocket.accept()
    await websocket.send_json(
        {
            "type": "connected",
            "city": city_config.slug,
            "connected_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    last_heartbeat = asyncio.get_running_loop().time()

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            payload = store.decode_message(message)
            if payload is not None:
                await websocket.send_json(
                    {
                        "type": "vehicle_update",
                        "city": city_config.slug,
                        "vehicle": payload,
                    }
                )

            now = asyncio.get_running_loop().time()
            if now - last_heartbeat >= 15:
                await websocket.send_json(
                    {
                        "type": "heartbeat",
                        "city": city_config.slug,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                last_heartbeat = now

    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe()
        await pubsub.aclose()
