from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import orjson
from fastapi.responses import ORJSONResponse
from app.core.config import get_settings
from app.api.health import router as health_router
from app.api.dataset import router as dataset_router
from app.api.games import router as games_router
from app.api.franchises import router as franchises_router
from app.api.ask import router as ask_router
from app.services.datastore import get_datastore

def _orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()

def create_app() -> FastAPI:
    s = get_settings()
    app = FastAPI(title=s.APP_NAME, version=s.APP_VERSION, default_response_class=ORJSONResponse)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
    )
    app.include_router(health_router)
    app.include_router(dataset_router)
    app.include_router(games_router)
    app.include_router(franchises_router)
    app.include_router(ask_router)

    @app.on_event("startup")
    def _startup():
        # preload dataset
        get_datastore()

    return app

app = create_app()

