"""Shepherd ARA."""

import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Body, FastAPI, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import (
    get_swagger_ui_html,
)
from fastapi.staticfiles import StaticFiles
from starlette.responses import HTMLResponse

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from shepherd_server.aras.aragorn import ARAGORN
from shepherd_server.base_routes import base_router
from shepherd_server.openapi import construct_open_api_schema
from shepherd_utils.broker import add_task
from shepherd_utils.db import (
    initialize_db,
    shutdown_db,
)
from shepherd_utils.logger import QueryLogger, setup_logging
from shepherd_utils.otel import setup_tracer

setup_logging()
tracer = setup_tracer("shepherd-server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle db connection."""
    await initialize_db()
    yield
    await shutdown_db()


APP = FastAPI(
    title="BioPack Shepherd",
    lifespan=lifespan,
    docs_url=None,
)

APP.include_router(base_router, prefix="")

APP.mount("/aragorn", ARAGORN)

APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

APP.mount("/static", StaticFiles(directory="shepherd_server/static"), name="static")

FastAPIInstrumentor.instrument_app(APP, excluded_urls="docs,openapi.json")


@APP.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
    """Customize Swagger UI."""
    root_path = req.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + APP.openapi_url
    swagger_favicon_url = root_path + "/static/favicon.png"
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=APP.title + " - Swagger UI",
        swagger_favicon_url=swagger_favicon_url,
    )


APP.openapi_schema = construct_open_api_schema(
    APP,
    infores="infores:shepherd",
)
