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
from shepherd_server.aras.arax import ARAX
from shepherd_server.aras.ars import ARS
from shepherd_server.aras.bte import BTE
from shepherd_server.aras.sipr import SIPR
from shepherd_server.base_routes import base_router
from shepherd_server.openapi import set_open_api_schema
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
    # Upsert the ARS actor registry (the tr_ara_*/tr_kp_* AppConfig.ready()
    # equivalent). Mounted sub-apps don't get their own lifespan, so the ARS
    # seeding runs here.
    from shepherd_utils.ars.lifecycle import seed_registry

    await seed_registry(logging.getLogger("shepherd.ars"))
    yield
    await shutdown_db()


APP = FastAPI(
    title="BioPack Shepherd",
    lifespan=lifespan,
    docs_url=None,
)

APP.include_router(base_router, prefix="")

APP.mount("/aragorn", ARAGORN)
APP.mount("/arax", ARAX)
# The Translator ARS surface (hosted port of NCATSTranslator/Relay)
APP.mount("/ars", ARS)
APP.mount("/bte", BTE)
APP.mount("/sipr", SIPR)

APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

APP.mount("/static", StaticFiles(directory="shepherd_server/static"), name="static")

FastAPIInstrumentor.instrument_app(
    APP,
    excluded_urls="docs,openapi.json",
    # Drop the per-ASGI-message receive/send spans.
    # They represent individual events that are part of a larger message
    # and can flood the OTEL backend with traces that aren't interesting.
    exclude_spans=["receive", "send"],
)


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


set_open_api_schema(
    APP,
    infores="infores:shepherd",
)
