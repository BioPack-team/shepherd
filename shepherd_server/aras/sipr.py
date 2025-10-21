from fastapi import Body, FastAPI, Request, Response
from fastapi.openapi.docs import (
    get_swagger_ui_html,
)
from starlette.responses import HTMLResponse

from shepherd_server.base_routes import (
    ARATargetEnum,
    base_router,
    default_input_query,
    run_async_query,
    run_sync_query,
)
from shepherd_server.openapi import construct_open_api_schema

SIPR = FastAPI(title="Shepherd SIPR")


@SIPR.post("/query")
async def sync_query(
    query: dict = Body(..., example=default_input_query),
) -> Response:
    response = await run_sync_query(ARATargetEnum.SIPR, query)
    return response


@SIPR.post("/asyncquery")
async def async_query(
    query: dict = Body(..., example=default_input_query),
) -> Response:
    response = await run_async_query(ARATargetEnum.SIPR, query)
    return response


SIPR.include_router(base_router, prefix="")


@SIPR.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
    """Customize Swagger UI."""
    root_path = req.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + SIPR.openapi_url
    swagger_favicon_url = root_path + "/static/favicon.png"
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=SIPR.title + " - Swagger UI",
        swagger_favicon_url=swagger_favicon_url,
    )


SIPR.openapi_schema = construct_open_api_schema(
    SIPR, infores="infores:shepherd-sipr", subpath="/sipr"
)
