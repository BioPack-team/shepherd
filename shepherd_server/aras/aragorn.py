from fastapi import FastAPI, Request, Response
from fastapi.openapi.docs import (
    get_swagger_ui_html,
)
from starlette.responses import HTMLResponse

from shepherd_server.base_routes import (
    ARATargetEnum,
    base_router,
    query_openapi_extra,
    run_async_query,
    run_sync_query,
    callback,
)
from shepherd_server.openapi import set_open_api_schema

ARAGORN = FastAPI(title="Shepherd Aragorn")


@ARAGORN.post("/query", openapi_extra=query_openapi_extra)
async def sync_query(request: Request) -> Response:
    response = await run_sync_query(ARATargetEnum.ARAGORN, request)
    return response


@ARAGORN.post("/asyncquery", openapi_extra=query_openapi_extra)
async def async_query(request: Request) -> Response:
    response = await run_async_query(ARATargetEnum.ARAGORN, request)
    return response


@ARAGORN.post("/callback/{callback_id}", status_code=200, include_in_schema=False)
async def handle_callback(
    callback_id: str,
    request: Request,
) -> Response:
    response = await callback(ARATargetEnum.ARAGORN, callback_id, request)
    return response


ARAGORN.include_router(base_router, prefix="")


@ARAGORN.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
    """Customize Swagger UI."""
    root_path = req.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + ARAGORN.openapi_url
    swagger_favicon_url = root_path + "/static/favicon.png"
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=ARAGORN.title + " - Swagger UI",
        swagger_favicon_url=swagger_favicon_url,
    )


set_open_api_schema(ARAGORN, infores="infores:shepherd-aragorn", subpath="/aragorn")
