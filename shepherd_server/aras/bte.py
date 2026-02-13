from fastapi import Body, FastAPI, Request, Response
from fastapi.openapi.docs import (
    get_swagger_ui_html,
)
from starlette.responses import HTMLResponse

from shepherd_server.base_routes import (
    ARATargetEnum,
    base_router,
    callback,
    default_input_query,
    run_async_query,
    run_sync_query,
)
from shepherd_server.openapi import construct_open_api_schema

BTE = FastAPI(title="Shepherd BTE")


@BTE.post("/query")
async def sync_query(
    query: dict = Body(..., examples=[default_input_query]),
) -> Response:
    response = await run_sync_query(ARATargetEnum.BTE, query)
    return response


@BTE.post("/asyncquery")
async def async_query(
    query: dict = Body(..., examples=[default_input_query]),
) -> Response:
    response = await run_async_query(ARATargetEnum.BTE, query)
    return response


@BTE.post("/callback/{callback_id}", status_code=200, include_in_schema=False)
async def handle_callback(
    callback_id: str,
    response: dict,
) -> Response:
    response = await callback(ARATargetEnum.BTE, callback_id, response)
    return response


BTE.include_router(base_router, prefix="")


@BTE.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
    """Customize Swagger UI."""
    root_path = req.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + BTE.openapi_url
    swagger_favicon_url = root_path + "/static/favicon.png"
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=BTE.title + " - Swagger UI",
        swagger_favicon_url=swagger_favicon_url,
    )


BTE.openapi_schema = construct_open_api_schema(
    BTE, infores="infores:shepherd-bte", subpath="/bte"
)
