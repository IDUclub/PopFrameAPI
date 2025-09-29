import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from loguru import logger

from app.common.exceptions.exception_handler import ExceptionHandlerMiddleware
from app.common.models.popframe_models.popframe_models_service import (
    pop_frame_model_service,
)
from app.routers import (
    router_agglomeration,
    router_frame,
    router_inequality,
    router_landuse,
    router_popframe,
    router_population,
    router_territory,
)
from app.routers.router_popframe_models import model_calculator_router

from .common.exceptions.http_exception_wrapper import http_exception
from .dependencies import config, towns_layers

logger.remove()
log_level = "DEBUG"
log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
logger.add(
    sys.stderr,
    level=log_level,
    format=log_format,
    colorize=True,
    backtrace=True,
    diagnose=True,
)
logger.add(
    ".log",
    level=log_level,
    format=log_format,
    colorize=False,
    backtrace=True,
    diagnose=True,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not config.get("APP_ENV") == "development":
        await pop_frame_model_service.load_and_cache_all_models_on_startup()
        await towns_layers.cache_all_towns()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="PopFrame API",
    description="API for PopFrame service, handling territory evaluation, population criteria, network frame, and land use data.",
    version="3.0.1",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(ExceptionHandlerMiddleware)


# Root endpoint
@app.get("/", response_model=dict[str, str])
def read_root():
    return RedirectResponse(url="/docs")


@app.get("/logs")
async def get_logs():
    """
    Get logs file from app
    """

    try:
        return FileResponse(
            ".log",
            media_type="application/octet-stream",
            filename=f"popframe.log",
        )
    except FileNotFoundError as e:
        raise http_exception(
            status_code=404,
            msg="Log file not found",
            _input={"lof_file_name": "popframe.log", "log_path": ".log"},
            _detail={"error": repr(e)},
        )
    except Exception as e:
        raise http_exception(
            status_code=500,
            msg="Internal server error during reading logs",
            _input={"lof_file_name": "popframe.log", "log_path": ".log"},
            _detail={"error": repr(e)},
        )


app.include_router(model_calculator_router)
# Include routers
app.include_router(router_territory.territory_router)
app.include_router(router_population.population_router)
app.include_router(router_frame.network_router)
app.include_router(router_agglomeration.agglomeration_router)
app.include_router(router_landuse.landuse_router)
app.include_router(router_popframe.popframe_router)
app.include_router(model_calculator_router)
app.include_router(router_inequality.inequality_router)
