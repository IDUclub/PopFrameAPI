import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from loguru import logger

from app.common.models.popframe_models.popframe_models_service import \
    pop_frame_model_service
from app.routers import (router_agglomeration, router_frame, router_landuse,
                         router_popframe, router_population, router_territory)
from app.routers.router_popframe_models import model_calculator_router

from .common.exceptions.http_exception_wrapper import http_exception
from .dependencies import config

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
    config.get("LOGS_FILE"),
    level=log_level,
    format=log_format,
    colorize=False,
    backtrace=True,
    diagnose=True,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await pop_frame_model_service.load_and_cache_all_models_on_startup()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="PopFrame API",
    description="API for PopFrame service, handling territory evaluation, population criteria, network frame, and land use data.",
    version="3.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
            f"{config.get('LOG_FILE')}.log",
            media_type="application/octet-stream",
            filename=f"{config.get('LOG_FILE')}.log",
        )
    except FileNotFoundError as e:
        raise http_exception(
            status_code=404,
            msg="Log file not found",
            _input={"lof_file_name": f"{config.get('LOG_FILE')}.log"},
            _detail={"error": e.__str__()},
        )
    except Exception as e:
        raise http_exception(
            status_code=500,
            msg="Internal server error during reading logs",
            _input={"lof_file_name": f"{config.get('LOG_FILE')}.log"},
            _detail={"error": e.__str__()},
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
