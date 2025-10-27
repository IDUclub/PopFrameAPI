import asyncio

from fastapi import APIRouter, BackgroundTasks
from loguru import logger

from app.common.models.popframe_models.popframe_models_service import (
    pop_frame_model_service,
)
from app.common.validators.region_validators import validate_region
from app.dependencies import towns_layers

recalculating = False

model_calculator_router = APIRouter(prefix="/model_calculator")


@model_calculator_router.put("/recalculate/all")
async def recalculate_all_popframe_models(models: bool = True, towns: bool = True):
    """
    Recalculate all popframe models and towns

    Params:

        - models (boolean): weather to recalculate models. Defaults to True.

        - towns: (boolean): weather to recalculate towns. Defaults to True.
    """

    if models:
        asyncio.create_task(pop_frame_model_service.load_and_cache_all_models())
    if towns:
        asyncio.create_task(towns_layers.cache_all_towns())
    return {"msg": "started recalculation"}


@model_calculator_router.put("/recalculate/{region_id}")
async def recalculate_region(region_id: int, model: bool = True, towns: bool = True):
    """
    Router recalculates model for region and towns recaching

    Params:

    - region_id (int): region id

    - model (boolean): weather to recalculate models. Defaults to True.

    - towns (boolean): weather to recalculate towns. Defaults to True.
    """

    validate_region(region_id)
    if model:
        await pop_frame_model_service.calculate_model(region_id)
    if towns:
        await towns_layers.get_towns(region_id, force=True)
    logger.info(f"Successfully calculated model for region with id {region_id}")
    return {"msg": f"successfully calculated model for region with id {region_id}"}


@model_calculator_router.get("/available_regions", response_model=list[int])
async def get_available_regions() -> list[int]:
    """Router returns calculated and cached models"""

    return await pop_frame_model_service.get_available_regions()
