import asyncio

from fastapi import APIRouter, BackgroundTasks
from loguru import logger

from app.common.models.popframe_models.popframe_models_service import \
    pop_frame_model_service

recalculating = False

model_calculator_router = APIRouter(prefix="/model_calculator")


@model_calculator_router.put("/recalculate/all")
async def recalculate_all_popframe_models():
    asyncio.create_task(pop_frame_model_service.load_and_cache_all_models())
    return {"msg": "started recalculation"}


@model_calculator_router.put("/recalculate/{region_id}")
async def recalculate_region(region_id: int):
    """Router recalculates model for region"""

    await pop_frame_model_service.calculate_model(region_id)
    logger.info(f"Successfully calculated model for region with id {region_id}")
    return {"msg": f"successfully calculated model for region with id {region_id}"}


@model_calculator_router.get("/available_regions", response_model=list[int])
async def get_available_regions() -> list[int]:
    """Router returns calculated and cached models"""

    return await pop_frame_model_service.get_available_regions()
