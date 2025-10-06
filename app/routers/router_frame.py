import json
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query
from popframe.method.agglomeration import AgglomerationBuilder
from popframe.method.popuation_frame import PopulationFrame

from app.common.models.popframe_models.popoframe_dtype.popframe_api_model import (
    PopFrameAPIModel,
)
from app.dependencies import pop_frame_model_service

network_router = APIRouter(prefix="/population", tags=["Population Frame"])


@network_router.get("/build_city_frame", response_model=Dict[str, Any])
async def build_circle_frame_endpoint(
    popframe_region_model: PopFrameAPIModel = Depends(
        pop_frame_model_service.get_model
    ),
):
    try:
        frame_method = PopulationFrame(region=popframe_region_model.region_model)
        gdf_frame = frame_method.build_circle_frame()
        return json.loads(gdf_frame.to_json())
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {repr(e)}")


@network_router.get("/build_agglomeration_frames", response_model=Dict[str, Any])
def build_agglomeration_frames(
    popframe_region_model: PopFrameAPIModel = Depends(
        pop_frame_model_service.get_model
    ),
):
    try:
        frame_method = PopulationFrame(region=popframe_region_model.region_model)
        gdf_frame = frame_method.build_circle_frame()

        builder = AgglomerationBuilder(region=popframe_region_model.region_model)
        agglomeration_gdf = builder.get_agglomerations()
        towns_with_status = builder.evaluate_city_agglomeration_status(
            gdf_frame, agglomeration_gdf
        )

        agglomeration_gdf["geometry"] = agglomeration_gdf["geometry"].simplify(
            30, preserve_topology=True
        )
        towns_with_status["geometry"] = towns_with_status["geometry"].simplify(
            10, preserve_topology=True
        )

        agglomerations = json.loads(agglomeration_gdf.to_json())
        towns = json.loads(towns_with_status.to_json())

        result = {"agglomerations": agglomerations, "towns": towns}

        with open("agglomerations_result.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=4)

        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error during city evaluation processing: {repr(e)}",
        )
