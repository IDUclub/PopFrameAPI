import json
from typing import Annotated, Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query
from popframe.method.aglomeration import AgglomerationBuilder
from popframe.method.popuation_frame import PopulationFrame

from app.common.models.popframe_models.popframe_models_service import \
    pop_frame_model_service
from app.common.storage.geoserver.geoserver_dto import PopFrameGeoserverDTO
from app.dependencies import geoserver_storage
from app.dto import RegionAgglomerationDTO

agglomeration_router = APIRouter(prefix="/agglomeration", tags=["Agglomeration"])


@agglomeration_router.get(
    "/geoserver/get_href", response_model=list[PopFrameGeoserverDTO]
)
async def get_href(region_id: int) -> list[PopFrameGeoserverDTO]:
    try:

        agglomeration_check = await geoserver_storage.check_cached_layers(
            region_id=region_id, layer_type="agglomerations"
        )
        cities_check = await geoserver_storage.check_cached_layers(
            region_id=region_id, layer_type="cities"
        )
        if agglomeration_check and cities_check:
            agglomerations = await geoserver_storage.get_layer_from_geoserver(
                region_id=region_id,
                layer_type="agglomerations",
            )
            cities = await geoserver_storage.get_layer_from_geoserver(
                region_id=region_id,
                layer_type="cities",
            )
            return [agglomerations, cities]
        else:
            await pop_frame_model_service.calculate_model(region_id)
            result = await get_href(region_id)
            return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error during agglomeration processing: {repr(e)}"
        )


@agglomeration_router.get("/build_agglomeration")
async def get_agglomeration_endpoint(
    agglomerations_params: Annotated[
        RegionAgglomerationDTO, Depends(RegionAgglomerationDTO)
    ],
):
    try:
        popframe_region_model = await pop_frame_model_service.get_model(
            agglomerations_params.region_id
        )
        builder = AgglomerationBuilder(region=popframe_region_model.region_model)
        agglomeration_gdf = builder.get_agglomerations(time=agglomerations_params.time)
        agglomeration_gdf.to_crs(4326, inplace=True)
        result = json.loads(agglomeration_gdf.to_json())
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error during agglomeration processing: {repr(e)}"
        )


@agglomeration_router.get(
    "/evaluate_city_agglomeration_status", response_model=Dict[str, Any]
)
async def evaluate_cities_in_agglomeration(
    agglomerations_params: Annotated[
        RegionAgglomerationDTO, Depends(RegionAgglomerationDTO)
    ],
):
    try:
        popframe_region_model = await pop_frame_model_service.get_model(
            agglomerations_params.region_id
        )
        frame_method = PopulationFrame(region=popframe_region_model.region_model)
        gdf_frame = frame_method.build_circle_frame()
        builder = AgglomerationBuilder(region=popframe_region_model.region_model)
        agglomeration_gdf = builder.get_agglomerations(time=agglomerations_params.time)
        towns_with_status = builder.evaluate_city_agglomeration_status(
            gdf_frame, agglomeration_gdf
        )
        towns_with_status.to_crs(4326, inplace=True)
        result = json.loads(towns_with_status.to_json())
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error during city evaluation processing: {repr(e)}",
        )
