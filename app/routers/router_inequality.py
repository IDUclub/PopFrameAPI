import asyncio
import json

from fastapi import APIRouter, Depends
from loguru import logger
from pydantic_geojson import FeatureCollectionModel

from popframe.method.anchor_settlement import AnchorSettlementBuilder
from popframe.method.spatial_inequality import SpatialInequalityCalculator

from app.common.models.popframe_models.popframe_models_service import \
    pop_frame_model_service
from app.dependencies import http_exception, towns_layers, urban_api_gateway
from app.common.auth.auth import verify_token

inequality_router = APIRouter(prefix="/inequality", tags=["inequality"])


@inequality_router.get("/anchor_cities")
async def get_anchor_cities(region_id: int, time: int = 50):

    logger.info(f"Processing anchor cities for region {region_id} at time {time}")
    model = await pop_frame_model_service.get_model(region_id)
    builder = AnchorSettlementBuilder(region=model.region_model)
    towns = await towns_layers.get_towns(region_id)
    settlement_boundaries = builder.get_anchor_settlement_boundaries(
        towns, time=time
    )
    logger.info(f"Anchor cities processed successfully for region {region_id}")
    return json.loads(settlement_boundaries.to_crs(4326).to_json())


@inequality_router.get("/spatial_inequality")
async def get_spatial_inequality(region_id: int, level: int | None = None):

    logger.info(f"Processing spatial inequality for region {region_id}")
    model = await pop_frame_model_service.get_model(region_id)
    towns = await towns_layers.get_towns(region_id)
    calculator = SpatialInequalityCalculator(region=model.region_model)
    spatial_inequality = calculator.calculate_spatial_inequality(towns)
    if level is not None and level < towns["level"].max():
        aggregate_territories = (
            await towns_layers.towns_api_service.get_territories_for_region(
                region_id, get_all_levels=True, level=level
            )
        )

        polygon_spatial_inequality, stats_json = (
            calculator.calculate_polygon_spatial_inequality(
                spatial_inequality, aggregate_territories
            )
        )
        logger.info(
            f"Spatial inequality processed successfully for region {region_id} and level {level}"
        )
        return json.loads(polygon_spatial_inequality.to_crs(4326).to_json())
    logger.info(f"Spatial inequality processed successfully for region {region_id}")
    return json.loads(spatial_inequality.to_crs(4326).to_json())


@inequality_router.get("/context_inequality")
async def get_context_inequality(
        project_id: int,
        token: str | None = Depends(verify_token)
) -> dict[str, FeatureCollectionModel]:
    """
    Endpoint returns spatial inequality for a project context. Auth required (via bearer token).

    Params:

    project_id: int - ID of the project to get context inequality for.

    Response Schema:

    {

        "polygon_spatial_inequality": ContextTerritoriesFeatureCollection,

        "context_towns_spatial_inequality": ContextTownsFeatureCollection

    }
    """

    project_info = await urban_api_gateway.get_project_info(project_id, token)
    model = await pop_frame_model_service.get_model(project_info["territory"]["id"])
    towns = await towns_layers.get_towns(project_info["territory"]["id"])
    calculator = SpatialInequalityCalculator(region=model.region_model)
    spatial_inequality = calculator.calculate_spatial_inequality(towns)
    aggregate_territories = await urban_api_gateway.get_territories_gdf_by_ids(project_info["properties"]["context"])
    context_poly_spatial_inequality, stats_json = (
        calculator.calculate_polygon_spatial_inequality(
            spatial_inequality, aggregate_territories
        )
    )
    context_towns_ids =  await urban_api_gateway.get_subterritories_ids_for_ter_ids(
        project_info["properties"]["context"],
        get_all_levels=True,
        cities_only=True
    )
    return {
        "polygon_spatial_inequality": json.loads(context_poly_spatial_inequality.to_crs(4326).to_json()),
        "context_towns_spatial_inequality": json.loads(spatial_inequality[spatial_inequality.index.isin(context_towns_ids)].to_crs(4326).to_json()),
    }

@inequality_router.put("/cache_towns/{region_id}")
async def cache_towns_for_region(region_id: int, force: bool = False):

    try:
        await towns_layers.get_towns(region_id, force)
        return f"Cached towns for region {region_id} successfully"
    except Exception as e:
        logger.exception(e)
        raise http_exception(
            500,
            f"Error during towns caching: {repr(e)}",
            _input={"region_id": region_id, "force": force},
            _detail={"Error": repr(e)},
        )
