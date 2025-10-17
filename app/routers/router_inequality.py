import json

from fastapi import APIRouter, Depends
from loguru import logger
from popframe.method.agglomeration import AgglomerationBuilder
from popframe.method.anchor_settlement import AnchorSettlementBuilder
from popframe.method.spatial_inequality import SpatialInequalityCalculator
from pydantic_geojson import FeatureCollectionModel

from app.common.auth.bearer import verify_bearer_token
from app.common.models.popframe_models.popframe_models_service import (
    pop_frame_model_service,
)
from app.dependencies import http_exception, towns_layers, urban_api_gateway

inequality_router = APIRouter(prefix="/inequality", tags=["inequality"])


@inequality_router.get("/anchor_cities")
async def get_anchor_cities(region_id: int, time: int = 50):

    logger.info(f"Processing anchor cities for region {region_id} at time {time}")
    model = await pop_frame_model_service.get_model(region_id)
    builder = AnchorSettlementBuilder(region=model.region_model)
    towns = await towns_layers.get_towns(region_id)
    towns.set_index("territory_id", inplace=True)
    towns["id"] = towns.index.copy()
    settlement_boundaries = builder.get_anchor_settlement_boundaries(towns, time=time)
    logger.info(f"Anchor cities processed successfully for region {region_id}")
    return json.loads(settlement_boundaries.to_crs(4326).to_json())


@inequality_router.get("/spatial_inequality")
async def get_spatial_inequality(region_id: int, level: int | None = None):

    logger.info(f"Processing spatial inequality for region {region_id}")
    model = await pop_frame_model_service.get_model(region_id)
    towns = await towns_layers.get_towns(region_id)
    if level is not None and level < towns["level"].max():
        aggregate_territories = (
            await towns_layers.towns_api_service.get_territories_for_region(
                region_id, get_all_levels=True, level=level
            )
        )
        calculator = SpatialInequalityCalculator(region=model.region_model)
        polygon_spatial_inequality = calculator.transfer_inequality_metrics_to_polygons(
            towns, aggregate_territories
        )[0]
        logger.info(
            f"Spatial inequality processed successfully for region {region_id} and level {level}"
        )
        return json.loads(polygon_spatial_inequality.to_crs(4326).to_json())
    logger.info(f"Spatial inequality processed successfully for region {region_id}")
    return json.loads(towns.to_crs(4326).to_json())


@inequality_router.get("/context_inequality")
async def get_context_inequality(
    project_id: int, token: str | None = Depends(verify_bearer_token)
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
    context_towns_ids = await urban_api_gateway.get_subterritories_ids_for_ter_ids(
        project_info["properties"]["context"], get_all_levels=True, cities_only=True
    )
    towns = towns[towns.index.isin(context_towns_ids)]
    aggregate_territories = await urban_api_gateway.get_territories_gdf_by_ids(
        project_info["properties"]["context"]
    )
    calculator = SpatialInequalityCalculator(region=model.region_model)
    aggregated_inequality = calculator.transfer_inequality_metrics_to_polygons(
        towns, aggregate_territories
    )[0]
    return {
        "polygon_spatial_inequality": json.loads(
            aggregated_inequality.to_crs(4326).to_json()
        ),
        "context_towns_spatial_inequality": json.loads(towns.to_crs(4326).to_json()),
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
