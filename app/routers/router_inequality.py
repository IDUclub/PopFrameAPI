import json

from fastapi import APIRouter
from loguru import logger
import geopandas as gpd
from popframe.method.anchor_settlement import AnchorSettlementBuilder
from popframe.method.spatial_inequality import SpatialInequalityCalculator

from app.dependencies import towns_layers, http_exception
from app.common.models.popframe_models.popframe_models_service import \
    pop_frame_model_service


inequality_router = APIRouter(prefix="/inequality", tags=["inequality"])


@inequality_router.get(
    "/anchor_cities"
)
async def get_anchor_cities(region_id: int, time: int=50):

    try:
        logger.info(f"Processing anchor cities for region {region_id} at time {time}")
        model = await pop_frame_model_service.get_model(region_id)
        builder = AnchorSettlementBuilder(region=model.region_model)
        towns = await towns_layers.get_towns(region_id)
        settlement_boundaries = builder.get_anchor_settlement_boundaries(towns, time=time)
        logger.info(f"Anchor cities processed successfully for region {region_id}")
        return json.loads(settlement_boundaries.to_crs(4326).to_json())
    except Exception as e:
        logger.exception(e)
        raise http_exception(
            500,
            f"Error during anchor cities processing: {str(e)}",
            _input={"region_id": region_id, "time": time},
            _detail={"Error": str(e)},
        )


@inequality_router.get(
    "/spatial_inequality"
)
async def get_spatial_inequality(
        region_id: int,
        level: int | None=None
):

    try:
        logger.info(f"Processing spatial inequality for region {region_id}")
        model = await pop_frame_model_service.get_model(region_id)
        towns = await towns_layers.get_towns(region_id)
        calculator = SpatialInequalityCalculator(region=model.region_model)
        spatial_inequality = calculator.calculate_spatial_inequality(towns)
        if level is not None and level < towns["level"].max():
            aggregate_territories = await towns_layers.towns_api_service.get_territories_for_region(
                region_id,
                get_all_levels=True,
                level=level
            )

            # polygon_spatial_inequality, stats_json = calculator.calculate_polygon_spatial_inequality(
            #     spatial_inequality, aggregate_territories)
            # ToDo: use calculator when will be fixed
            agg_columns = [
                'provision', 'basic', 'additional', 'comfort',
                'soc_workers_dev', 'soc_workers_soc', 'soc_workers_bas',
                'soc_old_dev', 'soc_old_soc', 'soc_old_bas',
                'soc_parents_dev', 'soc_parents_soc', 'soc_parents_bas',
            ]
            agg_dict = {i: "mean" for i in agg_columns}
            agg_dict.update({"geometry": "first", "territory_id_left": "first"})
            towns.reset_index(inplace=True, drop=True)
            polygon_spatial_inequality = gpd.sjoin(
                aggregate_territories, towns
            ).groupby("territory_id_left").agg(agg_dict).rename(
                columns={"territory_id_left": "territory_id", "name_left": "name"}
            )
            polygon_spatial_inequality['spatial_inequality'] = polygon_spatial_inequality[agg_columns].mean(axis=1)
            logger.info(f"Spatial inequality processed successfully for region {region_id} and level {level}")
            return json.loads(gpd.GeoDataFrame(polygon_spatial_inequality, geometry="geometry", crs=4326).to_json())
        logger.info(f"Spatial inequality processed successfully for region {region_id}")
        return json.loads(spatial_inequality.to_crs(4326).to_json())
    except Exception as e:
        logger.exception(e)
        raise http_exception(
            500,
            f"Error during spatial inequality processing: {str(e)}",
            _input={"region_id": region_id},
            _detail={"Error": str(e)},
        )

@inequality_router.put(
    "/cache_towns/{region_id}"
)
async def cache_towns_for_region(region_id: int, force: bool=False):

        try:
            await towns_layers.get_towns(region_id, force)
            return f"Cached towns for region {region_id} successfully"
        except Exception as e:
            logger.exception(e)
            raise http_exception(
                500,
                f"Error during towns caching: {str(e)}",
                _input={"region_id": region_id, "force": force},
                _detail={"Error": str(e)},
            )
