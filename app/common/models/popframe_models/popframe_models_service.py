import json

import geopandas as gpd
import pandas as pd
from loguru import logger
from popframe.method.agglomeration import AgglomerationBuilder
from popframe.method.city_evaluation import CityPopulationScorer
from popframe.method.popuation_frame import PopulationFrame
from popframe.method.territory_evaluation import TerritoryEvaluation
from popframe.models.region import Region
from popframe.preprocessing.level_filler import LevelFiller

from app.common.validators.region_validators import validate_region
from app.dependencies import (
    geoserver_storage,
    http_exception,
    pop_frame_caching_service,
    pop_frame_model_api_service,
    urban_api_gateway,
)
from app.routers.router_population import get_population_criterion_score_endpoint

from .popframe_dtype.popframe_api_model import (
    PopFrameAPIModel,
    PopFrameRegionalScenarioModel,
)


class PopFrameModelsService:
    """Class for popframe model handling"""

    @staticmethod
    async def create_model(
        region_borders: gpd.GeoDataFrame,
        towns: gpd.GeoDataFrame,
        adj_mx: pd.DataFrame,
        region_id: int,
    ) -> Region:
        """
        Function initialises popframe region model
        Args:
            region_borders (gpd.GeoDataFrame): region borders
            towns (gpd.GeoDataFrame): region towns layer
            adj_mx (pd.DataFrame): adjacency matrix for region from TransportFrame
            region_id (int): region id
        Returns:
            Region: PopFrame regional model
        Raises:
            500, internal error in case model initialization fails
        """

        local_crs = region_borders.estimate_utm_crs()
        try:
            region_model = Region(
                region=region_borders.to_crs(local_crs),
                towns=towns.to_crs(local_crs),
                accessibility_matrix=adj_mx,
            )
            return region_model

        except Exception as e:
            logger.exception(e)
            raise http_exception(
                status_code=500,
                msg=f"error during PopFrame model initialization with region {region_id}",
                _input={
                    "region": json.loads(region_borders.to_crs(local_crs).to_json()),
                    "cities": json.loads(towns.to_crs(local_crs).to_json()),
                    "adj_mx": adj_mx.to_dict(),
                },
                _detail={"Error": repr(e)},
            )

    async def calculate_model(self, region_id: int) -> None:
        """
        Function calculates popframe model for region
        Args:
            region_id (int): region id
        Returns:
            None
        """

        base_regional_scenario = (
            await pop_frame_model_api_service.get_base_regional_scenario_by_territory(
                region_id
            )
        )
        validate_region(region_id)
        logger.info(f"Started model calculation for the region {region_id}")
        region_borders = await pop_frame_model_api_service.get_region_borders(region_id)
        logger.info(f"Extracted region border for the region {region_id}")
        # ToDo revise cities after broker
        cities_gdf = await pop_frame_model_api_service.get_tf_cities(region_id)
        logger.info(f"Started population retrieval for region {region_id}")
        if "territory_id" in cities_gdf.columns:
            cities_gdf["original_index"] = cities_gdf.index.copy()
            cities_gdf.set_index("territory_id", inplace=True, drop=True)
        population_data_df = await pop_frame_model_api_service.get_regional_scenario_territories_population(
            base_regional_scenario,
            cities_gdf.index.to_list(),
        )
        logger.info(f"Successfully retrieved population data for region {region_id}")
        cities_gdf = pd.merge(
            cities_gdf, population_data_df, left_index=True, right_on="territory_id"
        )

        cities_gdf.set_index("territory_id", inplace=True, drop=True)
        cities_gdf = gpd.GeoDataFrame(cities_gdf, geometry="geometry", crs=4326)
        level_filler = LevelFiller(towns=cities_gdf)
        towns = level_filler.fill_levels()
        logger.info(f"Loaded cities for region {region_id}")
        logger.info(f"Started matrix retrieval for region {region_id}")
        matrix = await pop_frame_model_api_service.get_matrix_for_region(
            region_id=region_id, graph_type="car"
        )
        if "original_index" in cities_gdf.columns:
            towns = pd.merge(
                cities_gdf[["original_index"]], towns, left_index=True, right_index=True
            )
            towns = gpd.GeoDataFrame(towns, geometry="geometry", crs=4326)
            towns.set_index("original_index", inplace=True)
        logger.info(f"Retrieved matrix for region {region_id}")
        matrix = matrix.loc[towns.index, towns.index]
        logger.info(f"Loaded matrix for region {region_id}")
        model = await self.create_model(
            region_borders=region_borders,
            towns=towns,
            adj_mx=matrix,
            region_id=region_id,
        )
        await pop_frame_caching_service.cache_model_to_pickle(
            region_model=model,
            region_id=region_id,
        )
        frame_method = PopulationFrame(region=model)
        gdf_frame = frame_method.build_circle_frame()
        builder = AgglomerationBuilder(region=model)
        agglomeration_gdf = builder.get_agglomerations()
        towns_with_status = builder.evaluate_city_agglomeration_status(
            gdf_frame, agglomeration_gdf
        )
        agglomeration_indicators = towns_with_status[
            "agglomeration_status"
        ].value_counts()
        await pop_frame_model_api_service.upload_scenario_indicators(
            agglomeration_indicators, region_id, base_regional_scenario
        )
        await geoserver_storage.delete_geoserver_cached_layers(region_id)
        logger.info(f"All old .gpkg layer for region {region_id} are deleted")
        agglomeration_gdf.to_crs(4326, inplace=True)
        await geoserver_storage.save_gdf_to_geoserver(
            layer=agglomeration_gdf,
            name="popframe",
            region_id=region_id,
            layer_type="agglomerations",
        )
        logger.info(f"Loaded agglomerations for region {region_id} on geoserver")
        towns_with_status.to_crs(4326, inplace=True)
        await geoserver_storage.save_gdf_to_geoserver(
            layer=towns_with_status,
            name="popframe",
            region_id=region_id,
            layer_type="cities",
        )
        logger.info(f"Loaded cities for region {region_id} on geoserver")

    @staticmethod
    async def get_population_criterion_score(
        hexagons: gpd.GeoDataFrame, popframe_region_model: PopFrameAPIModel
    ) -> list | None:

        polygon_gdf = hexagons.to_crs(popframe_region_model.region_model.crs)
        if popframe_region_model.region_id in [3138, 3268, 16141]:
            region_mo = await urban_api_gateway.get_mo_for_fed_city_with_population(
                popframe_region_model.region_id
            )
            if len(polygon_gdf) == 1:
                polygon_gdf["hexagon_id"] = 0
            scorer = CityPopulationScorer(region_mo, polygon_gdf)
            return pd.DataFrame(scorer.run())["score"].tolist()
        else:
            evaluation = TerritoryEvaluation(region=popframe_region_model.region_model)
            scores = []
            result = evaluation.population_criterion(territories_gdf=polygon_gdf)
            if result:
                for res in result:
                    scores.append(float(res["score"]))
                return scores

    async def calculate_regional_scenario_model(
        self, region_id: int, regional_scenario_id: int
    ):

        logger.info(
            f"Started model calculation for regional scenario {regional_scenario_id}"
        )
        region_borders = await pop_frame_model_api_service.get_region_borders(region_id)
        logger.info(f"Extracted region border for the region {region_id}")
        cities_gdf = await pop_frame_model_api_service.get_tf_cities(region_id)
        logger.info(f"Started population retrieval for region {region_id}")
        if "territory_id" in cities_gdf.columns:
            cities_gdf["original_index"] = cities_gdf.index.copy()
            cities_gdf.set_index("territory_id", inplace=True, drop=True)
        population_data_df = await pop_frame_model_api_service.get_regional_scenario_territories_population(
            regional_scenario_id,
            cities_gdf.index.to_list(),
        )
        logger.info(f"Successfully retrieved population data for region {region_id}")
        cities_gdf = pd.merge(
            cities_gdf, population_data_df, left_index=True, right_on="territory_id"
        )

        cities_gdf.set_index("territory_id", inplace=True, drop=True)
        cities_gdf = gpd.GeoDataFrame(cities_gdf, geometry="geometry", crs=4326)
        level_filler = LevelFiller(towns=cities_gdf)
        towns = level_filler.fill_levels()
        logger.info(f"Loaded cities for region {region_id}")
        logger.info(f"Started matrix retrieval for region {region_id}")
        matrix = await pop_frame_model_api_service.get_matrix_for_region(
            region_id=region_id, graph_type="car"
        )
        if "original_index" in cities_gdf.columns:
            towns = pd.merge(
                cities_gdf[["original_index"]], towns, left_index=True, right_index=True
            )
            towns = gpd.GeoDataFrame(towns, geometry="geometry", crs=4326)
            towns.set_index("original_index", inplace=True)
        logger.info(f"Retrieved matrix for region {region_id}")
        matrix = matrix.loc[towns.index, towns.index]
        logger.info(f"Loaded matrix for region {region_id}")
        model = await self.create_model(
            region_borders=region_borders,
            towns=towns,
            adj_mx=matrix,
            region_id=region_id,
        )
        await pop_frame_caching_service.cache_model_to_pickle(
            region_model=model,
            region_id=region_id,
        )
        frame_method = PopulationFrame(region=model)
        gdf_frame = frame_method.build_circle_frame()
        builder = AgglomerationBuilder(region=model)
        agglomeration_gdf = builder.get_agglomerations()
        towns_with_status = builder.evaluate_city_agglomeration_status(
            gdf_frame, agglomeration_gdf
        )
        agglomeration_indicators = towns_with_status[
            "agglomeration_status"
        ].value_counts()
        await pop_frame_model_api_service.upload_scenario_indicators(
            agglomeration_indicators, region_id, regional_scenario_id
        )
        hexagons = await pop_frame_model_api_service.get_hexagons(region_id)
        hexagons["popframe_estimation"] = await self.get_population_criterion_score(
            hexagons, PopFrameAPIModel(region_id, model)
        )
        await pop_frame_model_api_service.upload_hexagons_indicators(
            hexagons[["popframe_estimation"]], regional_scenario_id, region_id
        )

    async def load_and_cache_all_models(self):
        """
        Functions loads and cashes all available models
        Returns:
            None
        """

        regions_ids_to_process = await pop_frame_model_api_service.get_regions()
        for region_id in regions_ids_to_process:
            try:
                await self.calculate_model(region_id=region_id)
            except Exception as e:
                logger.exception(e)

    async def load_and_cache_all_models_on_startup(self):
        """
        Functions loads and cashes all available models on app startup
        Returns:
            None
        """

        try:
            all_regions = await pop_frame_model_api_service.get_regions()
            cached_regions = await self.get_available_regions()
            regions_to_calculate = list(set(all_regions) - set(cached_regions))
        except Exception as e:
            logger.exception(e)
            return
        for region_id in regions_to_calculate:
            try:
                await self.calculate_model(region_id=region_id)
            except Exception as e:
                logger.exception(e)
                continue

    async def get_model(
        self,
        region_id: int,
    ) -> PopFrameAPIModel:
        """
        Function gets model for region
        Args:
            region_id (int): region id
        Returns:
            PopFrameAPIModel: PopFrameAPIModel model for region
        """

        if not await pop_frame_caching_service.check_path(region_id=region_id):
            await self.calculate_model(region_id=region_id)
        model = await pop_frame_caching_service.load_cached_model(region_id=region_id)
        return PopFrameAPIModel(region_id, model)

    @staticmethod
    async def get_available_regions() -> list[int]:
        """
        Function gets available models
        Returns:
            list[int]: available models list
        """

        result = await pop_frame_caching_service.get_available_models()
        return result


pop_frame_model_service = PopFrameModelsService()
