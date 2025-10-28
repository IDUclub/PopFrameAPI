import asyncio

import geopandas as gpd
import pandas as pd
from loguru import logger
from pyogrio.errors import DataSourceError

from app.common.storage.models.gdf_caching_service import GDFCachingService
from app.common.validators.region_validators import validate_region
from app.dependencies import TownsAPIService, http_exception


class TownsLayers:
    """
    Class for managing towns layers.
    """

    def __init__(
        self,
        towns_api_service: TownsAPIService,
        towns_caching_service: GDFCachingService,
    ) -> None:
        """
        Initializes the TownsLayers with a TownsAPIService instance.
        Args:
            towns_api_service (TownsAPIService): An instance of TownsAPIService to handle API requests for towns data.
        Returns:
            None
        """
        self.towns_api_service = towns_api_service
        self.towns_caching_service = towns_caching_service

    async def _retrieve_towns_for_region(self, region_id: int) -> gpd.GeoDataFrame:
        """
        Function to retrieve towns for a given region.
        Args:
            region_id (str): Region ID.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing towns for a given region with formed data.
        """

        def _rename_prov_columns(df: pd.DataFrame, group_name: str) -> pd.DataFrame:
            """
            Function renames columns to spatial inequality acceptable format
            Args:
                df (pd.DataFrame): Dataframe to rename
                group_name (str): Name of column to rename
            Returns:
                pd.DataFrame: Dataframe with renamed columns
            """

            return df[["Обеспеченность", "basic", "additional", "comfort"]].rename(
                columns={
                    "Обеспеченность": f"{group_name} - Неравенство",
                    "basic": f"{group_name} - Неравенство - basic",
                    "additional": f"{group_name} - Неравенство - additional",
                    "comfort": f"{group_name} - Неравенство - comfort",
                }
            )

        soc_groups = await self.towns_api_service.get_soc_groups()
        towns = await self.towns_api_service.get_territories_for_region(
            region_id, get_all_levels=True, cities_only=True, centers_only=True
        )
        towns["parent_id"] = towns["parent"].apply(lambda x: x["id"])
        towns["population"] = await self.towns_api_service.get_territories_population(
            towns["territory_id"].to_list()
        )
        towns["is_anchor_settlement"] = (
            towns["target_city_type"]
            .apply(lambda x: x is not None and "id" in x)
            .astype(bool)
        )
        towns["id"] = towns["territory_id"].copy()

        townsnet_prov_tasks = [
            self.towns_api_service.get_townsnet_region_evaluation(
                region_id, soc_group_info["soc_group_id"]
            )
            for soc_group_info in soc_groups
        ]
        townsnet_prov_data = await asyncio.gather(*townsnet_prov_tasks)
        combined_towns_layer = pd.concat(
            [
                _rename_prov_columns(townsnet_prov, soc_group_data["name"])
                for townsnet_prov, soc_group_data in zip(townsnet_prov_data, soc_groups)
            ],
            axis=1,
        )
        target_columns = [c for c in combined_towns_layer.columns if "Неравенство" in c]
        combined_towns_layer[target_columns] = 1 - combined_towns_layer[target_columns]
        combined_towns_layer["Пространственное неравенство"] = (
            combined_towns_layer[target_columns].mean(axis=1).round(2)
        )
        result = pd.concat([towns, combined_towns_layer], axis=1)
        result.set_index("id", inplace=True, drop=True)
        result[target_columns] = result[target_columns].apply(
            lambda x: (
                [round(i, 2) if i else i for i in x]
                if float in list(set(x.apply(type)))
                else x
            )
        )
        self.towns_caching_service.cache_gdf(region_id, result)
        return result

    async def get_towns(self, region_id: int, force: bool = False) -> gpd.GeoDataFrame:
        """
        Function retrieves towns for a given territory by its ID.
        Args:
            region_id (int): The ID of the territory.
            force (bool, optional): Whether to force caching.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the towns of the territory in 4326 crs.
        Raises:
            Any HTTP exception from Towns API.
        """

        validate_region(region_id)
        try:
            if force:
                logger.info(f"Force caching towns for region {region_id}")
                return await self._retrieve_towns_for_region(region_id)
            towns = self.towns_caching_service.read_gdf(region_id)
            return towns
        except FileNotFoundError:
            logger.info(
                f"Towns not found in cache for region {region_id}, retrieving from API"
            )
            return await self._retrieve_towns_for_region(region_id)
        except DataSourceError:
            logger.info(
                f"Towns not found in cache for region {region_id}, retrieving from API"
            )
            return await self._retrieve_towns_for_region(region_id)
        except Exception as e:
            logger.exception(e)
            raise http_exception(
                500,
                f"Error during processing towns",
                _input=region_id,
                _detail={"error": repr(e)},
            ) from e

    async def cache_all_towns(self) -> None:
        """
        Function caches all towns for a given region.
        Returns:
            None
        """

        regions = await self.towns_api_service.get_all_regions()
        for region_id in regions:
            try:
                await self.get_towns(region_id, force=True)
            except Exception as e:
                logger.exception(e)
                continue
