import geopandas as gpd
import pandas as pd
from lazy_object_proxy.utils import await_
from loguru import logger
from pyogrio.errors import DataSourceError

from app.common.storage.models.gdf_caching_service import GDFCachingService
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

    async def _retrieve_towns_for_region(self, region_id: int):

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
        towns = await self.towns_api_service.get_socdemo_indicators(towns, region_id)
        townsnet_prov_data = await self.towns_api_service.get_townsnet_prov(region_id)
        towns = pd.merge(
            towns,
            townsnet_prov_data[["territory_id", "basic", "additional", "comfort"]],
            left_on="territory_id",
            right_on="territory_id",
            how="left",
        )
        towns["provision"] = towns[["basic", "additional", "comfort"]].mean(axis=1)
        towns.set_index("id", drop=False, inplace=True)
        towns = gpd.GeoDataFrame(towns, geometry="geometry", crs=4326)
        self.towns_caching_service.cache_gdf(region_id, towns)
        return towns

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
