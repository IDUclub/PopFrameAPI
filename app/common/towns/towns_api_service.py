import asyncio

import pandas as pd
from tqdm.asyncio import tqdm

import geopandas as gpd

from app.common.api_handler.api_handler import APIHandler


tqdm.pandas()


class TownsAPIService:
    """
    Service for interacting with the Urban API to retrieve town data.
    """

    def __init__(
            self,
            urban_api_handler: APIHandler,
            townsnet_api_handler: APIHandler,
            socdemo_api_handler: APIHandler
    ) -> None:
        """
        Initializes the TownsAPIService with an APIHandler instance.
        Args:
            urban_api_handler (APIHandler): An instance of APIHandler to handle API requests for urban api url.
        Returns:
            None
        """
        self.urban_api_handler = urban_api_handler
        self.townsnet_api_handler = townsnet_api_handler
        self.socdemo_api_handler = socdemo_api_handler

    async def get_all_regions(self) -> list[int]:
        """
        Function retrieves all regions from Urban API.
        Returns:
            list[int]: A list of region IDs.
        Raises:
            Any HTTP exception from Urban API.
        """

        regions = await self.urban_api_handler.get(
            "/api/v1/territories",
            params={
                "parent_id": 12639,
                "page_size": 100
            }
        )
        return [i["territory_id"] for i in regions["results"]]

    async def get_territories_for_region(
            self,
            region_id: int,
            get_all_levels=False,
            cities_only=False,
            centers_only=False,
            level: int | None=None
    ):
        """
        Function retrieves territories for a given region by its ID.
        Args:
            region_id (int): Region ID.
            get_all_levels (bool, optional): Whether to retrieve all levels. Defaults to False.
            cities_only (bool, optional): Whether to retrieve only cities. Defaults to False.
            centers_only (bool, optional): Whether to retrieve only centers. Defaults to False.
            level (int, optional): Level to retrieve territories for. Defaults to None, returns all levels.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the territories of the region in 4326 crs.
        Raises:
            Any HTTP exception from Urban API.
        """

        #ToDo rewrite to bool handler (integrate in APIHandler)
        if get_all_levels:
            get_all_levels = "true"
        else:
            get_all_levels = "false"
        if cities_only:
            cities_only = "true"
        else:
            cities_only = "false"
        if centers_only:
            centers_only = "true"
        else:
            centers_only = "false"
        territories = await self.urban_api_handler.get(
            "/api/v1/all_territories",
            params={
                "parent_id": region_id,
                "get_all_levels": get_all_levels,
                "cities_only": cities_only,
                "centers_only": centers_only,
            }
        )
        gdf = gpd.GeoDataFrame.from_features(territories, crs=4326)
        if not level is None:
            gdf = gdf[gdf["level"] == level]
        return gdf

    async def get_territories_population(self, territories_ids: list[int]) -> list[int]:
        """
        Retrieves the population of a town by its ID.
        Args:
            territories_ids (list[int]): List of IDs to extract population for.
        Returns:
            list[int]: A list of int values representing the population of each territory.
        Raises:
            Any HTTP exception from Urban API.
        """

        task_list = [
            self.urban_api_handler.get(
                f"/api/v1/territory/{territory_id}/indicator_values",
                params={
                    "indicator_ids": 1
                }
            ) for territory_id in territories_ids
        ]

        results = []
        for i in range(0, len(task_list), 40):
            results += await tqdm.gather(*task_list[i:i+40], desc=f"Retrieving population data step {i//40}")
        return [i[0]["value"] for i in results]

    async def get_socdemo_indicators(self, territories_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

        """
        Retrieves soc demo indicators for a given territory.
        Returns passed GeoDataFrame with soc demo indicators.
        Args:
            territories_gdf (gpd.GeoDataFrame): GeoDataFrame containing the territories,
            containing column 'territory_id'.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame with soc demo indicators.
        Raises:
            Any HTTP exception from Urban API.
        """

        async def _parse_soc_demo_response(response: dict, territory_id: int) -> pd.Series:
            """
            Parses the soc demo response and returns a Series with the relevant data.
            Args:
                response (dict): The response from the soc demo API.
                territory_id (int): The ID of the territory.
            Returns:
                pd.Series: A Series containing the parsed soc demo data.
            Raises:
                None
            """

            some_data = pd.json_normalize(response, sep='_')
            some_data.drop(columns=[i for i in some_data.columns if "comm" in i or "loc" in i])
            some_data[some_data.columns] = some_data.apply(lambda x: x[0][0] if not pd.isna(x[0][0]) else 0)
            some_data["parent_id"] = [territory_id]
            return some_data.iloc[0]

        task_list = [
            self.socdemo_api_handler.get(
                "/api/regions/values_identities",
                params={
                    "territory_id": territory_id
                }
            ) for territory_id in territories_gdf["territory_id"]
        ]

        temp_soc_demo_resp_list = []
        for i in range(0, len(task_list), 40):
            temp_soc_demo_resp_list += await tqdm.gather(
                *task_list[i:i+40], desc=f"Retrieving soc demo data step {i//40}"
            )
        territories_gdf["temp_socdemo_dict"] = temp_soc_demo_resp_list
        soc_demo = await asyncio.to_thread(
            territories_gdf["temp_socdemo_dict"].progress_apply, _parse_soc_demo_response
        )
        territories_gdf = pd.merge(territories_gdf, soc_demo, left_on="parent_id", right_on="parent_id")
        territories_gdf.drop(columns="temp_socdemo_dict", inplace=True)
        return gpd.GeoDataFrame(territories_gdf, geometry="geometry", crs=4326)

    async def get_townsnet_prov(self, territory_id: int) -> gpd.GeoDataFrame:
        """
        Retrieves townsnet prov for a given territory.
        Args:
            territory_id (int): The ID of the territory.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame with townsnet prov data.
        Raises:
            Any HTTP exception from TownsNet API.
        """

        response = await self.townsnet_api_handler.get(
            f"/provision/{territory_id}/get_evaluation",
        )

        return gpd.GeoDataFrame.from_features(response, crs=4326)
