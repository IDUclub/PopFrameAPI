import asyncio

import pandas as pd
import geopandas as gpd
from tqdm.asyncio import tqdm

from app.common.api_handler.api_handler import APIHandler


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
            level: int | None=None,
            with_geometry: bool=True
    ) -> pd.DataFrame | gpd.GeoDataFrame:
        """
        Function retrieves territories for a given region by its ID.
        Args:
            region_id (int): Region ID.
            get_all_levels (bool, optional): Whether to retrieve all levels. Defaults to False.
            cities_only (bool, optional): Whether to retrieve only cities. Defaults to False.
            centers_only (bool, optional): Whether to retrieve only centers. Defaults to False.
            level (int, optional): Level to retrieve territories for. Defaults to None, returns all levels.
            with_geometry (bool, optional): Whether to include geometry in the response. Defaults to True.
        Returns:
            pd.DataFrame | gpd.GeoDataFrame: A GeoDataFrame containing the territories of the region in 4326 crs.
        Raises:
            Any HTTP exception from Urban API.
        """

        territories = await self.urban_api_handler.get(
            f"/api/v1/all_territories{'_without_geometry' if not with_geometry else ''}",
            params={
                "parent_id": region_id,
                "get_all_levels": get_all_levels,
                "cities_only": cities_only,
                "centers_only": centers_only,
            }
        )
        if with_geometry:
            df_object = gpd.GeoDataFrame.from_features(territories, crs=4326)
        else:
            df_object = pd.DataFrame.from_records(territories)
        if not level is None:
            df_object = df_object[df_object["level"] == level]
        return df_object

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

    async def get_territories_hierarchy(self, region_id: int) -> list[dict]:
        """
        Retrieves the hierarchy of territories for a given region.
        Args:
            region_id (int): The ID of the region.
        Returns:
            list[dict]: A list of dictionaries representing the hierarchy of territories.
        Raises:
            Any HTTP exception from Urban API.
        """

        response = await self.urban_api_handler.get(
            "/api/v1/all_territories_without_geometry/hierarchy",
            params={
                "parent_id": region_id
            }
        )
        return response

    @staticmethod
    async def create_hierarchy_map_from_level_to_city(
            tree_data: list[dict],
            target_level: int,
            limit_ids: list[int]
    ) -> dict[int, int]:
        """
        Function creates a hierarchy of territories from the given tree data to city level with id filter.
        Args:
            tree_data (list[dict]): List of dictionaries representing the hierarchy of territories.
            target_level (int): The level to which the hierarchy should be created.
            limit_ids (list[int]): List of IDs to filter the territories.
        Returns:
            dict[int | int]
        """

        async def explode_to_target_parent_level(tree_data: list[dict], target_level: int) -> list[dict]:
            """
            Function explodes the tree data to the target parent level.
            Args:
                tree_data (list[dict]): List of dictionaries representing the hierarchy of territories.
                target_level (int): The level to which the hierarchy should be created.
            Returns:
                list[dict]: A list of dictionaries representing the territories hierarchy at the target level.
            """

            results = []
            for node in tree_data:
                if node["level"] == target_level:
                    results.append(node)
                else:
                    new_node = node.copy()
                    if new_node.get("children"):
                        new_node = explode_to_target_parent_level(new_node["children"], target_level)
                    results += new_node
            return results

        async def get_cities_map_for_ter(tree_data: list[dict], limit_ids_list: list[int]) -> dict[int, int]:
            """
            Function creates a mapping of territory IDs to city IDs based on the given tree data and limit IDs.
            Args:
                tree_data: list[dict]: List of dictionaries representing the hierarchy of territories.
                limit_ids_list (list[int]): List of IDs to filter the territories.
            Returns:
                dict[int, int]: A dictionary mapping territory IDs to city IDs.
            """

            async def define_ter_id(data: list[dict], limit_ids_list: list[int]):
                res = []
                for node in data:
                    if node["is_city"]:
                        if node["territory_id"] in limit_ids_list:
                            res.append(node["territory_id"])
                        else:
                            continue
                    elif node["children"]:
                        res += await define_ter_id(node["children"], limit_ids_list)
                return res

            res = {}
            for ter in tree_data:
                res[ter["territory_id"]] = await define_ter_id(ter["children"], limit_ids_list)
            return {i: key for key, value in res.items() for i in value}

        filtered_tree_data = await explode_to_target_parent_level(tree_data, target_level)
        ter_city_map =  await get_cities_map_for_ter(filtered_tree_data, limit_ids)
        return ter_city_map

    async def get_socdemo_indicators(self, territories_gdf: gpd.GeoDataFrame, region_id: int) -> gpd.GeoDataFrame:
        """
        Retrieves soc demo indicators for a given territory.
        Returns passed GeoDataFrame with soc demo indicators.
        Args:
            territories_gdf (gpd.GeoDataFrame): GeoDataFrame containing the territories,
            containing column 'territory_id'.
            region_id (int): Region ID to extract child territories for.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame with soc demo indicators.
        Raises:
            Any HTTP exception from Urban API.
        """

        def _parse_soc_demo_response(row: pd.Series) -> pd.Series:
            """
            Parses the soc demo response and returns a Series with the relevant data.
            Args:
                row: pd.Series: A row from the DataFrame containing the soc demo response and territory_id.
            Returns:
                pd.Series: A Series containing the parsed soc demo data.
            Raises:
                None
            """

            some_data = pd.json_normalize(row["temp_socdemo_dict"], sep='_')
            some_data.drop(columns=[i for i in some_data.columns if "comm" in i or "loc" in i])
            some_data[some_data.columns] = some_data.apply(lambda x: x[0][0] if not pd.isna(x[0][0]) else 0)
            some_data["parent_id"] = [row["territory_id"]]
            return some_data.iloc[0]

        sub_territories = await self.get_territories_for_region(region_id, level=3, with_geometry=False)
        task_list = [
            self.socdemo_api_handler.get(
                "/api/regions/values_identities",
                params={
                    "territory_id": territory_id
                }
            ) for territory_id in sub_territories["territory_id"]
        ]

        #ToDo Remove when socdemo will be available for all territories
        region_hierarchy = await self.get_territories_hierarchy(region_id)
        city_ter_map = await self.create_hierarchy_map_from_level_to_city(
            region_hierarchy,
            target_level=3,
            limit_ids=territories_gdf["territory_id"].to_list()
        )

        territories_gdf["3_level_parent_id"] = territories_gdf["territory_id"].map(city_ter_map)

        temp_soc_demo_resp_list = []
        for i in range(0, len(task_list), 40):
            temp_soc_demo_resp_list += await tqdm.gather(
                *task_list[i:i+40], desc=f"Retrieving soc demo data step {i//40}"
            )
        sub_territories["temp_socdemo_dict"] = temp_soc_demo_resp_list
        soc_demo = await asyncio.to_thread(
            sub_territories[["territory_id", "temp_socdemo_dict"]].apply,
            _parse_soc_demo_response,
            axis=1
        )
        territories_gdf = pd.merge(territories_gdf, soc_demo, left_on="3_level_parent_id", right_on="parent_id")
        territories_gdf.drop(
            columns=["territory_type", "parent", "created_at", "updated_at", "properties", "parent_id_x", "parent_id_y"],
            inplace=True
        )
        territories_gdf.rename(columns={"3_level_parent_id": "parent_id"}, inplace=True)
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
