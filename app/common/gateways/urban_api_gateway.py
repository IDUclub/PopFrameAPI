import asyncio

import geopandas as gpd
import pandas as pd

from app.common.api_handler.api_handler import APIHandler
from app.common.exceptions.http_exception_wrapper import http_exception


class UrbanAPIGateway:
    """
    Class for requests to the Urban API

    Args:
        api_handler (APIHandler): An instance of APIHandler to handle API requests.
    """

    def __init__(self, api_handler: APIHandler):
        """
        Initializes the UrbanAPIGateway with an APIHandler instance.
        Args:
            api_handler (APIHandler): An instance of APIHandler to handle API requests.
        """

        self.api_handler = api_handler

    async def get_mo_for_fed_city_with_population(
        self, federal_city_id: int
    ) -> gpd.GeoDataFrame | pd.DataFrame:
        """
        Function retrieves territories for a given federal city by its ID.
        Args:
            federal_city_id (int): The ID of the federal city.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the territories of the federal city in 4326 crs.
        Raises:
            Any HTTP from Urban API.
        """

        resp = await self.api_handler.get(
            "/api/v1/territory/indicator_values",
            params={"parent_id": federal_city_id, "indicator_ids": 1},
        )
        gdf = gpd.GeoDataFrame.from_features(resp, crs=4326)
        all_gdfs = []
        for parent_id in gdf["territory_id"]:
            child_gdf_resp = await self.api_handler.get(
                "/api/v1/territory/indicator_values",
                params={"parent_id": parent_id, "indicator_ids": 1},
            )
            if child_gdf_resp["features"]:
                child_gdf = gpd.GeoDataFrame.from_features(child_gdf_resp, crs=4326)
                child_gdf["parent_territory_id"] = parent_id
                child_gdf["parent_name"] = gdf.loc[
                    gdf["territory_id"] == parent_id, "name"
                ].iloc[0]
                all_gdfs.append(child_gdf)
        res_gdf = pd.concat(all_gdfs, ignore_index=True)
        res_gdf["population"] = res_gdf["indicators"].apply(lambda x: x[0].get("value"))
        return res_gdf

    async def get_population_for_territory(self, territory_id: int) -> int | None:
        """
        Function retrieves population for a given territory by its ID.
        Args:
            territory_id (int): The ID of the territory.
        Returns:
            int | None: The population of the territory or None if population response is empty.
        Raises:
            Any HTTP from Urban API.
        """

        resp = await self.api_handler.get(
            "/api/v1/territory/indicator_values",
            params={"territory_id": territory_id, "indicator_ids": 1},
        )
        if not resp:
            return None
        return resp[0]["indicators"][0]["value"]

    async def get_project_id_by_scenario_id(
        self, scenario_id: int, token: str | None = None
    ) -> int:
        """
        Function retrieves project id by scenario id by its ID.
        Args:
            scenario_id (int): The ID of the scenario.
            token (str | None): The API token. Defaults to None.
        Returns:
            int: The project id.
        Raises:
            Any HTTP from Urban API.
        """

        headers = {"Authorization": f"Bearer {token}"} if token else None
        resp = await self.api_handler.get(
            f"/api/v1/scenarios/{scenario_id}", headers=headers
        )
        if resp:
            return resp["project_id"]["project_id"]
        raise http_exception(
            404,
            "No project data found",
            _input={
                "token": token,
                "scenario_id": scenario_id,
            },
            _detail={},
        )

    async def get_project_info(
        self,
        project_id: int,
        token: str | None = None,
    ) -> dict:
        """
        Function retrieves project info by its ID.
        Args:
            project_id (int): The ID of the project.
            token (str | None): User API token for private projects. Defaults to None.
        Returns
            dict: The project info.
        Raises:
            Any HTTP from Urban API.
        """

        headers = {"Authorization": f"Bearer {token}"} if token else None
        resp = await self.api_handler.get(
            f"/api/v1/projects/{project_id}", headers=headers
        )
        if resp:
            return resp
        raise http_exception(
            404,
            "No project data found",
            _input={
                "token": token,
                "project_id": project_id,
            },
            _detail={},
        )

    async def get_project_info_by_scenario(
        self,
        scenario_id: int,
        token: str | None = None,
    ) -> dict:
        """
        Function retrieves project info for a given scenario by its ID.
        Args:
            scenario_id (int): The ID of the scenario.
            token (str | None): The user private API token. Defaults to None.
        Returns:
            dict: The project info of the scenario.
        Raises:
            Any HTTP from Urban API.
        """

        project_id = await self.get_project_id_by_scenario_id(scenario_id, token)
        return await self.get_project_info(project_id, token)

    async def get_territories_gdf_by_ids(
        self,
        territories_ids: list[str],
        centers_only: bool = False,
    ) -> gpd.GeoDataFrame:
        """
        Function retrieves territories for a given list of IDs.
        Args:
            territories_ids (list[str]): The IDs of the territories.
            centers_only (bool): Whether the territories should be returned centers.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the territories of the territory in 4326 crs.
        """

        resp = await self.api_handler.get(
            f"/api/v1/territories/{', '.join([str(i) for i in territories_ids])}",
            params={"centers_only": centers_only},
        )
        return gpd.GeoDataFrame.from_features(resp, crs=4326)

    async def get_subterritories_ids_for_ter_ids(
        self,
        territories_ids: list[int],
        get_all_levels: bool = False,
        cities_only: bool = False,
    ) -> list[int]:
        """
        Function retrieves subterritories IDs for a given list of territory IDs.
        Args:
            territories_ids (list[int]): The IDs of the territories.
            get_all_levels (bool): Whether to get all levels of subterritories. Defaults to False.
            cities_only (bool): Whether to return only cities. Defaults to False.
        Returns:
            list[int]: A list of subterritories IDs.
        Raises:
            Any HTTP from Urban API.
        """

        tasks = [
            self.api_handler.get(
                "/api/v1/all_territories_without_geometry",
                params={
                    "parent_id": ter_id,
                    "get_all_levels": get_all_levels,
                    "cities_only": cities_only,
                },
            )
            for ter_id in territories_ids
        ]
        territories = await asyncio.gather(*tasks)
        res = []
        for i in territories:
            res += [ter["territory_id"] for ter in i]
        return res

    async def get_territory_hexagons(self, territory_id: int) -> gpd.GeoDataFrame:
        """
        Function retrieves territory hexagons geometries for a given territory ID.
        Args:
            territory_id (int): The ID of the territory.
        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the territory hexagons geometry.
        Raises:
            Any HTTP from Urban API.
        """

        response = await self.api_handler.get(
            f"api/v1/territory/{territory_id}/hexagons",
        )
        return gpd.GeoDataFrame.from_features(response, crs=4326)
