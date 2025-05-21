import asyncio
import json
import pickle
from typing import Literal

import requests
import aiohttp
import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from loguru import logger

from app.dependences import (
    urban_api_handler,
    transportframe_api_handler,
    http_exception,
)


class PopFrameModelApiService:
    """Class for external api services data retrieving"""

    # ToDo processing database level changes
    @staticmethod
    async def get_regions() -> list[int]:
        """
        Function retrieves all available regions
        Returns:
            list[int]: list of available regions as int id
        """

        response = await urban_api_handler.get(
            endpoint_url="/api/v1/all_territories_without_geometry",
            params={
                "parent_id": 12639
            }
        )
        regions_id_list = [i["territory_id"] for i in response]
        return regions_id_list

    @staticmethod
    async def get_region_borders(
            region_id: int,
    ) -> gpd.GeoDataFrame:
        """
        Function retrieves region borders based on region id
        Args:
            region_id (int): region id in urban db
        Returns:
            gpd.GeoDataFrame: region borders
        Raises:
            500, error during geometry parsing,
            Any, error from urban api
        """

        response = await urban_api_handler.get(
            endpoint_url=f"/api/v1/territory/{region_id}",
        )
        try:
            region_borders = gpd.GeoDataFrame(
                geometry=[shape(response["geometry"])],
                crs=4326
            )
            return region_borders
        except Exception as e:
            raise http_exception(
                status_code=500,
                msg=f"Error geometry parsing for borders of region {region_id}",
                _input=response["geometry"],
                _detail={
                    "Error": str(e),
                }
            )

    @staticmethod
    async def get_territories_population(territories_ids: list[int]) -> pd.DataFrame:
        """
        Function retrieves population data for provided territories
        Args:
            territories_ids (list): list of territories ids
        Returns:
            pd.DataFrame with id and population data
        Raises:
            500, internal error in case population data parsing fails
        """

        population_list = []
        async with aiohttp.ClientSession() as session:
            for item in range(0, len(territories_ids), 15):
                current_ids = territories_ids[item: item + 15]
                task_list = [
                    urban_api_handler.get(
                        session=session,
                        endpoint_url=f"/api/v1/territory/{ter_id}/indicator_values",
                        params={
                            "indicator_ids": 1
                        }
                    ) for ter_id in current_ids
                ]
                results = await asyncio.gather(*task_list)
                pop_to_add = [i[0]["value"] if len(i) > 0 else 1 for i in results]
                pop_to_add = [int(i) for i in pop_to_add]
                population_list += pop_to_add
        try:
            population_df = pd.DataFrame(
                np.array([territories_ids, population_list]).T,
                columns=["territory_id", "population"]
            )
            population_df = population_df[population_df["population"] > 0].copy()
            return population_df
        except Exception as e:
            logger.exception(e)
            raise http_exception(
                status_code=500,
                msg=f"error during population data retrieval",
                _input=[territories_ids, population_list],
                _detail={"Error": str(e)}
            )

    # ToDo rewrite to object api or graph api
    @staticmethod
    async def get_matrix_for_region(
            region_id: int,
            graph_type: Literal["car", "walk", "intermodal"]
    ) -> pd.DataFrame:
        """
        Function retrieves matrix for region
        Args:
            region_id (int): region id
            graph_type (Literal["", ""]): graph type
        Returns:
            pd.DataFrame: matrix index-values
        Raises:
            404, not found, got empty matrix
            500, internal error, matrix parsing fails
        """

        response = await transportframe_api_handler.get(
            endpoint_url=f"/{region_id}/get_matrix",
            params={
                "graph_type": graph_type,
            },
        )
        try:
            adj_mx = pd.DataFrame(response['values'], index=response['index'], columns=response['columns'])
        except Exception as e:
            logger.exception(e)
            raise http_exception(
                status_code=500,
                msg=f"error during matrix parsing",
                _input=response,
                _detail={"Error": str(e)}
            )
        if adj_mx.empty:
            logger.warning(f"matrix for region {region_id} is empty")
            raise http_exception(
                status_code=404,
                msg=f"matrix for region {region_id} not found",
                _input=response,
                _detail={}
            )
        return adj_mx


    #ToDo Rewrite to api handler
    @staticmethod
    async def get_tf_cities(region_id: int) -> gpd.GeoDataFrame:
        """
        Function retrieves cities for region in matrix
        Args:
            region_id (int): region id
        Returns:
            gpd.GeoDataFrame: gdf with territories
        Raises:
            404, not found, got empty matrix
            500, internal error, matrix parsing fails
        """

        response = requests.get(
            url=f"{transportframe_api_handler.base_url}/{region_id}/get_towns",
        )
        if response.status_code != 200:
            tmp = json.loads(response.text)
            raise http_exception(
                response.status_code,
                msg=f"error during cities parsing",
                _input=response.request.url,
                _detail=tmp if tmp is not dict and "detail" not in tmp else tmp["detail"]
            )

        towns_gdf = pickle.loads(response.content)
        return towns_gdf

    # ToDo Rewrite to hash object
    @staticmethod
    async def get_cities_indicators_map() -> dict[str, dict[str, str | int | None]]:
        """
        Function retrieves map of cities indicators
        Returns:
            dict[]
        Raises:
            Any from urban api
        """

        response = await urban_api_handler.get(
            "/api/v1/indicators_by_parent",
            params={
                "parent_id": 6,
                "get_all_subtree": "false"
            }
        )
        custom_map = {
            "Населенные пункты в агломерациях": "В агломерации",
            "Населенные пункты вне агломераций": "Вне агломерации"
        }
        res = {custom_map.get(i["name_short"]): i for i in response if custom_map.get(i["name_short"])}
        return res

    async def upload_popframe_indicators(self, indicators_series: pd.Series, territory_id: int) -> None:
        """
        Function uploads popframe indicators to urban api
        Args:
            indicators_series (pd.Series): series with indicators
        Returns:
            None
        Raises:
            500, internal error, in case of upload failure
        """

        map_dict = await self.get_cities_indicators_map()
        try:
            for i in indicators_series.index:
                if map_dict.get(i):
                    await urban_api_handler.put(
                        endpoint_url="/api/v1/indicator_value",
                        data={
                            "indicator_id": map_dict[i]["indicator_id"],
                            "territory_id": territory_id,
                            "date_type": "year",
                            "date_value": "2025-01-01",
                            "value": int(indicators_series[i]),
                            "value_type": "real",
                            "information_source": "modeled/PopFrame",
                        }
                    )
        except Exception as e:
            raise e


pop_frame_model_api_service = PopFrameModelApiService()
