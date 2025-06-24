from pathlib import Path

import pandas as pd
import geopandas as gpd
from pyogrio.errors import DataSourceError

from .caching_serivce import CachingService


class GdfCachingService(CachingService):

    def __init__(self, cache_path: Path) -> None:

        super().__init__(cache_path)

    def cache_gdf(self, region_id, gdf: gpd.GeoDataFrame):

        string_path = self.caching_path.joinpath(
            ".".join(["_".join([str(region_id), "towns"]), "pkl"])
        ).__str__()
        try:
            gdf.to_pickle(string_path)
        except Exception as e:
            raise Exception(f"Error caching GeoDataFrame for region {region_id}: {e}")

    def read_gdf(self, region_id: int) -> gpd.GeoDataFrame:

        string_path = self.caching_path.joinpath(
            ".".join(["_".join([str(region_id), "towns"]), "pkl"])
        ).__str__()
        try:
            df = pd.read_pickle(string_path)
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=4326)
        except FileNotFoundError:
            raise FileNotFoundError(f"GeoDataFrame for region {region_id} not found in cache.")
        except DataSourceError:
            raise DataSourceError(f"GeoDataFrame for region {region_id} not found in cache.")
        return gdf
