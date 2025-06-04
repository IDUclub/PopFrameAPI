import pickle
from datetime import datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
from idustorage import Cacheable


class CacheableGeopackageObject(Cacheable):
    def __init__(self, to_cache: gpd.GeoDataFrame):
        self.object = to_cache

    def to_file(
        self,
        path: Path,
        name: str,
        ext: str = ".gpkg",
        date: str = datetime.now(),
        separator: str = "_",
        *args,
    ) -> str:
        filepath = f"{date}{separator}{name}"
        for arg in args:
            filepath += f"{separator}{arg}"
        filepath += ext
        path_to_file = path / filepath
        try:
            self.object.to_file(path_to_file)
        except Exception as error:
            print(error)
        return filepath
