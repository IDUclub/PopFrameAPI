import asyncio
from pathlib import Path

from iduconfig import Config
from loguru import logger
from popframe.models.region import Region

from app.common.exceptions.http_exception_wrapper import http_exception

from .caching_service import CachingService


class PopFrameCachingService(CachingService):
    """
    Popframe model caching service. Inherits from CachingService from caching_serivce.py
    Attributes:
        config (Config): configuration instance from IDUConfig Config class
    """

    def __init__(
        self,
        popframe_cache_path: Path,
        config: Config,
    ):

        super().__init__(popframe_cache_path)
        self.config = config

    async def check_path(self, region_id: int) -> bool:
        """
        Function checks weather cached model exists
        Args:
            region_id (int): Region ID
        Returns:
            bool: weather model exists
        """

        files = list(self.caching_path.iterdir())
        for file in files:
            if file.name == f"{region_id}.pkl":
                return True
        return False

    async def get_available_models(self) -> list[int]:
        """
        Function returns all cached models
        """

        files = list(self.caching_path.iterdir())
        if len(files) > 0:
            regions = [int(file.name.split(".")[0]) for file in files]
        else:
            regions = []
        return regions

    async def cache_model_to_pickle(self, region_model: Region, region_id: int) -> None:
        """
        Function caches popframe model to pickle
        Args:
            region_model (Region): popframe region model to cache
            region_id (int): region id
        Returns:
            None
        """

        string_path = self.caching_path.joinpath(
            ".".join([str(region_id), "pkl"])
        ).__str__()
        try:
            region_model.to_pickle(string_path),
            logger.info(f"Cached file {region_id} to {string_path}")
        except Exception as e:
            logger.exception(e)
            raise http_exception(
                status_code=500,
                msg=f"Failed to cache file to pickle {region_id}",
                _input={"region": region_model.__str__()},
                _detail={
                    "Error": repr(e),
                    "available_files": await self.get_available_models(),
                },
            )

    async def load_cached_model(self, region_id: int):
        """
        Function loads model from cache
        Args:
            region_id (int): region id
        Returns:
            Region: popframe region model
        Raises:
            500, Error during model loading
        """

        model_to_load = self.caching_path.joinpath(
            ".".join([str(region_id), "pkl"])
        ).__str__()
        try:
            model = await asyncio.to_thread(Region.from_pickle, model_to_load)
            logger.info(f"Loaded file {region_id} to {model_to_load}")
            return model
        except Exception as e:
            raise http_exception(
                status_code=500,
                msg=f"Failed to load file from pickle {region_id}",
                _input={"filepath": model_to_load},
                _detail={"Error": repr(e)},
            )
