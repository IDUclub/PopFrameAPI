from pathlib import Path

from iduconfig import Config

from app.common.api_handler.api_handler import APIHandler
from app.common.exceptions.http_exception_wrapper import http_exception
from app.common.storage.geoserver.goserver import GeoserverStorage

config = Config()

urban_api_handler = APIHandler(config.get("URBAN_API"))
transportframe_api_handler = APIHandler(config.get("TRANSPORTFRAME_API"))

geoserver_storage = GeoserverStorage(
    cache_path=Path().absolute() / config.get("GEOSERVER_CACHE_PATH"), config=config
)
