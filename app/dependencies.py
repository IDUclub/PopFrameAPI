from pathlib import Path

from iduconfig import Config
from prometheus_client import start_http_server

from app.common.api_handler.api_handler import APIHandler
from app.common.checkers.territory_checker import TerritoryChecker
from app.common.exceptions.http_exception_wrapper import http_exception
from app.common.gateways.urban_api_gateway import UrbanAPIGateway
from app.common.logs.loging import init_logger
from app.common.models.popframe_models.popframe_models_service import (
    PopFrameModelsService,
)
from app.common.models.popframe_models.services.popframe_models_api_service import (
    PopFrameModelApiService,
)
from app.common.storage.geoserver.goserver import GeoserverStorage
from app.common.storage.models.gdf_caching_service import GDFCachingService
from app.common.storage.models.pop_frame_caching_service import PopFrameCachingService
from app.common.towns.towns_api_service import TownsAPIService
from app.common.towns.towns_layers import TownsLayers

init_logger()
config = Config()

start_http_server(
    int(config.get("PROMETHEUS_PORT")),
)

urban_api_handler = APIHandler(config.get("URBAN_API"))
transportframe_api_handler = APIHandler(config.get("TRANSPORTFRAME_API"))
townsnet_api_handler = APIHandler(config.get("TOWNSNET_API"))
socdemo_api_handler = APIHandler(config.get("SOCDEMO_API"))

towns_caching_service = GDFCachingService(
    Path().absolute() / config.get("COMMON_CACHE") / config.get("POPFRAME_TOWNS_CACHE")
)
urban_api_gateway = UrbanAPIGateway(urban_api_handler)
townsnet_api_service = TownsAPIService(
    urban_api_handler, townsnet_api_handler, socdemo_api_handler
)

territory_checker = TerritoryChecker(urban_api_gateway)

towns_layers = TownsLayers(townsnet_api_service, towns_caching_service)
geoserver_storage = GeoserverStorage(
    cache_path=Path().absolute()
    / config.get("COMMON_CACHE")
    / config.get("GEOSERVER_CACHE_PATH"),
    config=config,
)

pop_frame_model_api_service = PopFrameModelApiService(
    config, transportframe_api_handler, urban_api_handler
)
pop_frame_caching_service = PopFrameCachingService(
    (
        Path().absolute()
        / config.get("COMMON_CACHE")
        / config.get("POPFRAME_MODEL_CACHE")
    ),
    config,
)
pop_frame_model_service = PopFrameModelsService(
    geoserver_storage,
    pop_frame_caching_service,
    pop_frame_model_api_service,
    urban_api_gateway,
    territory_checker,
)
