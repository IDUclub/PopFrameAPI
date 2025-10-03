from pathlib import Path

from iduconfig import Config
from otteroad import (
    KafkaConsumerService,
    KafkaConsumerSettings,
    KafkaProducerClient,
    KafkaProducerSettings,
)

from app.broker.broker_service import BrokerService
from app.common.api_handler.api_handler import APIHandler
from app.common.exceptions.http_exception_wrapper import http_exception
from app.common.gateways.urban_api_gateway import UrbanAPIGateway
from app.common.logs.loging import init_logger
from app.common.storage.geoserver.goserver import GeoserverStorage
from app.common.storage.models.gdf_caching_service import GDFCachingService
from app.common.towns.towns_api_service import TownsAPIService
from app.common.towns.towns_layers import TownsLayers

init_logger()
config = Config()
consumer_settings = KafkaConsumerSettings.from_env()
producer_settings = KafkaProducerSettings.from_env()

urban_api_handler = APIHandler(config.get("URBAN_API"))
transportframe_api_handler = APIHandler(config.get("TRANSPORTFRAME_API"))
townsnet_api_handler = APIHandler(config.get("TOWNSNET_API"))
socdemo_api_handler = APIHandler(config.get("SOCDEMO_API"))
broker_client = KafkaConsumerService(consumer_settings)
broker_producer = KafkaProducerClient(producer_settings)
broker_service = BrokerService(config, broker_client, broker_producer)

towns_caching_service = GDFCachingService(
    Path().absolute() / config.get("COMMON_CACHE") / config.get("POPFRAME_TOWNS_CACHE")
)
urban_api_gateway = UrbanAPIGateway(urban_api_handler)
townsnet_api_service = TownsAPIService(
    urban_api_handler, townsnet_api_handler, socdemo_api_handler
)

towns_layers = TownsLayers(townsnet_api_service, towns_caching_service)
geoserver_storage = GeoserverStorage(
    cache_path=Path().absolute()
    / config.get("COMMON_CACHE")
    / config.get("GEOSERVER_CACHE_PATH"),
    config=config,
)
