from iduconfig import Config
from otteroad import KafkaConsumerService, KafkaProducerClient

from app.common.models.popframe_models.popframe_models_service import (
    PopFrameModelsService,
)

from .handlers import ProjectHandler, RegionScenarioHandler
from .producer_wrapper import ProducerWrapper


class BrokerService:

    def __init__(
        self,
        config: Config,
        broker_client: KafkaConsumerService,
        pop_frame_model_service: PopFrameModelsService,
    ):

        self.config = config
        self.broker_client = broker_client
        self.pop_frame_model_service = pop_frame_model_service

    async def register_and_start(self):

        producer = ProducerWrapper()

        self.broker_client.register_handler(
            ProjectHandler(self.config, self.pop_frame_model_service)
        )
        self.broker_client.register_handler(
            RegionScenarioHandler(
                self.config, self.pop_frame_model_service, producer.producer_service
            )
        )
        self.broker_client.add_worker(topics=["scenario.events"])

        await self.broker_client.start()

    async def stop(self):

        await self.broker_client.stop()
