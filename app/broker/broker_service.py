from iduconfig import Config
from otteroad import KafkaConsumerService, KafkaProducerClient

from .handlers import ProjectHandler


class BrokerService:

    def __init__(
        self,
        config: Config,
        broker_client: KafkaConsumerService,
        broker_producer: KafkaProducerClient,
    ):

        self.config = config
        self.broker_client = broker_client
        self.broker_producer = broker_producer

    async def register_and_start(self):

        self.broker_client.register_handler(ProjectHandler(self.config))
        self.broker_client.add_worker(topics=["scenario.event"])
        await self.broker_client.start()
        await self.broker_producer.start()

    async def stop(self):

        await self.broker_client.stop()
        await self.broker_producer.close()
