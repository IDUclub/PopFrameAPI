from otteroad import KafkaProducerClient, KafkaProducerSettings


class ProducerWrapper:
    def __init__(self):
        self.producer_settings = KafkaProducerSettings.from_env()
        self.producer_service = KafkaProducerClient(
            self.producer_settings, init_loop=False
        )

    async def start(self):
        self.producer_service.init_loop()
        await self.producer_service.start()

    async def stop(self):

        await self.producer_service.close()
