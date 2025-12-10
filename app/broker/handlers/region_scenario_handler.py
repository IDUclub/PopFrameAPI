from iduconfig import Config
from loguru import logger
from otteroad.consumer import BaseMessageHandler
from otteroad.models import RegionalScenarioCreated

from app.common.models.popframe_models.popframe_models_service import (
    PopFrameModelsService,
)


class RegionScenarioHandler(BaseMessageHandler[RegionalScenarioCreated]):

    def __init__(
        self,
        config: Config,
        pop_frame_model_service: PopFrameModelsService,
    ):

        super().__init__()
        self.config = config
        self.pop_frame_model_service = pop_frame_model_service

    async def handle(self, event: RegionalScenarioCreated, ctx):

        await self.pop_frame_model_service.calculate_regional_scenario_model(
            event.territory_id, event.scenario_id
        )
        logger.info(f"Finished calculating regional scenario {event.scenario_id}")

    async def on_startup(self):

        pass

    async def on_shutdown(self):

        pass
