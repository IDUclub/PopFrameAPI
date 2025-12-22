from fastapi import HTTPException
from iduconfig import Config
from loguru import logger
from otteroad.consumer import BaseMessageHandler
from otteroad.models import ProjectCreated

from app.common.models.popframe_models.popframe_models_service import (
    PopFrameModelsService,
)
from app.routers.router_population import process_population_criterion
from app.routers.router_territory import process_evaluation


class ProjectHandler(BaseMessageHandler[ProjectCreated]):

    def __init__(
        self,
        config: Config,
        pop_frame_model_service: PopFrameModelsService,
    ):

        super().__init__()
        self.config = config
        self.pop_frame_model_service = pop_frame_model_service

    # TODO revise ctx
    async def handle(self, event: ProjectCreated, ctx):
        """
        Function handles ProjectCreated events from broker
        Args:
            event (ProjectCreated): ProjectCreated event, should contain base_scenario attribute
            ctx: Any additional context (not used here)
        Returns:
            None
        """

        logger.info("Started processing event {}", repr(event))
        model = await self.pop_frame_model_service.get_model(event.territory_id)
        try:
            await process_population_criterion(
                model,
                event.base_scenario_id,
                self.config.get("URBAN_API_ACCESS_TOKEN"),
            )
            await process_evaluation(
                model,
                event.base_scenario_id,
                self.config.get("URBAN_API_ACCESS_TOKEN"),
            )
        except HTTPException as http_e:
            if http_e.status_code == 404:
                logger.info(
                    "Project with id {} not found, event would not be handled. Most likely it was deleted".format(
                        event.base_scenario_id
                    )
                )
        except Exception as e:
            logger.error(
                "Failed to process event {}. Failed with error {}".format(
                    repr(event.base_scenario_id), repr(e)
                )
            )
        logger.info(f"Finished processing event {repr(event)}")

    async def on_startup(self):
        pass

    async def on_shutdown(self):
        pass
