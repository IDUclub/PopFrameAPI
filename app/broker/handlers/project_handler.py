from typing import Awaitable, Callable

from iduconfig import Config
from loguru import logger
from otteroad.consumer import BaseMessageHandler
from otteroad.models import ProjectCreated

from app.common.models.popframe_models.popframe_models_service import (
    pop_frame_model_service,
)
from app.routers.router_population import process_population_criterion


class ProjectHandler(BaseMessageHandler):

    def __init__(
        self,
        config: Config,
    ):

        super().__init__()
        self.config = config

    # TODO revise ctx
    async def handle(self, event: ProjectCreated, ctx):
        """
        Function handles ProjectCreated events from broker
        Args:
            event (ProjectCreated): ProjectCreated event, should contain base_scenario attribute
            ctx: None
        Returns:
            None
        """

        logger.info(f"Started processing event {repr(event)}")
        try:
            if isinstance(event, ProjectCreated):
                model = pop_frame_model_service.get_model(event.territory_id)
                await process_population_criterion(
                    model,
                    event.base_scenario_id,
                    self.config.get("URBAN_API_ACCESS_TOKEN"),
                )
                logger.info(f"Finished processing event {repr(event)}")
            else:
                raise NotImplementedError(
                    f"Event type {type(event)} not supported for {type(self)} class"
                )
        except Exception as e:
            logger.exception(f"ProjectHandler::handle: {repr(e)}")
            raise e

    async def on_startup(self):
        pass

    async def on_shutdown(self):
        pass
