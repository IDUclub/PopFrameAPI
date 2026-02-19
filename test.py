import asyncio

from iduconfig import Config
from otteroad import KafkaProducerClient, KafkaProducerSettings
from otteroad.models import ScenarioObjectsUpdated
from otteroad.models.scenario_events.projects.BaseScenarioCreated import (
    BaseScenarioCreated,
)
from otteroad.models.scenario_events.projects.ProjectCreated import ProjectCreated
from otteroad.models.scenario_events.regional_scenarios.RegionalScenarioCreated import (
    RegionalScenarioCreated,
)

config = Config()
producer_settings = KafkaProducerSettings.from_env()


async def send_event():
    async with KafkaProducerClient(producer_settings) as producer:
        event = ProjectCreated(project_id=120, base_scenario_id=198, territory_id=1)
        await producer.send(event)


asyncio.run(send_event())
