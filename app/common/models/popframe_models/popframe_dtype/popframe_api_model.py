from dataclasses import dataclass

from popframe.models.region import Region
from pydantic import field_validator

from app.common.validators.region_validators import validate_region


@dataclass
class PopFrameAPIModel:
    """
    Args:
        region_id (int): The region ID
        region_model (Region): The Region model from popframe.models.region module of PopFrame lib
    """

    region_id: int
    region_model: Region

    @field_validator("region_id")
    @classmethod
    def validate_region_id(cls, v: int):
        return validate_region(v)


class PopFrameRegionalScenarioModel(PopFrameAPIModel):

    regional_scenario_id: int
