from dataclasses import dataclass

from popframe.models.region import Region


@dataclass
class PopFrameAPIModel:
    """
    Args:
        region_id (int): The region ID
        region_model (Region): The Region model from popframe.models.region module of PopFrame lib
    """

    region_id: int
    region_model: Region
