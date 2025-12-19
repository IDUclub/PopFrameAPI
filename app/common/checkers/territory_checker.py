import asyncio

import numpy as np

from app.common.gateways.urban_api_gateway import UrbanAPIGateway


class TerritoryChecker:
    """
    This class is aimed to check territory statuses for.
    Attributes:
        urban_api_gateway (UrbanAPIGateway): API Gateway for Urban API requests
        federal_cities (np.ndarray): numpy array of federal cities ids (as int) from Urban API. None on init.
    """

    def __init__(self, urban_api_gateway: UrbanAPIGateway):
        """
        Initialize TerritoryChecker class
        Args:
            urban_api_gateway (UrbanAPIGateway): API Gateway for Urban API requests
        """

        self.urban_api_gateway: UrbanAPIGateway = urban_api_gateway
        self.federal_cities: np.ndarray | None = None

    async def check_on_federal_city(self, territory_id: int) -> bool:
        """
        Function checks weather territory is a federal city
        Args:
            territory_id (int): territory id from Urban API
        Returns:
            bool: True if territory is a federal city else False
        """

        if not self.federal_cities:
            countries_ids = await self.urban_api_gateway.get_countries_ids()
            results = await asyncio.gather(
                *(
                    self.urban_api_gateway.get_federal_cities(country_id)
                    for country_id in countries_ids
                )
            )
            self.federal_cities = np.concatenate([np.array(r) for r in results])

        return territory_id in self.federal_cities
