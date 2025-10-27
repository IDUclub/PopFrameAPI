import aiohttp
from loguru import logger

from app.common.exceptions.http_exception_wrapper import http_exception


class APIHandler:

    def __init__(
        self,
        base_url: str,
    ) -> None:
        """Initialisation function

        Args:
            base_url (str): Base api url
        Returns:
            None
        """

        self.base_url = base_url

    @staticmethod
    async def check_request_params(
        params: dict[str, str | int | float | bool] | None,
    ) -> dict | None:
        """
        Function checks request parameters
        Args:
            params (dict[str, str | int | float | bool]  | None): Request parameters
        Returns:
            dict | None: Returns modified parameters if they are not empty, otherwise returns None
        """

        if params:
            for key, param in params.items():
                if isinstance(param, bool):
                    params[key] = {True: "true", False: "false"}[param]
            return params
        return params

    @staticmethod
    async def _check_response_status(
        response: aiohttp.ClientResponse,
    ) -> list | dict | None:
        """Function handles response

        Args:
            response (aiohttp.ClientResponse): Response object
        Returns:
            list|dict: requested data with additional info, e.g. {"retry": True | False, "response": {response.json}}
        Raises:
            http_exception with response status code from API
        """

        if response.status in (200, 201):
            result = await response.json(content_type="application/json")
            return result
        elif response.status == 500:
            if response.content_type == "application/json":
                response_info = await response.json()
                if "error" in response_info:
                    if "reset by peer" in response_info["error"]:
                        return None
            else:
                response_info = await response.text()
            exception = http_exception(
                response.status,
                "Couldn't extract request from API",
                _input=response.url.__str__(),
                _detail=response_info,
            )
            logger.error(exception)
            raise exception
        else:
            exception = http_exception(
                response.status,
                "Couldn't get data from API",
                _input=response.url.__str__(),
                _detail=await response.json(),
            )
            logger.error(exception)
            raise exception

    async def get(
        self,
        endpoint_url: str,
        headers: dict | None = None,
        params: dict | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict | list:
        """Function to get data from api

        Args:
            endpoint_url (str): Endpoint url
            headers (dict | None): Headers
            params (dict | None): Query parameters
            session (aiohttp.ClientSession | None): Session to use
        Returns:
            dict | list: Response data as python object
        """

        if not session:
            async with aiohttp.ClientSession() as session:
                return await self.get(
                    endpoint_url=endpoint_url,
                    headers=headers,
                    params=params,
                    session=session,
                )
        url = self.base_url + endpoint_url
        params = await self.check_request_params(params)
        async with session.get(url=url, headers=headers, params=params) as response:
            result = await self._check_response_status(response)
            if result is None:
                return await self.get(
                    endpoint_url=endpoint_url,
                    headers=headers,
                    params=params,
                    session=session,
                )
            return result

    async def post(
        self,
        endpoint_url: str,
        headers: dict | None = None,
        params: dict | None = None,
        data: dict | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict | list:
        """Function to post data from api

        Args:
            endpoint_url (str): Endpoint url
            headers (dict | None): Headers
            params (dict | None): Query parameters
            data (dict | None): Request data
            session (aiohttp.ClientSession | None): Session to use
        Returns:
            dict | list: Response data as python object
        """

        if not session:
            async with aiohttp.ClientSession() as session:
                return await self.post(
                    endpoint_url=endpoint_url,
                    headers=headers,
                    params=params,
                    data=data,
                    session=session,
                )
        url = self.base_url + endpoint_url
        params = await self.check_request_params(params)
        async with session.post(
            url=url,
            headers=headers,
            params=params,
            json=data,
        ) as response:
            result = await self._check_response_status(response)
            if result is None:
                return await self.post(
                    endpoint_url=endpoint_url,
                    headers=headers,
                    params=params,
                    session=session,
                )
            return result

    async def put(
        self,
        endpoint_url: str,
        headers: dict | None = None,
        params: dict | None = None,
        data: dict | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict | list:
        """Function to post data from api

        Args:
            endpoint_url (str): Endpoint url
            headers (dict | None): Headers
            params (dict | None): Query parameters
            data (dict | None): Request data
            session (aiohttp.ClientSession | None): Session to use
        Returns:
            dict | list: Response data as python object
        """

        if not session:
            async with aiohttp.ClientSession() as session:
                return await self.put(
                    endpoint_url=endpoint_url,
                    headers=headers,
                    params=params,
                    data=data,
                    session=session,
                )
        url = self.base_url + endpoint_url
        params = await self.check_request_params(params)
        async with session.put(
            url=url,
            headers=headers,
            params=params,
            json=data,
        ) as response:
            result = await self._check_response_status(response)
            if result is None:
                return await self.put(
                    endpoint_url=endpoint_url,
                    headers=headers,
                    params=params,
                    session=session,
                )
            return result

    async def delete(
        self,
        endpoint_url: str,
        headers: dict | None = None,
        params: dict | None = None,
        data: dict | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict | list:
        """Function to post data from api

        Args:
            endpoint_url (str): Endpoint url
            headers (dict | None): Headers
            params (dict | None): Query parameters
            data (dict | None): Request data
            session (aiohttp.ClientSession | None): Session to use
        Returns:
            dict | list: Response data as python object
        """

        if not session:
            async with aiohttp.ClientSession() as session:
                return await self.delete(
                    endpoint_url=endpoint_url,
                    headers=headers,
                    params=params,
                    data=data,
                    session=session,
                )
        url = self.base_url + endpoint_url
        params = await self.check_request_params(params)
        async with session.delete(
            url=url,
            headers=headers,
            params=params,
            data=data,
        ) as response:
            result = await self._check_response_status(response)
            if result is None:
                return await self.delete(
                    endpoint_url=endpoint_url,
                    headers=headers,
                    params=params,
                    session=session,
                )
            return result
