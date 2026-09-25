from typing import Awaitable, Callable, Protocol, TypeVar

import requests
from fastapi import HTTPException
from idu_service_auth import KeycloakAuthError, KeycloakTokenClient, KeycloakTokenConfig
from iduconfig import Config
from loguru import logger

KEYCLOAK_CONFIG_KEYS = (
    "KEYCLOAK_URL",
    "KEYCLOAK_REALM",
    "KEYCLOAK_CLIENT_ID",
    "KEYCLOAK_CLIENT_SECRET",
)

T = TypeVar("T")


class ServiceAuthError(RuntimeError):
    pass


class AuthHeadersProvider(Protocol):
    async def __call__(self, *, force_refresh: bool = False) -> dict[str, str]: ...


def static_bearer_headers(token: str) -> AuthHeadersProvider:
    """Headers provider for a fixed token that cannot be refreshed, e.g. a user's token."""

    async def provide(*, force_refresh: bool = False) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    return provide


async def send_with_fresh_headers(
    send: Callable[[dict[str, str]], requests.Response],
    auth_headers: AuthHeadersProvider,
) -> requests.Response:
    """Sends a request with headers obtained right before it, refreshing them once on 401."""
    response = send(await auth_headers())
    if response.status_code != 401:
        return response
    logger.warning("Urban API rejected token, refreshing")
    return send(await auth_headers(force_refresh=True))


def _get_optional(config: Config, key: str) -> str | None:
    # iduconfig raises ValueError on empty/missing keys; collect them all instead of failing on the first one
    try:
        return config.get(key)
    except ValueError:
        return None


class ServiceAuth:
    """
    Machine-to-machine auth against Keycloak (client credentials flow).
    Used for background (Kafka) processing where no user token is available.
    """

    def __init__(self, client: KeycloakTokenClient):
        self._client = client

    @classmethod
    def from_config(cls, config: Config) -> "ServiceAuth":
        values = {key: _get_optional(config, key) for key in KEYCLOAK_CONFIG_KEYS}
        missing = [key for key, value in values.items() if not value]
        if missing:
            raise ServiceAuthError(
                f"Missing Keycloak service credentials in config: {', '.join(missing)}"
            )
        return cls(
            KeycloakTokenClient(
                KeycloakTokenConfig(
                    auth_server_url=values["KEYCLOAK_URL"],
                    realm=values["KEYCLOAK_REALM"],
                    client_id=values["KEYCLOAK_CLIENT_ID"],
                    client_secret=values["KEYCLOAK_CLIENT_SECRET"],
                )
            )
        )

    async def get_token(self, *, force_refresh: bool = False) -> str:
        try:
            return await self._client.get_access_token(force_refresh=force_refresh)
        except KeycloakAuthError as e:
            logger.error(f"Failed to obtain Keycloak service token: {e}")
            raise ServiceAuthError(f"Keycloak service token unavailable: {e}") from e

    async def get_headers(self, *, force_refresh: bool = False) -> dict[str, str]:
        token = await self.get_token(force_refresh=force_refresh)
        return {"Authorization": f"Bearer {token}"}

    async def call_with_headers(
        self, request: Callable[[dict[str, str]], Awaitable[T]]
    ) -> T:
        """Runs request with service auth headers, refreshing the token once on 401."""
        try:
            return await request(await self.get_headers())
        except HTTPException as e:
            if e.status_code != 401:
                raise
            logger.warning("Urban API rejected service token, refreshing")
            return await request(await self.get_headers(force_refresh=True))

    async def aclose(self) -> None:
        await self._client.aclose()
