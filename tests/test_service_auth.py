from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from fastapi import HTTPException
from idu_service_auth import TokenRequestError

from app.common.auth.service_auth import ServiceAuth, ServiceAuthError
from app.common.models.popframe_models.services.popframe_models_api_service import (
    PopFrameModelApiService,
)

SECRET = "super-secret-value"


class FakeConfig:
    def __init__(self, values: dict):
        self.values = values

    def get(self, key):
        return self.values.get(key)


def _service_auth(*tokens: str) -> ServiceAuth:
    client = MagicMock()
    if len(tokens) == 1:
        # the real client caches the token, so every call returns the same one
        client.get_access_token = AsyncMock(return_value=tokens[0])
    else:
        client.get_access_token = AsyncMock(side_effect=list(tokens))
    return ServiceAuth(client)


def _api_service(service_auth: ServiceAuth, put: AsyncMock) -> PopFrameModelApiService:
    urban_api_handler = MagicMock()
    urban_api_handler.put = put
    return PopFrameModelApiService(
        FakeConfig({}), MagicMock(), urban_api_handler, service_auth
    )


def test_from_config_reports_missing_keys_without_values():
    config = FakeConfig({"KEYCLOAK_URL": "http://kc", "KEYCLOAK_CLIENT_SECRET": SECRET})

    with pytest.raises(ServiceAuthError) as exc_info:
        ServiceAuth.from_config(config)

    message = str(exc_info.value)
    assert "KEYCLOAK_REALM" in message
    assert "KEYCLOAK_CLIENT_ID" in message
    assert SECRET not in message


class RaisingConfig(FakeConfig):
    """Mimics iduconfig.Config, which raises ValueError on empty or missing keys."""

    def get(self, key):
        value = self.values.get(key)
        if not value:
            raise ValueError(f"No such env: {key}")
        return value


def test_from_config_aggregates_iduconfig_value_errors():
    config = RaisingConfig({"KEYCLOAK_URL": "http://kc", "KEYCLOAK_CLIENT_SECRET": SECRET})

    with pytest.raises(ServiceAuthError) as exc_info:
        ServiceAuth.from_config(config)

    message = str(exc_info.value)
    assert "KEYCLOAK_REALM" in message
    assert "KEYCLOAK_CLIENT_ID" in message
    assert SECRET not in message


def test_from_config_builds_client():
    config = FakeConfig(
        {
            "KEYCLOAK_URL": "http://kc",
            "KEYCLOAK_REALM": "realm",
            "KEYCLOAK_CLIENT_ID": "popframe",
            "KEYCLOAK_CLIENT_SECRET": SECRET,
        }
    )

    auth = ServiceAuth.from_config(config)

    assert isinstance(auth, ServiceAuth)
    assert SECRET not in repr(auth._client.config)


@pytest.mark.asyncio
async def test_get_token_wraps_keycloak_errors():
    client = MagicMock()
    client.get_access_token = AsyncMock(side_effect=TokenRequestError("down"))

    with pytest.raises(ServiceAuthError):
        await ServiceAuth(client).get_token()


@pytest.mark.asyncio
async def test_upload_hexagons_indicators_sends_service_token():
    put = AsyncMock(return_value={})
    service = _api_service(_service_auth("service-token"), put)

    await service.upload_hexagons_indicators(
        pd.Series({101: 5, 102: 7}), regional_scenario_id=10, territory_id=1
    )

    assert put.await_count == 2
    for call in put.await_args_list:
        assert call.kwargs["headers"] == {"Authorization": "Bearer service-token"}
        assert call.kwargs["endpoint_url"] == "/api/v1/scenarios/10/indicators_values"
        assert call.kwargs["data"]["indicator_id"] == 197


@pytest.mark.asyncio
async def test_upload_scenario_indicators_sends_service_token():
    put = AsyncMock(return_value={})
    service = _api_service(_service_auth("service-token"), put)
    service.get_cities_indicators_map = AsyncMock(
        return_value={"В агломерации": {"indicator_id": 42}}
    )

    await service.upload_scenario_indicators(
        pd.Series({"В агломерации": 3, "unknown": 1}),
        territory_id=1,
        regional_scenario_id=10,
    )

    put.assert_awaited_once()
    assert put.await_args.kwargs["headers"] == {"Authorization": "Bearer service-token"}
    assert put.await_args.kwargs["data"]["indicator_id"] == 42


@pytest.mark.asyncio
async def test_put_as_service_refreshes_token_once_on_401():
    put = AsyncMock(side_effect=[HTTPException(401), {"ok": True}])
    auth = _service_auth("stale-token", "fresh-token")
    service = _api_service(auth, put)

    result = await service._put_as_service("/api/v1/scenarios/10/indicators_values", {})

    assert result == {"ok": True}
    assert [c.kwargs["headers"]["Authorization"] for c in put.await_args_list] == [
        "Bearer stale-token",
        "Bearer fresh-token",
    ]
    assert auth._client.get_access_token.await_args_list[1].kwargs == {
        "force_refresh": True
    }


@pytest.mark.asyncio
async def test_put_as_service_does_not_retry_other_errors():
    put = AsyncMock(side_effect=HTTPException(500))
    service = _api_service(_service_auth("service-token"), put)

    with pytest.raises(HTTPException) as exc_info:
        await service._put_as_service("/api/v1/scenarios/10/indicators_values", {})

    assert exc_info.value.status_code == 500
    put.assert_awaited_once()
