# mypy: disable-error-code=no-untyped-def
from unittest.mock import MagicMock

import pytest

from cledar.nonce import NonceService


@pytest.fixture(name="redis_client")
def fixture_redis_client():
    """Mock Redis client"""
    mock_redis = MagicMock()
    mock_redis._client = MagicMock()
    return mock_redis


@pytest.fixture(name="nonce_service")
def fixture_nonce_service(redis_client):
    """Create NonceService with a mocked Redis client"""
    return NonceService(redis_client)


def test_init(redis_client):
    """Test NonceService initialization"""
    service = NonceService(redis_client)

    assert service.redis_client == redis_client
    assert service.nonce_prefix == "nonce"
    assert service.default_ttl == 3600


def test_get_nonce_key(nonce_service):
    """Test nonce key generation"""
    nonce = "test-nonce-123"
    endpoint = "/api/payment"

    key = nonce_service._get_nonce_key(nonce, endpoint)

    assert key == "nonce:/api/payment:test-nonce-123"


@pytest.mark.asyncio
async def test_is_duplicate_first_time(nonce_service):
    """Test that the first use of nonce returns False (not duplicate)"""
    nonce = "unique-nonce-456"
    endpoint = "/api/transaction"

    # Mock Redis set to return True (key was set successfully)
    nonce_service.redis_client._client.set.return_value = True

    result = await nonce_service.is_duplicate(nonce, endpoint)

    assert result is False
    nonce_service.redis_client._client.set.assert_called_once_with(
        "nonce:/api/transaction:unique-nonce-456",
        "used",
        nx=True,
        ex=3600,
    )


@pytest.mark.asyncio
async def test_is_duplicate_second_time(nonce_service):
    """Test that second use of same nonce returns True (duplicate)"""
    nonce = "duplicate-nonce-789"
    endpoint = "/api/refund"

    # Mock Redis set to return None (key already exists)
    nonce_service.redis_client._client.set.return_value = None

    result = await nonce_service.is_duplicate(nonce, endpoint)

    assert result is True
    nonce_service.redis_client._client.set.assert_called_once_with(
        "nonce:/api/refund:duplicate-nonce-789",
        "used",
        nx=True,
        ex=3600,
    )


@pytest.mark.asyncio
async def test_is_duplicate_redis_not_initialized(nonce_service):
    """Test that RuntimeError is raised when a Redis client is not initialized"""
    nonce = "test-nonce"
    endpoint = "/api/test"

    # Simulate uninitialized Redis client
    nonce_service.redis_client._client = None

    with pytest.raises(RuntimeError, match="Redis client is not initialized"):
        await nonce_service.is_duplicate(nonce, endpoint)


@pytest.mark.asyncio
async def test_is_duplicate_different_endpoints(nonce_service):
    """Test that the same nonce is independent across different endpoints"""
    nonce = "same-nonce"
    endpoint1 = "/api/endpoint1"
    endpoint2 = "/api/endpoint2"

    # Both calls should treat the nonce as new (not duplicate)
    nonce_service.redis_client._client.set.return_value = True

    result1 = await nonce_service.is_duplicate(nonce, endpoint1)
    result2 = await nonce_service.is_duplicate(nonce, endpoint2)

    assert result1 is False
    assert result2 is False

    # Verify both keys were set with different endpoint prefixes
    assert nonce_service.redis_client._client.set.call_count == 2
    calls = nonce_service.redis_client._client.set.call_args_list
    assert calls[0][0][0] == "nonce:/api/endpoint1:same-nonce"
    assert calls[1][0][0] == "nonce:/api/endpoint2:same-nonce"


@pytest.mark.asyncio
async def test_is_duplicate_custom_ttl(redis_client):
    """Test that custom TTL is used correctly"""
    service = NonceService(redis_client)
    service.default_ttl = 7200  # 2 hours

    nonce = "test-nonce"
    endpoint = "/api/test"

    redis_client._client.set.return_value = True

    await service.is_duplicate(nonce, endpoint)

    redis_client._client.set.assert_called_once_with(
        "nonce:/api/test:test-nonce",
        "used",
        nx=True,
        ex=7200,
    )
