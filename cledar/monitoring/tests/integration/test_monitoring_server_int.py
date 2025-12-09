# pylint: disable=redefined-outer-name, unreachable
import logging
from collections.abc import Awaitable, Callable

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from cledar.monitoring import EndpointFilter, MonitoringServer, MonitoringServerConfig

# ------------------------------------------------------------------------------
#  Fixtures
# ------------------------------------------------------------------------------


@pytest.fixture
def test_app() -> FastAPI:
    """Creates a FastAPI app with configured monitoring endpoints."""
    app = FastAPI()
    config = MonitoringServerConfig(
        readiness_checks={"db": lambda: True},
        liveness_checks={"heartbeat": lambda: True},
    )
    server = MonitoringServer("127.0.0.1", 8000, config)
    server.add_paths(app)
    return app


ASGIClientFactory = Callable[[FastAPI], Awaitable[AsyncClient]]


@pytest.fixture
def asgi_client_factory() -> ASGIClientFactory:
    """Factory to create an httpx.AsyncClient for ASGI apps (httpx>=0.28)."""

    async def _make_client(app: FastAPI) -> AsyncClient:
        transport = ASGITransport(app=app)
        return AsyncClient(transport=transport, base_url="http://test")

    return _make_client


# ------------------------------------------------------------------------------
#  Readiness & Liveness Endpoints
# ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_readiness_ok(
    test_app: FastAPI, asgi_client_factory: ASGIClientFactory
) -> None:
    """Readiness endpoint returns OK when all checks pass."""
    async with await asgi_client_factory(test_app) as client:
        response = await client.get("/healthz/readiness")

    data = response.json()
    assert response.status_code == 200
    assert data["status"] == "ok"
    assert data["checks"]["db"] is True


@pytest.mark.asyncio
async def test_liveness_ok(
    test_app: FastAPI, asgi_client_factory: ASGIClientFactory
) -> None:
    """Liveness endpoint returns OK when all checks pass."""
    async with await asgi_client_factory(test_app) as client:
        response = await client.get("/healthz/liveness")

    data = response.json()
    assert response.status_code == 200
    assert data["status"] == "ok"
    assert data["checks"]["heartbeat"] is True


@pytest.mark.asyncio
async def test_readiness_failing_check(asgi_client_factory: ASGIClientFactory) -> None:
    """Readiness endpoint returns 503 and error status if a check fails."""
    app = FastAPI()
    config = MonitoringServerConfig(readiness_checks={"db": lambda: False})
    server = MonitoringServer("127.0.0.1", 8000, config)
    server.add_paths(app)

    async with await asgi_client_factory(app) as client:
        response = await client.get("/healthz/readiness")

    data = response.json()
    assert response.status_code == 503
    assert data["status"] == "error"
    assert data["checks"]["db"] is False


@pytest.mark.asyncio
async def test_readiness_with_exception(asgi_client_factory: ASGIClientFactory) -> None:
    """Readiness returns 503 if a check raises an exception."""

    def failing_check() -> bool:
        raise RuntimeError("DB not reachable")
        return False

    app = FastAPI()
    config = MonitoringServerConfig(readiness_checks={"db": failing_check})
    server = MonitoringServer("127.0.0.1", 8000, config)
    server.add_paths(app)

    async with await asgi_client_factory(app) as client:
        response = await client.get("/healthz/readiness")

    data = response.json()
    assert response.status_code == 503
    assert data["status"] == "error"
    assert "DB not reachable" in data["message"]


# ------------------------------------------------------------------------------
#  Metrics Endpoint
# ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metrics_endpoint_returns_prometheus_format(
    test_app: FastAPI, asgi_client_factory: ASGIClientFactory
) -> None:
    """Metrics endpoint should return valid Prometheus output."""
    async with await asgi_client_factory(test_app) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "python_info" in response.text  # standard metric


# ------------------------------------------------------------------------------
#  Logging Filter
# ------------------------------------------------------------------------------


def test_endpoint_filter_excludes_health_paths() -> None:
    """EndpointFilter should exclude healthz paths from logs."""
    filter_ = EndpointFilter(["/healthz/readiness", "/healthz/liveness"])

    record_excluded = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="GET /healthz/readiness 200 OK",
        args=(),
        exc_info=None,
    )
    assert filter_.filter(record_excluded) is False

    record_included = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="GET /metrics 200 OK",
        args=(),
        exc_info=None,
    )
    assert filter_.filter(record_included) is True
