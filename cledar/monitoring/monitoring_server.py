"""Prometheus monitoring and health checks server implementation."""

from __future__ import annotations

import inspect
import json
import logging
import threading
from collections.abc import Awaitable, Callable

import prometheus_client
import uvicorn
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic.dataclasses import dataclass

type CheckResult = bool | Awaitable[bool]
type Check = Callable[[], CheckResult]
type Checks = dict[str, Check]


def _create_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app


def _run_monitoring_server(host: str, port: int, app: FastAPI) -> None:
    uvicorn.run(app, host=host, port=port)


@dataclass
class MonitoringServerConfig:
    """Configuration for the MonitoringServer.

    Args:
        readiness_checks: A dictionary of name to callable for readiness checks.
        liveness_checks: An optional dictionary for liveness checks.

    """

    readiness_checks: Checks
    liveness_checks: Checks | None = None


class EndpointFilter(logging.Filter):
    """Filter for logging that excludes certain paths."""

    def __init__(self, paths_excluded_for_logging: list[str]):
        """Initialize the EndpointFilter.

        Args:
            paths_excluded_for_logging: List of paths to exclude from logs.

        """
        super().__init__()
        self.paths_excluded_for_logging = paths_excluded_for_logging

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter log records based on path exclusions.

        Args:
            record: The log record to check.

        Returns:
            bool: True if record should be logged, False otherwise.

        """
        return not any(
            path in record.getMessage() for path in self.paths_excluded_for_logging
        )


class MonitoringServer:
    """A server that exposes Prometheus metrics and health check endpoints."""

    PATHS_EXCLUDED_FOR_LOGGING = ["/healthz/readiness", "/healthz/liveness"]

    def __init__(
        self,
        host: str,
        port: int,
        config: MonitoringServerConfig,
    ):
        """Initialize the MonitoringServer.

        Args:
            host: The host to bind the server to.
            port: The port to bind the server to.
            config: The server configuration.

        """
        self.config = config
        self.host = host
        self.port = port
        logging.getLogger("uvicorn.access").addFilter(
            EndpointFilter(self.PATHS_EXCLUDED_FOR_LOGGING)
        )

    def add_paths(self, app: FastAPI) -> None:
        """Add monitoring and health check endpoints to the FastAPI application.

        Args:
            app: The FastAPI application to add routes to.

        """

        @app.get("/metrics")
        async def get_metrics() -> Response:
            return Response(
                content=prometheus_client.generate_latest(),
                media_type=prometheus_client.CONTENT_TYPE_LATEST,
            )

        @app.get("/healthz/liveness")
        async def get_healthz_liveness() -> Response:
            return await self._get_healthz_response(self.config.liveness_checks)

        @app.get("/healthz/readiness")
        async def get_healthz_readiness() -> Response:
            return await self._get_healthz_response(self.config.readiness_checks)

    async def _get_healthz_response(self, checks: Checks | None) -> Response:
        """Build the health check response payload and HTTP status.

        Args:
            checks: Mapping of check names to callables.

        Returns:
            FastAPI response with health status and check results.

        """
        try:
            results = await self._collect_check_results(checks)
            status, status_code = self._evaluate_health_status(results)

            data = {"status": status, "checks": results}
            data_json = json.dumps(data)
            return Response(content=data_json, status_code=status_code)

        except Exception as e:
            data = {"status": "error", "message": str(e)}
            data_json = json.dumps(data)
            return Response(content=data_json, status_code=503)

    async def _collect_check_results(self, checks: Checks | None) -> dict[str, bool]:
        """Run health checks and return their boolean results.

        Args:
            checks: Mapping of check names to callables.

        Returns:
            A dictionary of check names to boolean results.

        """
        results: dict[str, bool] = {}
        if not checks:
            return results

        for check_name, check_fn in checks.items():
            result = check_fn()
            results[check_name] = (
                await result if inspect.isawaitable(result) else result
            )

        return results

    def _evaluate_health_status(self, results: dict[str, bool]) -> tuple[str, int]:
        """Evaluate overall health status and HTTP code based on results.

        Args:
            results: Mapping of check names to boolean results.

        Returns:
            A tuple of status string and HTTP status code.

        """
        if not results or all(results.values()):
            return "ok", 200

        return "error", 503

    def start_monitoring_server(self) -> None:
        """Start the monitoring server in a background thread."""
        local_app = _create_app()
        self.add_paths(local_app)
        server_thread = threading.Thread(
            target=_run_monitoring_server,
            args=(self.host, self.port, local_app),
        )
        server_thread.daemon = True  # to ensure it dies with the main thread
        server_thread.start()
        logging.info("Monitoring server listening at %s:%s.", self.host, self.port)
