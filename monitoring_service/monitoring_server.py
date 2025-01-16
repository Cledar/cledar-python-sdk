# pylint: disable=broad-exception-caught
import json
import logging
import logging.config
import threading
from typing import Callable, List

import prometheus_client
import uvicorn
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic.dataclasses import dataclass


def create_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app


@dataclass
class MonitoringServerConfig:
    readiness_checks: dict[str, Callable[[], bool]]
    liveness_checks: dict[str, Callable[[], bool]] | None = None


class EndpointFilter(logging.Filter):
    def __init__(self, paths_excluded_for_logging: List[str]):
        super().__init__()
        self.paths_excluded_for_logging = paths_excluded_for_logging

    def filter(self, record: logging.LogRecord) -> bool:
        return not any(
            path in record.getMessage() for path in self.paths_excluded_for_logging
        )


class MonitoringServer:
    # TODO(Husia) remove /health after finishing subtasks of KSR-339 - task KSR-700
    PATHS_EXCLUDED_FOR_LOGGING = ["/health", "/healthz/readiness", "/healthz/liveness"]

    def __init__(
        self,
        host: str,
        port: int,
        config: MonitoringServerConfig,
    ):
        self.config = config
        self.host = host
        self.port = port
        logging.getLogger("uvicorn.access").addFilter(
            EndpointFilter(self.PATHS_EXCLUDED_FOR_LOGGING)
        )

    def add_paths(self, app: FastAPI) -> None:
        # TODO(Husia) Remove /health after finishing subtasks of KSR-339 - task KSR-700
        @app.get("/health")
        async def get_health() -> Response:
            return Response(
                status_code=200,
            )

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

    async def _get_healthz_response(
        self, checks: dict[str, Callable[[], bool]] | None
    ) -> Response:
        try:
            results = (
                {check_name: check_fn() for check_name, check_fn in checks.items()}
                if checks
                else {}
            )

            status = "error"
            status_code = 503
            if all(results.values()):
                status = "ok"
                status_code = 200

            data = {"status": status, "checks": results}
            data_json = json.dumps(data)
            return Response(content=data_json, status_code=status_code)

        except Exception as e:
            data = {"status": "error", "message": str(e)}
            data_json = json.dumps(data)
            return Response(content=data_json, status_code=503)

    def start_monitoring_server(self) -> None:
        local_app = create_app()
        self.add_paths(local_app)
        server_thread = threading.Thread(
            target=run_monitoring_server,
            args=(self.host, self.port, local_app),
        )
        server_thread.daemon = True  # to ensure it dies with the main thread
        server_thread.start()
        logging.info("Monitoring server listening at %s:%s.", self.host, self.port)


def run_monitoring_server(host: str, port: int, local_app: FastAPI) -> None:
    uvicorn.run(local_app, host=host, port=port)


# TODO(Husia) Remove this method after finishing subtasks of KSR-339 - task KSR-700
def start_monitoring_server(host: str, port: int) -> None:
    default_readiness_checks = dict({"default": lambda: True})
    config = MonitoringServerConfig(default_readiness_checks)
    monitoring_server = MonitoringServer(host, port, config)
    monitoring_server.start_monitoring_server()
