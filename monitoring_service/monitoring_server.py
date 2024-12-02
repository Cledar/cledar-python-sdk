# pylint: disable=broad-exception-caught
from typing import Callable, List
import logging
import logging.config
import threading
import json
import prometheus_client
from pydantic.dataclasses import dataclass
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


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
            try:
                data = {"status": "ok"}
                data_json = json.dumps(data)
                return Response(content=data_json, status_code=200)
            except Exception as e:
                data = {"status": "error", "message": str(e)}
                data_json = json.dumps(data)
                return Response(content=data_json, status_code=503)

        @app.get("/healthz/readiness")
        async def get_healthz_readiness() -> Response:
            try:
                checks = {
                    check_name: check_fn()
                    for check_name, check_fn in self.config.readiness_checks.items()
                }
                if all(checks.values()):
                    data = {"status": "ok", "checks": checks}
                    data_json = json.dumps(data)
                    return Response(content=data_json, status_code=200)
                failed = {
                    check_name: check_value
                    for check_name, check_value in checks.items()
                    if not check_value
                }
                data = {"status": "error", "failed_checks": failed}
                data_json = json.dumps(data)
                return Response(content=data_json, status_code=503)
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
