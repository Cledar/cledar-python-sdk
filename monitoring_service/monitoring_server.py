import json
import logging
import logging.config
import threading
from collections.abc import Callable

import prometheus_client
import uvicorn
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic.dataclasses import dataclass


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
    readiness_checks: dict[str, Callable[[], bool]]
    liveness_checks: dict[str, Callable[[], bool]] | None = None


class EndpointFilter(logging.Filter):
    def __init__(self, paths_excluded_for_logging: list[str]):
        super().__init__()
        self.paths_excluded_for_logging = paths_excluded_for_logging

    def filter(self, record: logging.LogRecord) -> bool:
        return not any(
            path in record.getMessage() for path in self.paths_excluded_for_logging
        )


class MonitoringServer:
    PATHS_EXCLUDED_FOR_LOGGING = ["/healthz/readiness", "/healthz/liveness"]

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
        local_app = _create_app()
        self.add_paths(local_app)
        server_thread = threading.Thread(
            target=_run_monitoring_server,
            args=(self.host, self.port, local_app),
        )
        server_thread.daemon = True  # to ensure it dies with the main thread
        server_thread.start()
        logging.info("Monitoring server listening at %s:%s.", self.host, self.port)
