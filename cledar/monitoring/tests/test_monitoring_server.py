import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from cledar.monitoring import MonitoringServer, MonitoringServerConfig

HOST = "localhost"
PORT = 9999


class ReadinessFlag:
    def __init__(self) -> None:
        self.is_ready = False

    def mark_ready(self) -> None:
        self.is_ready = True

    def check_if_ready(self) -> bool:
        return self.is_ready


@pytest.fixture
def readiness_flag() -> ReadinessFlag:
    _readiness_flag = ReadinessFlag()
    return _readiness_flag


@pytest.fixture
def app(readiness_flag: ReadinessFlag) -> FastAPI:
    _app = FastAPI()
    default_readiness_checks = dict({"is_ready": readiness_flag.check_if_ready})
    config = MonitoringServerConfig(default_readiness_checks)
    monitoring_server = MonitoringServer(HOST, PORT, config)
    monitoring_server.add_paths(_app)
    return _app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    _client = TestClient(app)
    return _client


def test_liveness(client: TestClient) -> None:
    response = client.get("/healthz/liveness")
    assert response.status_code == 200
    assert response.text == '{"status": "ok", "checks": {}}'


def test_readiness(client: TestClient, readiness_flag: ReadinessFlag) -> None:
    response = client.get("/healthz/readiness")
    assert response.status_code == 503
    assert response.text == '{"status": "error", "checks": {"is_ready": false}}'

    readiness_flag.mark_ready()

    response = client.get("/healthz/readiness")
    assert response.status_code == 200
    assert response.text == '{"status": "ok", "checks": {"is_ready": true}}'
