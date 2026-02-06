"""Monitoring module for Prometheus metrics and health checks."""

from collections.abc import Awaitable, Callable

from .monitoring_server import EndpointFilter, MonitoringServer, MonitoringServerConfig

type CheckResult = bool | Awaitable[bool]
type Check = Callable[[], CheckResult]
type Checks = dict[str, Check]

__all__ = [
    "MonitoringServer",
    "MonitoringServerConfig",
    "EndpointFilter",
    "CheckResult",
    "Check",
    "Checks",
]
