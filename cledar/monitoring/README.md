# Monitoring Service

Monitoring service provides endpoints for healthchecks and Prometheus metrics.

This module creates a monitoring server with uvicorn endpoints for managing health of applications.

## Endpoints

- GET /healthz/liveness ->
Provides information if the app is alive. 
Example return:
```json
{"status": "ok", "checks": {}}
```
- GET /healthz/readiness -> 
Provides information if the app is ready and which components are active. 
Example return:
```json
{"status": "ok", "checks": {"kafka_alive": true, "model_ready": true, "redis_alive": true}}
```
- GET /metrics ->
Provides metrics collected by Prometheus client, to be used in metrics visualization client f.e. Grafana.

## Usage

In your app you have to define readiness checks - services that need to be running for app to work properly
f.e. S3, Kafka, model loading, etc.
This is usually solved by creating MonitoringContext object. 

```python
class MonitoringContext:
    def __init__(self) -> None:
        self.kafka_client: Optional[BaseKafkaClient] = None
        self.redis_client: Optional[RedisService] = None
        self._model_ready_flag: bool = False

    def prepare_readiness_checks(self) -> dict[str, Callable[[], bool]]:
        return {
            "kafka_alive": self._kafka_check,
            "model_ready": lambda: self._model_ready_flag,
            "redis_alive": self._redis_alive,
        }

    def _redis_alive(self) -> bool:
        if self.redis_client is None:
            return False
        return self.redis_client.is_alive()

    def _kafka_check(self) -> bool:
        if self.kafka_client is None:
            return False
        return self.kafka_client.is_alive()

    def set_model_ready_flag(self, flag: bool) -> None:
        self._model_ready_flag = flag
```

Now you can prepare your monitoring server by running in __main__:

```python
monitoring_context = MonitoringContext()
monitoring_config = MonitoringServerConfig(
    monitoring_context.prepare_readiness_checks()
)
monitoring_server = MonitoringServer(
    host="0.0.0.0",
    port=8000,
    config=monitoring_config,
)
monitoring_server.start_monitoring_server()
```