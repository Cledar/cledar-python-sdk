import logging
import logging.config
import threading
import prometheus_client
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "/health" not in record.getMessage()


logging.getLogger("uvicorn.access").addFilter(EndpointFilter())


@app.get("/metrics")
async def get_metrics():
    return Response(
        content=prometheus_client.generate_latest(),
        media_type=prometheus_client.CONTENT_TYPE_LATEST,
    )


@app.get("/health")
async def get_health():
    return Response(
        status_code=200,
    )


def run_monitoring_server(host, port):
    app_server = f"{__name__}:app"
    uvicorn.run(app_server, host=host, port=port)


def start_monitoring_server(host, port):
    server_thread = threading.Thread(
        target=run_monitoring_server,
        args=(host, port),
    )
    server_thread.daemon = True  # to ensure it dies with the main thread
    server_thread.start()
    logging.info("Monitoring server listening at %s:%s.", host, port)
