from fastapi import FastAPI
from .auth.router import router as auth_router
from .device.router import router as device_router

app = FastAPI()

app.include_router(auth_router)
app.include_router(device_router)
