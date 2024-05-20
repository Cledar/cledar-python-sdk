from uuid import uuid4
from datetime import datetime
from .schemas import Credential

def generate_uuid() -> str:
    return str(uuid4())

def get_current_time() -> datetime:
    return datetime.now()

def create_credentials(imei: str) -> Credential:
    return Credential(
        api_application=generate_uuid(),
        api_secret_key=generate_uuid(),
        api_type="device",
        api_device_imei=imei,
        api_active=True,
        api_is_admin=False
    )
