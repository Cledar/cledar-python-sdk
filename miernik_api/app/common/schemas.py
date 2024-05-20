from pydantic import BaseModel, Field

class Credential(BaseModel):
    api_application: str = Field(
        description="Miernik identifier."
    )
    api_secret_key: str = Field(
        description="Miernik password."
    )
    api_type: str = Field(
        description="So far hardcoded value 'device'"
    )
    api_device_imei: str = Field(
        description="IMEI of the Miernik. According to the documention hashed with MD5."
    )
    api_active: bool = Field(
        description="Is user active. So far hardcoded to 'True'."
    )
    api_is_admin: bool = Field(
        description="Is user admin."
    )
