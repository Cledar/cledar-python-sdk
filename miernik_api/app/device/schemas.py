from typing import List
from datetime import datetime
from pydantic import BaseModel, create_model, Field

class HashRequest(BaseModel):
    audio_hash: str = Field(
        description="Encoded peaks from which final hashes are generated."
    )
    duration_ms: int = Field(
        description="Not sure."
    )
    start_ms: int = Field(
        description="Not sure, start time of the audio? in unix timestamp format."
    )

class DeviceInfoRequest(BaseModel):
    battery_percentage: float = Field(
        description="Battery percentage of the device."
    )
    geo_lat: float | None = Field(
        description="Latitude of the device."
    )
    geo_lng: float | None = Field(
        description="Longitude of the device."
    )
    imei: str = Field(
        description="IMEI of the Miernik. According to the documention hashed with MD5."
    )
    connection_type: str | None = Field(
        description="Type of the connection, seen 'MOBILE' and 'WIFI'."
    )
    app_version: str | None = Field(
        description="Version of the application for audio matching (according to the documentation)."
    )
    os_version: str | None = Field(
        description="Version of the Android (according to the documentation)."
    )
    imsi: str | None = Field(
        description="Not sure."
    )
    activity_type: str | None = Field(
        description="Not sure."
    )
    startup_time: int | None = Field(
        description="Not sure."
    )

class DeviceStatusRequest(BaseModel):
    status: DeviceInfoRequest
    audio_hashes: List[HashRequest]

class ConnectedStatusRequest(BaseModel):
    imei: str = Field(
        description="IMEI of the Miernik. According to the documention hashed with MD5."
    )

class EventsApp(BaseModel):
    device_id: str
    start_time: str | datetime #TODO(pkomor): check format
    end_time: str | datetime #TODO(pkomor): check format
    package_name: str | None
    app_name: str
    active: bool #TODO(pkomor): check if None or just true/false

class EventsAppRequest(BaseModel):
    events: List[EventsApp]

class EventsWww(BaseModel):
    device_id: str
    start_time: str | datetime #TODO(pkomor): check format
    end_time: str | datetime #TODO(pkomor): check format
    package_name: str | None
    domain: str
    url: str
    ontop: bool #TODO(pkomor): check if None or just true/false

class EventsWwwRequest(BaseModel):
    events: List[EventsWww]

class Device(BaseModel):
    imei: str
    model: str
    os: str

class DeviceRegisterRequest(BaseModel):
    device: Device

class BaseFields(BaseModel):
    id: int = Field(
        description="Unique identifier of the message within the endpoint execution."
    )
    created_at: datetime = Field(
        description="Time of the message creation."
    )
    updated_at: datetime = Field(
        description="Time of the message update. For now the same as created_at."
    )
    deleted_at: datetime | None = Field(
        description="Time of the message deletion. For now always None."
    )

# dynamically created models to utilize multiple inheritance
DeviceInfoMessage = create_model(
    'DeviceInfoMessage',
    uuid = (str, Field(
        ..., description="Unique identifier of the messages within the endpoint execution."
    )),
    send_time = (datetime | None, Field(
        ..., description="Time of the message sending. For now always None."
    )),
    __base__= (BaseFields, DeviceInfoRequest)
)

HashMessage = create_model(
    'HashMessage',
    uuid = (str, Field(
        ..., description="Unique identifier of the messages within the endpoint execution. Same as in DeviceInfoMessage."
    )),
    imei = (str, Field(
        ..., description="IMEI of the Miernik. According to the documention hashed with MD5."
    )),
    __base__= (BaseFields, HashRequest)
)

ConnectedStatusMessage = create_model(
    'ConnectedStatusMessage',
    time = (datetime, Field(
        ..., description="Time of the message creation."
    )),
    __base__= (BaseFields, ConnectedStatusRequest)
)

EventsAppMessage = create_model(
    'EventsAppMessage',
    __base__= (BaseFields, EventsApp)
)

EventsWwwMessage = create_model(
    'EventsWwwMessage',
    __base__= (BaseFields, EventsWww)
)
