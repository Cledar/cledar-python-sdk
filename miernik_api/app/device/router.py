from fastapi import APIRouter, status
from fastapi.encoders import jsonable_encoder
from .utils import (
    generate_base_fields,
    success_response
)
from ..common.utils import (
    generate_uuid,
    get_current_time,
    create_credentials
)
from .schemas import (
    DeviceStatusRequest,
    DeviceInfoMessage,
    HashMessage,
    ConnectedStatusRequest,
    ConnectedStatusMessage,
    EventsAppRequest,
    EventsAppMessage,
    EventsWwwRequest,
    EventsWwwMessage,
    DeviceRegisterRequest,
)
from ..dependencies import (
    SessionDep,
    Depends,
    get_current_session
)
from ..exceptions import (
    forbidden_error
)
from ..services.kafka_service import prepare_and_send_messages

router = APIRouter(
    prefix="/device",
    tags=["device"],
    dependencies=[Depends(get_current_session)]
)

@router.post("/status", status_code=status.HTTP_200_OK, response_model=dict)
async def send_status(device_status: DeviceStatusRequest):
    now = get_current_time()
    uuid = generate_uuid()
    messages_device = [
        DeviceInfoMessage(
            **jsonable_encoder(generate_base_fields(id_counter=1, time=now)),
            **jsonable_encoder(device_status.status),
            uuid=uuid,
            send_time=None
        )
    ]
    await prepare_and_send_messages("telemetry-device-status-device-info", messages_device)
    messages_fingerprints = [
        HashMessage(
            **jsonable_encoder(generate_base_fields(id_counter, time=now)),
            **jsonable_encoder(audio_hash),
            uuid=uuid,
            imei=device_status.status.imei
        )
        for id_counter, audio_hash in enumerate(device_status.audio_hashes, start=1)
    ]
    await prepare_and_send_messages("telemetry-device-status-fingerprints", messages_fingerprints)
    return success_response

@router.post("/connection_status", status_code=status.HTTP_200_OK, response_model=dict)
async def send_connection_status(connection_status: ConnectedStatusRequest):
    now = get_current_time()
    messages = [
        ConnectedStatusMessage(
            **jsonable_encoder(generate_base_fields(id_counter=1, time=now)),
            imei=connection_status.imei,
            time=now,
            errorcode=None,
            httperrorcode=None
        )
    ]
    await prepare_and_send_messages("telemetry-device-status-connection", messages)
    return success_response

@router.post("/app_events", status_code=status.HTTP_200_OK, response_model=dict)
async def send_app_events(app_events: EventsAppRequest):
    now = get_current_time()
    messages = [
        EventsAppMessage(
            **jsonable_encoder(generate_base_fields(id_counter, time=now)),
            **jsonable_encoder(event)
        )
        for id_counter, event in enumerate(app_events.events, start=1)
    ]
    await prepare_and_send_messages("telemetry-device-events-app", messages)
    return success_response

@router.post("/www_events", status_code=status.HTTP_200_OK, response_model=dict)
async def send_www_events(www_events: EventsWwwRequest):
    now = get_current_time()
    messages = [
            EventsWwwMessage(
                **jsonable_encoder(generate_base_fields(id_counter, time=now)),
                **jsonable_encoder(event)
            )
        for id_counter, event in enumerate(www_events.events, start=1)
    ]
    await prepare_and_send_messages("telemetry-device-events-www", messages)
    return success_response

@router.post("/register", status_code=status.HTTP_201_CREATED, response_model=dict)
async def register_device(register_data: DeviceRegisterRequest, current_session: SessionDep):
    if not current_session.is_admin:
        raise forbidden_error("Access denied")
    new_credentials = create_credentials(imei = register_data.device.imei)
    messages = [
        new_credentials
    ]
    await prepare_and_send_messages("telemetry-device-registration", messages) # or maybe directly to db?
    return success_response
