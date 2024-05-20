from datetime import datetime
from .schemas import BaseFields

def generate_base_fields(id_counter: int, time: datetime) -> BaseFields:
    # TODO(pkomor): id is not unique across endpoints execution
    return BaseFields(
        id=id_counter,
        created_at=time,
        updated_at=time,
        deleted_at=None
    )

success_response = {"status": "OK"}