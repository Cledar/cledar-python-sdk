import hmac
from datetime import datetime, timedelta, timezone
import bcrypt
from jose import jwt
from ..common.utils import generate_uuid
from .schemas import SessionJWT
from ..common.schemas import Credential
from ..config import settings

# TODO(pkomor): Store hashed password in the db intead of plain ones
# In GO API no password is hashed, so leave it for now
def verify_password(provided_password: str, reference_password: str) -> bool:
    # if reference_password is hashed
    # return bcrypt.checkpw(provided_password.encode("utf-8"), reference_password.encode("utf-8"))
    salt = bcrypt.gensalt()
    hashed_provided_password = get_hashed_password(provided_password, salt)
    hashed_reference_password = get_hashed_password(reference_password, salt)
    return hmac.compare_digest(hashed_provided_password, hashed_reference_password)

def get_hashed_password(password: str, salt: bytes) -> bytes:
    return bcrypt.hashpw(password.encode("utf-8"), salt)

def get_user(db, username: str) -> Credential | None:
    user_dict = db.get(username)
    return Credential(**user_dict) if user_dict is not None else None

def authenticate_user(db, username: str, password: str) -> Credential | None:
    user = get_user(db, username)
    return user if user and verify_password(password, user.api_secret_key) else None

def create_session_jwt(user: Credential) -> SessionJWT:
    return SessionJWT(
            uuid=generate_uuid(),
            is_admin=user.api_is_admin,
            imei=user.api_device_imei
        )

def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta # TODO(pkomor): check timezones
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return encoded_jwt

#TODO(pkomor): Integrate with db
users_db = {
    "xxxx": {
        "api_application": "xxxx",
        "api_secret_key": "xxxx",
        "api_type": "device",
        "api_device_imei": "test",
        "api_active": True,
        "api_is_admin": True
    }
}
