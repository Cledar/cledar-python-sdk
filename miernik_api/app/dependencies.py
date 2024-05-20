from typing import Annotated
from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import ValidationError
from jose import JWTError, jwt
from .auth.schemas import SessionJWT
from .exceptions import unauthorized_error
from .config import Settings, get_settings

SettingsDep = Annotated[Settings, Depends(get_settings)]

# for extracting token from the Authorization header
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login") # tokenUrl is for automatic docs, can be used later

async def get_current_session(
    token: Annotated[str, Depends(oauth2_scheme)], settings: SettingsDep
) -> SessionJWT:
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    except JWTError as error:
        raise unauthorized_error("Could not validate credentials") from error
    try:
        token_data = SessionJWT(**payload)
    except ValidationError as error:
        raise unauthorized_error("Invalid token payload") from error
    return token_data

SessionDep = Annotated[SessionJWT, Depends(get_current_session)]
