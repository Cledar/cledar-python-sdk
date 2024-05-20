from datetime import timedelta
from fastapi import APIRouter, status
from .schemas import (
    LoginRequest,
    Token
)
from .utils import (
    create_session_jwt,
    authenticate_user,
    create_access_token,
    users_db
)
from ..exceptions import (
    unauthorized_error,
    bad_request_error
)
from ..dependencies import (
    SettingsDep
)

router = APIRouter(
    prefix="/auth",
    tags=["auth"]
)

@router.post("/login", status_code=status.HTTP_200_OK, response_model=Token)
async def login_for_access_token(
    login_data: LoginRequest, settings: SettingsDep
) -> Token:
    user = authenticate_user(users_db, login_data.app_id, login_data.app_secret)
    if not user:
        raise unauthorized_error("Incorrect username or password")
    if not user.api_active:
        raise bad_request_error("Inactive user")
    access_token_expires = timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    session = create_session_jwt(user)
    access_token = create_access_token(
        data=session.dict(), expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")