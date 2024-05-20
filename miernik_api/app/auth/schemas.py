from pydantic import BaseModel, Field

class SessionJWT(BaseModel):
    '''
    JWT token payload.
    '''
    uuid: str = Field(
        description="Unique identifier of the session."
    )
    is_admin: bool = Field(
        description="Is user admin."
    )
    imei: str = Field(
        description="IMEI of the Miernik. According to the documention hashed with MD5."
    )

class LoginRequest(BaseModel):
    app_id: str = Field(
        description="Miernik identifier. Has to be the same as in the database to authenticate."
    )
    app_secret: str = Field(
        description="Miernik password. Has to be the same as in the database to authenticate."
    )

class Token(BaseModel):
    access_token: str = Field(
        description="JWT token."
    )
    token_type: str = Field(
        description="Token type. Usually 'bearer'."
    )