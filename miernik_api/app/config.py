from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:19092" # or list?

    model_config = SettingsConfigDict(env_file=".env")

@lru_cache() # no need to recreate Settings object
def get_settings():
    return Settings()

settings = get_settings()