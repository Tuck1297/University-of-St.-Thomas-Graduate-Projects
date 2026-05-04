from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):
    # Database Configuration
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "password"
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "gis_migration"

    # API Configuration
    API_TITLE: str = "GIS Data Migration API"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    
    # Database View/Table Names
    SPATIAL_VIEW: str = "v_spatial_locations"
    GEOM_COL: str = "geometry"

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

@lru_cache
def get_settings():
    return Settings()
