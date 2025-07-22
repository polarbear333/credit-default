from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Centralized application configuration.
    Values are loaded from a .env file and environment variables.
    """

    # package_show endpoint
    SBA_API_BASE_URL: str = "https://data.sba.gov/api/3/action"
    SBA_DATASET_ID: str = "7-a-504-foia"

    RAW_DATA_DIR: Path = Path("data/raw")
    FINAL_SBA_7A_FILENAME: str = "sba_7a_loans_all_years.csv"

    MAX_CONCURRENT_REQUESTS: int = 5
    HTTP_TIMEOUT: int = 300

    # loads from .env file
    POSTGRES_USER: str = Field(..., description="PostgreSQL username")
    POSTGRES_PASSWORD: str = Field(..., description="PostgreSQL password")
    POSTGRES_DB: str = Field(..., description="PostgreSQL database name")

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False
    )


settings = Settings()
