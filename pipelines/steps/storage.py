# pipelines/steps/storage.py
import pandas as pd
import sqlalchemy as sa
import structlog
from zenml import step

from core.config.config import Settings, settings

log = structlog.get_logger(__name__)


@step
def write_to_postgres(df: pd.DataFrame, cfg: Settings = settings) -> None:
    """
    ZenML step to write the final DataFrame to a PostgreSQL table.
    """
    log.info(f"Writing {len(df)} rows to PostgreSQL table 'sba_7a_raw'.")

    try:
        # NOTE: The hostname 'db' matches the service name in docker-compose.yml
        # For local runs outside compose, this might be 'localhost'.
        # We will manage this with environment variables.
        password = cfg.POSTGRES_PASSWORD.get_secret_value()
        connection_url = (
            f"postgresql+psycopg://{cfg.POSTGRES_USER}:{password}"
            f"@{cfg.POSTGRES_HOST}:5432/{cfg.POSTGRES_DB}"
        )
        engine = sa.create_engine(connection_url)

        df.to_sql(
            "sba_7a_raw",
            engine,
            if_exists="replace",
            index=False,
            chunksize=10_000,
            method="multi",  # efficient bulk inserts
        )
        log.info("Successfully wrote data to PostgreSQL.")
    except Exception as e:
        log.error("Failed to write data to PostgreSQL.", exc_info=True)
        raise e
