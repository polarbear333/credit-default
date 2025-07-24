import asyncio
import httpx
import pandas as pd
import structlog
from zenml import step

from core.config.config import settings, Settings
from core.services.sba_ingestion import SBAIngestionService
from core.schemas.sba_loans import LoanDataSmokeSchema

log = structlog.get_logger(__name__)


@step(enable_cache=True)
def fetch_and_combine_sba_data(cfg: Settings = settings) -> pd.DataFrame:
    """
    ZenML step to fetch all SBA 7(a) CSVs, combine them into a single
    DataFrame, and perform initial validation.
    """
    log.info("Starting SBA data ingestion step")

    async def _run_ingestion():
        async with httpx.AsyncClient(timeout=cfg.HTTP_TIMEOUT) as client:
            service = SBAIngestionService(client, cfg.SBA_API_BASE_URL)
            urls = await service.get_csv_download_urls(cfg.SBA_DATASET_ID)
            if not urls:
                raise RuntimeError("No CSV urls found, aborting pipeline")

            # further optimized with asyncio.gather if needed
            all_dfs = [await service.download_csv_to_dataframe(url) for url in urls]

            successful_dfs = [df for df in all_dfs if df is not None]
            if not successful_dfs:
                raise RuntimeError("All CSV downloads failed, aborting pipeline")

            combined_df = pd.concat(successful_dfs, ignore_index=True)
            log.info("Successfully combined all datasets.", total_rows=len(combined_df))

            # run Pandera validation (data contract enforcement)
            LoanDataSmokeSchema.validate(combined_df)
            log.info("Data smoke tests passed successfully.")

            return combined_df

    return asyncio.run(_run_ingestion())
