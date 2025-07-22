import asyncio
import httpx
import structlog
import typer
import pandas as pd

from pathlib import Path
from pandera.errors import SchemaErrors

from core.config.config import settings
from core.services.sba_ingestion import SBAIngestionService
from core.schemas.sba_loans import LoanDataSmokeSchema, filter_schema_for_df

log = structlog.get_logger(__name__)
app = typer.Typer(
    help="SBA 7(a) Loan Data Ingestion CLI. Fetches all historical CSVs, "
    "combines them, validates the result, and saves a single file."
)


@app.command()
def fetch(
    skip_if_exists: bool = typer.Option(
        False, "--skip", help="Skip download if final file exists."
    ),
):
    final_output_path = settings.RAW_DATA_DIR / settings.FINAL_SBA_7A_FILENAME
    if final_output_path.exists() and skip_if_exists:
        log.info(
            "Final dataset already exists. Skipping process.",
            path=str(final_output_path),
        )
        raise typer.Exit()

    settings.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    asyncio.run(_orchestrate_ingestion(final_output_path))


async def _orchestrate_ingestion(output_path: Path):
    """Optimized ingestion: stream, validate, and append CSVs one by one."""
    async with httpx.AsyncClient(timeout=settings.HTTP_TIMEOUT) as client:
        service = SBAIngestionService(client, settings.SBA_API_BASE_URL)

        try:
            csv_urls = await service.get_csv_download_urls(settings.SBA_DATASET_ID)
            if not csv_urls:
                log.error("No CSV URLs found. Aborting.")
                raise typer.Exit(code=1)

            output_path.unlink(missing_ok=True)
            is_first = True

            semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_REQUESTS)

            for url in csv_urls:
                df = await _download_and_load_with_semaphore(service, url, semaphore)
                if df is None:
                    continue

                try:
                    filtered_schema = filter_schema_for_df(LoanDataSmokeSchema, df)
                    filtered_schema.validate(
                        df.sample(n=1000, random_state=42)
                    )  # smoke check
                except SchemaErrors as e:
                    log.warning(
                        "Validation failed for partial data", url=url, error=str(e)
                    )
                    continue

                log.info("Appending CSV to final output", rows=len(df), url=url)
                df.to_csv(output_path, mode="a", index=False, header=is_first)
                is_first = False

                del df
                import gc

                gc.collect()

            if not output_path.exists() or output_path.stat().st_size == 0:
                log.critical("No valid CSVs were written. Output is empty.")
                raise typer.Exit(code=1)

            log.info("All CSVs ingested and saved successfully.", path=str(output_path))

        except (IOError, SchemaErrors) as e:
            log.critical("Ingestion process failed.", error=str(e), exc_info=True)
            raise typer.Exit(code=1)


async def _download_and_load_with_semaphore(
    service: SBAIngestionService, url: str, semaphore: asyncio.Semaphore
) -> pd.DataFrame | None:
    async with semaphore:
        return await service.download_csv_to_dataframe(url)


if __name__ == "__main__":
    app()
