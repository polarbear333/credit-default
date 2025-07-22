import io
import httpx
import pandas as pd
import structlog

log = structlog.get_logger(__name__)


class SBAIngestionService:
    """A service dedicated to fetching data from the SBA API."""

    def __init__(self, http_client: httpx.AsyncClient, base_url: str):
        self.client = http_client
        self.base_url = base_url

    async def get_csv_download_urls(self, dataset_id: str) -> list[str]:
        package_show_url = f"{self.base_url}/package_show"
        params = {"id": dataset_id}
        log.info("Fetching dataset metadata", url=package_show_url, dataset=dataset_id)

        try:
            response = await self.client.get(package_show_url, params=params)
            response.raise_for_status()

            result = response.json()
            if not result.get("success"):
                raise IOError(
                    "API returned a failure response when fetching dataset details."
                )

            resources = result["result"]["resources"]
            csv_urls = [
                r["url"] for r in resources if r.get("format", "").lower() == "csv"
            ]

            if not csv_urls:
                log.warning("No CSV resources found in dataset.", dataset=dataset_id)

            log.info("Successfully retrieved CSV URLs.", count=len(csv_urls))
            return csv_urls

        except httpx.HTTPStatusError as e:
            log.error(
                "HTTP error while fetching dataset URLs.",
                status=e.response.status_code,
                exc_info=True,
            )
            raise IOError(f"HTTP error fetching dataset URLs: {e}") from e
        except Exception as e:
            log.error("Failed to retrieve CSV download URLs.", exc_info=True)
            raise IOError(f"An unexpected error occurred: {e}") from e

    async def download_csv_to_dataframe(self, url: str) -> pd.DataFrame | None:
        log.info("Downloading CSV", url=url)
        try:
            async with self.client.stream("GET", url) as response:
                response.raise_for_status()
                content = io.BytesIO(await response.aread())

            df = pd.read_csv(content, low_memory=False)
            log.info("Successfully downloaded and parsed CSV.", url=url, rows=len(df))
            return df

        except httpx.HTTPStatusError as e:
            log.error(
                "HTTP error downloading file.", url=url, status=e.response.status_code
            )
            return None
        except Exception:
            log.error("Failed to process CSV from URL.", url=url, exc_info=True)
            return None
