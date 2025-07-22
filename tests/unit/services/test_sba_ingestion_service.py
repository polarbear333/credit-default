# tests/unit/services/test_sba_ingestion_service.py
import pytest
import httpx
import pandas as pd
from unittest.mock import AsyncMock, MagicMock

from core.services.sba_ingestion import SBAIngestionService

pytestmark = pytest.mark.asyncio


@pytest.fixture
def mock_http_client() -> MagicMock:
    """Fixture to create a mock for httpx.AsyncClient."""
    return MagicMock(spec=httpx.AsyncClient)


@pytest.fixture
def mock_sba_service(mock_http_client: MagicMock) -> SBAIngestionService:
    """Fixture to create an instance of the service with a mock client."""
    return SBAIngestionService(
        http_client=mock_http_client, base_url="https://fake-api.gov"
    )


class TestGetCsvDownloadUrls:
    async def test_success(
        self, mock_sba_service: SBAIngestionService, mock_http_client: MagicMock
    ):
        mock_api_response = {
            "success": True,
            "result": {
                "resources": [
                    {"format": "CSV", "url": "https://example.com/file1.csv"},
                    {"format": "PDF", "url": "https://example.com/file2.pdf"},
                    {
                        "format": "csv",
                        "url": "https://example.com/file3.csv",
                    },  # Test case insensitivity
                ]
            },
        }
        mock_http_client.get.return_value = MagicMock(
            spec=httpx.Response, json=lambda: mock_api_response
        )
        urls = await mock_sba_service.get_csv_download_urls("any-dataset")

        assert len(urls) == 2
        assert "https://example.com/file1.csv" in urls
        assert "https://example.com/file3.csv" in urls
        mock_http_client.get.assert_called_once()

    async def test_api_failure_raises_ioerror(
        self, mock_sba_service: SBAIngestionService, mock_http_client: MagicMock
    ):
        mock_http_client.get.return_value = MagicMock(
            spec=httpx.Response, json=lambda: {"success": False}
        )

        with pytest.raises(IOError, match="API returned a failure response"):
            await mock_sba_service.get_csv_download_urls("any-dataset")


class TestDownloadCsvToDataframe:
    async def test_success(
        self, mock_sba_service: SBAIngestionService, mock_http_client: MagicMock
    ):
        csv_content = b"header1,header2\nvalue1,100\nvalue2,200"
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.aread = AsyncMock(return_value=csv_content)
        mock_http_client.stream.return_value.__aenter__.return_value = mock_response

        df = await mock_sba_service.download_csv_to_dataframe(
            "https://example.com/file.csv"
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df["header1"].tolist() == ["value1", "value2"]

    async def test_http_error_returns_none(
        self, mock_sba_service: SBAIngestionService, mock_http_client: MagicMock
    ):
        mock_http_client.stream.side_effect = httpx.HTTPStatusError(
            "Not Found", request=MagicMock(), response=MagicMock(status_code=404)
        )

        result = await mock_sba_service.download_csv_to_dataframe(
            "https://example.com/not-found.csv"
        )
        assert result is None
