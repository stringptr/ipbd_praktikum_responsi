"""API client configuration and management."""

from typing import Any
import httpx

from settings import settings


class APIClient:
    """HTTP client for API requests with retry logic."""

    def __init__(self, base_url: str | None = None, timeout: int | None = None):
        self.base_url = base_url or settings.API_BASE_URL
        self.timeout = timeout or settings.API_TIMEOUT
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Lazy-load HTTP client."""
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout)
        return self._client

    def get(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Perform GET request."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.client.get(url, params=params)
        _ = response.raise_for_status()
        return response.json()

    def close(self):
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def get_api_client() -> APIClient:
    """Factory function for API client."""
    return APIClient()
