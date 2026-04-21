"""Extract tasks for fetching data from APIs."""

from typing import Any

import httpx

from prefect import task
from prefect.tasks import task_input_hash

from settings import settings
from config.api import APIClient


@task(
    name="fetch_posts",
    retries=settings.API_MAX_RETRIES,
    retry_delay_seconds=settings.API_RETRY_DELAYS,
    log_prints=True,
    cache_key_fn=task_input_hash,
    cache_expiration=3600,
    tags=["extract", "api"],
)
def fetch_posts(api_url: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
    """
    Fetch posts from a REST API endpoint.

    Args:
        api_url: Optional custom API URL (defaults to settings.API_BASE_URL)
        limit: Maximum number of posts to fetch

    Returns:
        List of post dictionaries
    """
    base_url = api_url or settings.API_BASE_URL
    url = f"{base_url}/api/v1/articles"
    params = {"_limit": limit}

    print(f"Fetching data from {url}...")

    try:
        with httpx.Client(timeout=settings.API_TIMEOUT) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            posts = response.json()
    except httpx.HTTPStatusError as e:
        print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        raise
    except httpx.RequestError as e:
        print(f"Request error: {str(e)}")
        raise

    print(f"Successfully fetched {len(posts)} posts")
    return posts


@task(
    name="fetch_posts_with_client",
    retries=settings.API_MAX_RETRIES,
    retry_delay_seconds=settings.API_RETRY_DELAYS,
    log_prints=True,
    tags=["extract", "api"],
)
def fetch_posts_with_client(limit: int = 10) -> list[dict[str, Any]]:
    """
    Alternative extraction using the APIClient class.
    Better for complex API interactions.
    """
    with APIClient() as client:
        posts = client.get("/posts", params={"_limit": limit})

    print(f"Successfully fetched {len(posts)} posts")
    return posts
