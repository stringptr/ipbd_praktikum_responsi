"""Main ETL flow for fetching API data and loading to database."""

import os

from prefect import flow

from settings import settings
from tasks.extract import fetch_posts
from tasks.transform import transform_posts, validate_transformed_data
from tasks.load import insert_to_database, insert_to_database_hybrid
from utils.helpers import log_metrics, save_checkpoint


@flow(
    name="API to Database ETL",
    description="Extract data from API, transform, and load to PostgreSQL",
    log_prints=True,
    retries=1,
    retry_delay_seconds=10,
)
def api_to_db_flow(
    api_url: str | None = None,
    limit: int | None = None,
    table_name: str | None = None,
    use_hybrid: bool = True,
    save_intermediate: bool = False,
) -> dict:
    """
    Main ETL flow for API to database pipeline.

    Args:
        api_url: Optional custom API URL
        limit: Number of records to fetch
        table_name: Target database table name
        use_hybrid: Use hybrid loading (block + .env fallback)
        save_intermediate: Save checkpoints for debugging

    Returns:
        Dictionary with flow metrics
    """
    limit = limit or settings.DEFAULT_LIMIT
    table_name = table_name or settings.DEFAULT_TABLE_NAME

    print("Starting ETL flow")
    print(f"Environment: {'Production' if os.getenv('PREFECT_API_URL') else 'Local'}")
    print(f" Configuration: limit={limit}, table={table_name}, hybrid={use_hybrid}")

    # ============================================================
    # Extract Phase
    # ============================================================

    print("\nPhase 1: Extraction")
    raw_data = fetch_posts(api_url, limit=limit)

    if save_intermediate:
        save_checkpoint(raw_data, "extracted_data")

    # ============================================================
    # Transform Phase
    # ============================================================

    print("\nPhase 2: Transformation")
    transformed_df = transform_posts(articles_list, WiredArticleSchema)

    validate_transformed_data(transformed_df)

    if save_intermediate:
        save_checkpoint(transformed_df.to_dict(), "transformed_data")

    # ============================================================
    # Load Phase
    # ============================================================

    print("\nPhase 3: Loading")

    if use_hybrid:
        rows_inserted = insert_to_database_hybrid(transformed_df, table_name)
    else:
        rows_inserted = insert_to_database(transformed_df, table_name)

    # ============================================================
    # Metrics and Cleanup
    # ============================================================
    metrics = {
        "records_fetched": len(raw_data),
        "records_transformed": len(transformed_df),
        "records_inserted": rows_inserted,
        "success": rows_inserted > 0,
    }

    log_metrics(metrics, prefix="api_to_db")

    print(f"\nETL flow complete!")
    print(
        f"Summary: Fetched={metrics['records_fetched']}, "
        f"Inserted={metrics['records_inserted']}"
    )

    return metrics


@flow(name="API to Database ETL - Simple", log_prints=True)
def api_to_db_flow_simple(
    limit: int = 10,
    table_name: str = "posts",
) -> int:
    """
    Simplified version of the ETL flow with minimal configuration.

    Args:
        limit: Number of records to fetch
        table_name: Target database table

    Returns:
        Number of rows inserted
    """
    raw_data = fetch_posts(limit=limit)

    transformed_df = transform_posts(raw_data)

    rows_inserted = insert_to_database(transformed_df, table_name)

    return rows_inserted


# ============================================================
# Script Entry Point
# ============================================================
if __name__ == "__main__":
    # Run locally with .env file
    result = api_to_db_flow(limit=10, save_intermediate=True)
    print(f"\nFlow result: {result}")
