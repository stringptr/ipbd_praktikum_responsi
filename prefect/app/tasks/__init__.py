"""Prefect tasks for ETL pipeline."""

from tasks.extract.articles import fetch_posts, fetch_posts_with_client
from tasks.transform import transform_posts, validate_transformed_data
from tasks.load import (
    insert_to_database,
    insert_to_database_batch,
    insert_to_database_hybrid,
)

__all__ = [
    "fetch_posts",
    "fetch_posts_with_client",
    "transform_posts",
    "validate_transformed_data",
    "insert_to_database",
    "insert_to_database_batch",
    "insert_to_database_hybrid",
]
