"""Transform tasks for data cleaning and enrichment."""

import pandas as pd
from typing import Any, TypedDict
from datetime import datetime

from prefect import task


@task(
    name="transform_posts",
    log_prints=True,
    tags=["transform"],
)
def transform_posts(
    raw_posts: list[dict[str, Any]], schema: type[TypedDict]
) -> pd.DataFrame:
    """
    Transform raw API data into a clean DataFrame.

    Args:
        raw_posts: List of post dictionaries from API

    Returns:
        Cleaned and enriched DataFrame
    """
    if not raw_posts:
        print("No data to transform")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(raw_posts)

    # Data cleaning
    df = _clean_dataframe(df, schema)

    # Data enrichment
    df = _enrich_dataframe(df)

    print(f"Transformed {len(df)} rows")
    print(f"Schema: {list(df.columns)}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")

    return df


def _clean_dataframe(df: pd.DataFrame, schema: type[TypedDict]) -> pd.DataFrame:
    """Clean dataframe according to TypedDict schema."""
    if schema is None:
        return df

    annotations = schema.__annotations__
    defaults = schema.defaults()

    for col, default_val in defaults.items():
        if col in df.columns:
            df[col] = df[col].fillna(default_val)

    for field, field_type in annotations.items():
        if "NotRequired" not in str(field_type):
            if field not in df.columns:
                raise ValueError(f"Missing required column: {field}")

    return df


def _enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich the dataframe with additional computed columns."""
    # Add processing timestamp
    df["processed_at"] = datetime.now()

    # Add text length metrics
    if "title" in df.columns:
        df["title_length"] = df["title"].str.len()

    if "body" in df.columns:
        df["body_length"] = df["body"].str.len()

    # Add word count metrics (optional)
    if "title" in df.columns:
        df["title_word_count"] = df["title"].str.split().str.len()

    if "body" in df.columns:
        df["body_word_count"] = df["body"].str.split().str.len()

    return df


@task(
    name="validate_transformed_data",
    log_prints=True,
    tags=["transform", "validation"],
)
def validate_transformed_data(
    df: pd.DataFrame, schema: list[str], critical_cols: list[str]
) -> bool:
    """
    Validate the transformed data before loading.

    Args:
        df: DataFrame to validate

    Returns:
        True if valid, raises exception otherwise
    """
    if df.empty:
        raise ValueError("DataFrame is empty - nothing to load")

    required_columns = schema if schema else ["userId", "title", "body"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Check for nulls in critical columns
    for col in critical_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            print(f"{null_count} null values found in column '{col}'")

    print("Data validation passed")
    return True
