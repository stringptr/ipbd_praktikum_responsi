"""Transform tasks for data cleaning and enrichment."""

import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

from prefect import task


@task(
    name="transform_posts",
    log_prints=True,
    tags=["transform"],
)
def transform_posts(raw_posts: List[Dict[str, Any]]) -> pd.DataFrame:
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
    df = _clean_dataframe(df)

    # Data enrichment
    df = _enrich_dataframe(df)

    print(f"Transformed {len(df)} rows")
    print(f"Schema: {list(df.columns)}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")

    return df


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the dataframe by handling missing values and type conversions."""
    # Fill missing values
    df = df.fillna(
        {
            "userId": 0,
            "title": "",
            "body": "",
        }
    )

    # Ensure correct data types
    if "userId" in df.columns:
        df["userId"] = df["userId"].astype(int)

    if "id" in df.columns:
        df["id"] = df["id"].astype(int)

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
def validate_transformed_data(df: pd.DataFrame) -> bool:
    """
    Validate the transformed data before loading.

    Args:
        df: DataFrame to validate

    Returns:
        True if valid, raises exception otherwise
    """
    if df.empty:
        raise ValueError("DataFrame is empty - nothing to load")

    required_columns = ["userId", "title", "body"]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Check for nulls in critical columns
    critical_cols = ["userId"]
    for col in critical_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            print(f"{null_count} null values found in column '{col}'")

    print("Data validation passed")
    return True
