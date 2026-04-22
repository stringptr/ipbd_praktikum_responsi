"""Load tasks for inserting data into databases."""

import pandas as pd

from prefect import task
from sqlalchemy import text

from settings import settings
from config.database import db_manager


@task(
    name="insert_to_database",
    log_prints=True,
    tags=["load", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def insert_to_database(
    df: pd.DataFrame,
    schema_name: str | None = None,
    table_name: str | None = None,
    if_exists: str = "append",
) -> int:
    """
    Insert DataFrame into PostgreSQL database using environment variables.

    Args:
        df: DataFrame to insert
        table_name: Target table name (defaults to settings.DEFAULT_TABLE_NAME)
        if_exists: What to do if table exists ('append', 'replace', 'fail')

    Returns:
        Number of rows inserted
    """
    table_name = table_name or settings.DEFAULT_TABLE_NAME

    if not settings.validate_db_settings():
        raise ValueError("Missing database environment variables")

    with db_manager.get_connection() as connection:
        _ = df.to_sql(
            name=table_name,
            schema=schema_name,
            con=connection,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000,
        )

        total_count = db_manager.get_table_count(schema_name, table_name)

        print(f"Inserted {len(df)} rows into '{table_name}'")
        print(f"Total rows in table: {total_count}")

    return len(df)


@task(
    name="insert_to_database_batch",
    log_prints=True,
    tags=["load", "database", "batch"],
)
def insert_to_database_batch(
    df: pd.DataFrame,
    schema_name: str | None = None,
    table_name: str | None = None,
    batch_size: int = 500,
) -> int:
    """
    Insert DataFrame in batches for better memory management.

    Args:
        df: DataFrame to insert
        table_name: Target table name
        batch_size: Number of rows per batch

    Returns:
        Total rows inserted
    """
    table_name = table_name or settings.DEFAULT_TABLE_NAME
    total_inserted = 0

    with db_manager.get_connection() as connection:
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]
            batch.to_sql(
                name=table_name,
                schema=schema_name,
                con=connection,
                if_exists="append" if i > 0 else "append",
                index=False,
                method="multi",
            )
            total_inserted += len(batch)
            print(f"Inserted batch {i // batch_size + 1}: {len(batch)} rows")

    print(f"Total inserted: {total_inserted} rows into '{table_name}'")
    return total_inserted


@task(
    name="insert_to_database_hybrid",
    log_prints=True,
    tags=["load", "database", "hybrid"],
)
async def insert_to_database_hybrid(
    df: pd.DataFrame, table_name: str | None = None, schema_name: str | None = None
) -> int:
    """
    Hybrid approach: Try Prefect Block first, fallback to .env.
    """
    table_name = table_name or settings.DEFAULT_TABLE_NAME

    try:
        from prefect_sqlalchemy import SqlAlchemyConnector

        database_block = SqlAlchemyConnector.load(settings.PREFECT_BLOCK_NAME)

        with database_block.get_connection(begin=True) as connection:
            _ = df.to_sql(
                name=table_name,
                schema=schema_name,
                con=connection.engine,
                if_exists="append",
                index=False,
            )
        print("Used Prefect block (production mode)")

    except (ImportError, ValueError, KeyError) as e:
        print(f"Prefect block not found ({str(e)}), falling back to .env")

        if not settings.validate_db_settings():
            raise ValueError("Neither Prefect block nor .env configuration found")

        with db_manager.get_connection() as connection:
            _ = df.to_sql(
                name=table_name,
                con=connection,
                if_exists="append",
                index=False,
            )
        print("Used .env configuration (local mode)")

    total_count = db_manager.get_table_count(schema_name, table_name)
    print(f"Total rows in table: {total_count}")

    return len(df)
