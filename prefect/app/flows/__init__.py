"""Prefect flow definitions."""

from flows.article_api_to_warehouse import api_to_db_flow, api_to_db_flow_simple

__all__ = [
    "api_to_db_flow",
    "api_to_db_flow_simple",
]
