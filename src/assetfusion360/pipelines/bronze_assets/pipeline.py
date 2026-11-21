"""
Kedro Bronze Assets pipeline registration.
"""

from kedro.pipeline import Pipeline

from assetfusion360.pipelines.bronze_assets_ingestion import (
    create_bronze_assets_pipeline,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Expose the existing Bronze Assets ingestion pipeline."""
    return create_bronze_assets_pipeline()
