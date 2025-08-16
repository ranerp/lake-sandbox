"""DLT pipeline stage for timeseries generation."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import typer

from lake_sandbox.timeseries_generator.generator import generate_timeseries
from lake_sandbox.utils.performance import monitor_performance


@dlt.resource(name="timeseries_generator")
@monitor_performance()
def generate_timeseries_resource(
    output_dir: str = "./output/timeseries-raw",
    total_parcels: int = 500_000,
    start_date: str = "2024-01-01",
    end_date: str = "2024-04-15",
    tiles: int = 2,
    batch_size: int = 50_000,
) -> Iterator[dict[str, Any]]:
    """DLT resource for generating timeseries data.

    Args:
        output_dir: Directory to save generated parquet files
        total_parcels: Total number of parcels to generate
        start_date: Start date for timeseries (YYYY-MM-DD)
        end_date: End date for timeseries (YYYY-MM-DD)
        tiles: Number of UTM tiles to generate
        batch_size: Batch size for processing

    Yields:
        Dict with generation status and statistics
    """

    typer.echo("=== DLT STAGE: TIMESERIES GENERATION ===")

    try:
        # Call the existing timeseries generator
        stats = generate_timeseries(
            output_dir=output_dir,
            total_parcels=total_parcels,
            start_date=start_date,
            end_date=end_date,
            tiles=tiles,
            batch_size=batch_size,
            dry_run=False,
            force=False
        )

        # Verify output exists
        output_path = Path(output_dir)
        if output_path.exists():
            parquet_files = list(output_path.rglob("*.parquet"))
            file_count = len(parquet_files)
        else:
            file_count = 0

        yield {
            "stage": "timeseries_generation",
            "status": "completed",
            "output_dir": output_dir,
            "stats": stats,
            "output_files": file_count,
            "total_parcels": total_parcels,
            "date_range": f"{start_date} to {end_date}",
            "tiles": tiles
        }

    except Exception as e:
        yield {
            "stage": "timeseries_generation",
            "status": "failed",
            "error": str(e),
            "output_dir": output_dir
        }


def create_timeseries_pipeline(
    pipeline_name: str = "timeseries_pipeline",
    destination: str = "duckdb"
) -> dlt.Pipeline:
    """Create a DLT pipeline for timeseries generation.

    Args:
        pipeline_name: Name of the pipeline
        destination: DLT destination (parquet, filesystem, etc.)

    Returns:
        Configured DLT pipeline
    """

    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name="lake_sandbox_timeseries"
    )
