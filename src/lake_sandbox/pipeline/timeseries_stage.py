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
    utm_tiles: str = "32TNR,32TPR",
    start_date: str = "2024-01-01",
    end_date: str = "2024-04-15",
    num_parcels: int = 500_000,
) -> Iterator[dict[str, Any]]:
    """DLT resource for generating timeseries data.

    Args:
        output_dir: Directory to save generated parquet files
        utm_tiles: Comma-separated UTM tile identifiers
        start_date: Start date for timeseries (YYYY-MM-DD)
        end_date: End date for timeseries (YYYY-MM-DD)
        num_parcels: Number of parcels to generate per tile

    Yields:
        Dict with generation status and statistics
    """

    typer.echo("=== DLT STAGE: TIMESERIES GENERATION ===")

    try:
        # Call the existing timeseries generator
        generate_timeseries(
            output_dir=output_dir,
            utm_tiles=utm_tiles,
            start_date=start_date,
            end_date=end_date,
            num_parcels=num_parcels
        )

        # Verify output exists
        output_path = Path(output_dir)
        if output_path.exists():
            parquet_files = list(output_path.rglob("*.parquet"))
            file_count = len(parquet_files)
        else:
            file_count = 0

        # Count tiles from comma-separated string
        tiles_count = len([tile.strip() for tile in utm_tiles.split(",") if tile.strip()])

        yield {
            "stage": "timeseries_generation",
            "status": "completed",
            "output_dir": output_dir,
            "output_files": file_count,
            "num_parcels": num_parcels,
            "date_range": f"{start_date} to {end_date}",
            "utm_tiles": utm_tiles,
            "tiles_count": tiles_count
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
