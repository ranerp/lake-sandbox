"""DLT pipeline stage for data reorganization."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import typer

from lake_sandbox.reorg_pattern.reorganization import reorg
from lake_sandbox.utils.performance import monitor_performance


@dlt.resource(name="reorganization")
@monitor_performance()
def reorganization_resource(
    input_dir: str = "./output/timeseries-raw",
    output_dir: str = "./output/timeseries-organized",
    delta_dir: str = "./output/timeseries-delta",
    chunk_size: int = 10_000,
    phase: str = "all",
    force: bool = False
) -> Iterator[dict[str, Any]]:
    """DLT resource for reorganizing timeseries data.

    Args:
        input_dir: Input directory with raw parquet files
        output_dir: Output directory for reorganized parquet files
        delta_dir: Output directory for Delta Lake tables
        chunk_size: Number of parcels per chunk
        phase: Which phase to run ('reorg', 'delta', 'optimize', or 'all')
        force: Force reprocessing of existing files

    Yields:
        Dict with reorganization status and statistics
    """

    typer.echo("=== DLT STAGE: DATA REORGANIZATION ===")

    try:
        # Verify input directory exists
        input_path = Path(input_dir)
        if not input_path.exists():
            raise FileNotFoundError(f"Input directory {input_dir} does not exist")

        # Get input statistics
        parquet_files = list(input_path.rglob("*.parquet"))
        input_file_count = len(parquet_files)

        if input_file_count == 0:
            raise ValueError(f"No parquet files found in {input_dir}")

        # Call the existing reorganization function
        # Note: reorg() doesn't return stats directly, so we'll capture the process
        reorg(
            input_dir=input_dir,
            output_dir=output_dir,
            delta_dir=delta_dir,
            chunk_size=chunk_size,
            phase=phase,
            force=force,
            dry_run=False,
            status=False
        )

        # Verify outputs
        organized_path = Path(output_dir)
        delta_path = Path(delta_dir)

        organized_chunks = 0
        delta_tables = 0

        if organized_path.exists():
            organized_chunks = len([d for d in organized_path.iterdir()
                                    if
                                    d.is_dir() and d.name.startswith("parcel_chunk=")])

        if delta_path.exists():
            delta_tables = len([d for d in delta_path.iterdir()
                                if d.is_dir() and d.name.startswith("parcel_chunk=")])

        yield {
            "stage": "reorganization",
            "status": "completed",
            "input_dir": input_dir,
            "output_dir": output_dir,
            "delta_dir": delta_dir,
            "phase": phase,
            "chunk_size": chunk_size,
            "input_files": input_file_count,
            "organized_chunks": organized_chunks,
            "delta_tables": delta_tables,
            "force_used": force
        }

    except Exception as e:
        yield {
            "stage": "reorganization",
            "status": "failed",
            "error": str(e),
            "input_dir": input_dir,
            "output_dir": output_dir,
            "delta_dir": delta_dir,
            "phase": phase
        }


def create_reorganization_pipeline(
    pipeline_name: str = "reorganization_pipeline",
    destination: str = "duckdb"
) -> dlt.Pipeline:
    """Create a DLT pipeline for data reorganization.

    Args:
        pipeline_name: Name of the pipeline
        destination: DLT destination (parquet, filesystem, etc.)

    Returns:
        Configured DLT pipeline
    """

    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name="lake_sandbox_reorganization"
    )
