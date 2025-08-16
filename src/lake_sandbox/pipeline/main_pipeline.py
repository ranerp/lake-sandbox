"""Main DLT pipeline orchestrating timeseries generation, reorganization, and validation."""

from typing import Any

import dlt
import typer

from lake_sandbox.pipeline.reorganization_stage import reorganization_resource
from lake_sandbox.pipeline.timeseries_stage import generate_timeseries_resource
from lake_sandbox.pipeline.validation_stage import validation_resource
from lake_sandbox.utils.performance import monitor_performance


@monitor_performance()
def run_full_pipeline(
    # Timeseries generation parameters
    output_dir: str = "./output/timeseries-raw",
    total_parcels: int = 500_000,
    start_date: str = "2024-01-01",
    end_date: str = "2024-04-15",
    tiles: str = "32TNR,32TPR",

    # Reorganization parameters
    organized_dir: str = "./output/timeseries-organized",
    delta_dir: str = "./output/timeseries-delta",
    chunk_size: int = 10_000,
    reorg_phase: str = "all",
    force: bool = False,

    # Validation parameters
    validation_target: str = "both",
    expected_dates: int | None = None,

    # Pipeline parameters
    pipeline_name: str = "lake_sandbox_full_pipeline",
    destination: str = "duckdb"
) -> dict[str, Any]:
    """Run the complete lake-sandbox pipeline: generate -> reorganize -> validate.

    Args:
        # Timeseries generation
        output_dir: Directory to save raw timeseries data
        total_parcels: Total number of parcels to generate
        start_date: Start date for timeseries (YYYY-MM-DD)
        end_date: End date for timeseries (YYYY-MM-DD)
        tiles: Number of UTM tiles to generate

        # Reorganization
        organized_dir: Directory for reorganized parquet chunks
        delta_dir: Directory for Delta Lake tables
        chunk_size: Number of parcels per chunk
        reorg_phase: Which phase to run ('reorg', 'delta', 'optimize', or 'all')
        force: Force reprocessing of existing files

        # Validation
        validation_target: What to validate ('raw', 'organized', 'delta', or 'both')
        expected_dates: Expected number of dates per parcel

        # Pipeline
        pipeline_name: Name of the DLT pipeline
        destination: DLT destination (filesystem, parquet, etc.)

    Returns:
        Dict with pipeline execution results
    """

    # Count tiles from comma-separated string
    tiles_count = len([tile.strip() for tile in tiles.split(",") if tile.strip()])

    typer.echo("=== LAKE SANDBOX FULL PIPELINE ===")
    typer.echo(f"Pipeline: {pipeline_name}")
    typer.echo(f"Destination: {destination}")
    typer.echo(f"Tiles: {tiles} (count: {tiles_count})")
    typer.echo(f"Raw data: {output_dir}")
    typer.echo(f"Organized: {organized_dir}")
    typer.echo(f"Delta: {delta_dir}")

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name="lake_sandbox"
    )

    pipeline_results: dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "stages_completed": [],
        "stages_failed": [],
        "overall_status": "running"
    }

    try:
        # Stage 1: Timeseries Generation
        typer.echo("\nSTAGE 1: Timeseries Generation")
        timeseries_data = generate_timeseries_resource(
            output_dir=output_dir,
            utm_tiles=tiles,
            start_date=start_date,
            end_date=end_date,
            num_parcels=total_parcels
        )
        load_info = pipeline.run(timeseries_data)
        typer.echo(f"Timeseries generation completed: {load_info}")
        pipeline_results["stages_completed"].append("timeseries_generation")

        # Stage 2: Reorganization
        typer.echo("\nSTAGE 2: Data Reorganization")
        reorganization_data = reorganization_resource(
            input_dir=output_dir,
            output_dir=organized_dir,
            delta_dir=delta_dir,
            chunk_size=chunk_size,
            phase=reorg_phase,
            force=force
        )
        load_info = pipeline.run(reorganization_data)
        typer.echo(f"Reorganization completed: {load_info}")
        pipeline_results["stages_completed"].append("reorganization")

        # Stage 3: Validation
        typer.echo("\nSTAGE 3: Data Validation")
        validation_data = validation_resource(
            target=validation_target,
            organized_dir=organized_dir,
            delta_dir=delta_dir,
            raw_dir=output_dir,
            expected_total_parcels=total_parcels,
            expected_chunk_size=chunk_size,
            expected_tiles=tiles_count,
            expected_dates=expected_dates
        )
        load_info = pipeline.run(validation_data)
        typer.echo(f"Validation completed: {load_info}")
        pipeline_results["stages_completed"].append("validation")

        # Pipeline completed successfully
        pipeline_results["overall_status"] = "completed"
        typer.echo("\nPIPELINE COMPLETED SUCCESSFULLY!")
        typer.echo(
            f"Stages completed: {', '.join(pipeline_results['stages_completed'])}")

        return pipeline_results

    except Exception as e:
        pipeline_results["overall_status"] = "failed"
        pipeline_results["error"] = str(e)
        typer.echo(f"\nPIPELINE FAILED: {e}")
        raise


@monitor_performance()
def run_pipeline_cli(
    # Data generation options
    total_parcels: int = typer.Option(
        500_000, "--num-parcels", help="Number of parcels to generate per tile"
    ),
    start_date: str = typer.Option(
        "2024-01-01", "--start-date", help="Start date (YYYY-MM-DD)"
    ),
    end_date: str = typer.Option(
        "2024-04-15", "--end-date", help="End date (YYYY-MM-DD)"
    ),
    tiles: str = typer.Option(
        "32TNR,32TPR", "--utm-tiles", help="Comma-separated UTM tile identifiers"
    ),

    # Directory options
    output_dir: str = typer.Option(
        "./output/timeseries-raw", "--output-dir", help="Raw data output directory"
    ),
    organized_dir: str = typer.Option(
        "./output/timeseries-organized", "--organized-dir",
        help="Organized data directory"
    ),
    delta_dir: str = typer.Option(
        "./output/timeseries-delta", "--delta-dir", help="Delta Lake directory"
    ),

    # Processing options
    chunk_size: int = typer.Option(
        10_000, "--chunk-size", help="Number of parcels per chunk"
    ),
    reorg_phase: str = typer.Option(
        "all", "--reorg-phase",
        help="Reorganization phase ('reorg', 'delta', 'optimize', 'all')"
    ),
    validation_target: str = typer.Option(
        "both", "--validation-target",
        help="Validation target ('raw', 'organized', 'delta', 'both')"
    ),
    expected_dates: int | None = typer.Option(
        None, "--expected-dates", help="Expected number of dates per parcel"
    ),

    # Pipeline control
    force: bool = typer.Option(
        False, "--force", help="Force reprocessing of existing files"
    ),
    pipeline_name: str = typer.Option(
        "lake_sandbox_pipeline", "--pipeline-name", help="DLT pipeline name"
    ),
    destination: str = typer.Option(
        "duckdb", "--destination", help="DLT destination"
    )
) -> None:
    """Run the complete lake-sandbox data pipeline using DLT."""

    run_full_pipeline(
        # Timeseries generation
        output_dir=output_dir,
        total_parcels=total_parcels,
        start_date=start_date,
        end_date=end_date,
        tiles=tiles,

        # Reorganization
        organized_dir=organized_dir,
        delta_dir=delta_dir,
        chunk_size=chunk_size,
        reorg_phase=reorg_phase,
        force=force,

        # Validation
        validation_target=validation_target,
        expected_dates=expected_dates,

        # Pipeline
        pipeline_name=pipeline_name,
        destination=destination
    )
