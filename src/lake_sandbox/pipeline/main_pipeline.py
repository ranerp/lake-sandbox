"""Main DLT pipeline orchestrating timeseries generation, reorganization, and validation."""

from pathlib import Path
from typing import Any

import dlt
import typer

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
    destination: str = "duckdb",
    skip_existing: bool = True,
    validate_only: bool = False
) -> dict[str, Any]:
    """Run the complete lake-sandbox pipeline: generate -> reorganize -> validate.

    Args:
        # Timeseries generation
        output_dir: Directory to save raw timeseries data
        total_parcels: Total number of parcels to generate
        start_date: Start date for timeseries (YYYY-MM-DD)
        end_date: End date for timeseries (YYYY-MM-DD)
        tiles: Number of UTM tiles to generate
        batch_size: Batch size for processing

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
        skip_existing: Skip stages if output already exists
        validate_only: Only run validation (skip generation and reorganization)

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
        if not validate_only:
            raw_path = Path(output_dir)
            should_generate = not skip_existing or not raw_path.exists() or len(
                list(raw_path.rglob("*.parquet"))) == 0

            if should_generate:
                typer.echo("\nSTAGE 1: Timeseries Generation")

                # Execute timeseries generation directly (not as DLT resource)
                from lake_sandbox.timeseries_generator.generator import (
                    generate_timeseries,
                )

                generate_timeseries(
                    output_dir=output_dir,
                    utm_tiles=tiles,
                    start_date=start_date,
                    end_date=end_date,
                    num_parcels=total_parcels
                )

                typer.echo("Timeseries generation completed")
                pipeline_results["stages_completed"].append("timeseries_generation")
            else:
                typer.echo("\nSTAGE 1: Skipping timeseries generation (data exists)")
                pipeline_results["stages_completed"].append(
                    "timeseries_generation_skipped")

        # Stage 2: Reorganization
        if not validate_only:
            organized_path = Path(organized_dir)
            should_reorganize = not skip_existing or not organized_path.exists() or len(
                list(organized_path.glob("parcel_chunk=*"))) == 0

            if should_reorganize:
                typer.echo("\n🔄 STAGE 2: Data Reorganization")

                # Execute reorganization directly (not as DLT resource)
                from lake_sandbox.reorg_pattern.reorganization import reorg

                reorg(
                    input_dir=output_dir,
                    output_dir=organized_dir,
                    delta_dir=delta_dir,
                    chunk_size=chunk_size,
                    phase=reorg_phase,
                    force=force,
                    dry_run=False,
                    status=False
                )

                typer.echo("Reorganization completed")
                pipeline_results["stages_completed"].append("reorganization")
            else:
                typer.echo("\nSTAGE 2: Skipping reorganization (data exists)")
                pipeline_results["stages_completed"].append("reorganization_skipped")

        # Stage 3: Validation (always run)
        typer.echo("\nSTAGE 3: Data Validation")

        # Execute validation directly and log results to DLT
        import sys
        from io import StringIO

        from lake_sandbox.validator.validation import validate

        # Capture validation output
        old_stdout = sys.stdout
        sys.stdout = validation_output = StringIO()

        try:
            validate(
                target=validation_target,
                organized_dir=organized_dir,
                delta_dir=delta_dir,
                raw_dir=output_dir,
                expected_total_parcels=total_parcels,
                expected_chunk_size=chunk_size,
                expected_tiles=tiles_count,
                expected_dates=expected_dates,
                verbose=False
            )
        finally:
            sys.stdout = old_stdout

        validation_output_str = validation_output.getvalue()
        typer.echo(validation_output_str)

        # Log validation results to DLT for tracking
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
        typer.echo(f"Validation completed and logged: {load_info}")
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
    skip_existing: bool = typer.Option(
        True, "--skip-existing/--no-skip-existing", help="Skip stages if output exists"
    ),
    validate_only: bool = typer.Option(
        False, "--validate-only",
        help="Only run validation (skip generation and reorganization)"
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
        destination=destination,
        skip_existing=skip_existing,
        validate_only=validate_only
    )
