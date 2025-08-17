import typer

from lake_sandbox.reorg_pattern.reorganize.reorg import (
    get_reorganization_progress,
    reorganize_by_parcel_chunks,
)
from lake_sandbox.reorg_pattern.delta.delta import (
    convert_to_delta_lake,
    get_delta_conversion_progress,
    optimize_all_delta_tables,
)
from lake_sandbox.utils.performance import monitor_performance


@monitor_performance()
def reorg(
    input_dir: str = typer.Option(
        "./output/timeseries-raw",
        "--input-dir",
        "-i",
        help="Input directory with raw parquet files partitioned by utm_tile/year/date"
    ),
    output_dir: str = typer.Option(
        "./output/timeseries-organized",
        "--output-dir",
        "-o",
        help="Output directory for reorganized parquet files partitioned by parcel_chunk"
    ),
    delta_dir: str = typer.Option(
        "./output/timeseries-delta",
        "--delta-dir",
        "-d",
        help="Output directory for Delta Lake tables"
    ),
    chunk_size: int = typer.Option(
        10_000, "--chunk-size", help="Number of parcels per chunk"
    ),
    phase: str = typer.Option(
        "all", "--phase",
        help="Which phase to run: 'reorg', 'delta', 'optimize', or 'all'"
    ),
    force: bool = typer.Option(
        False, "--force", help="Force reprocessing of existing files"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be done without making changes"
    ),
    status: bool = typer.Option(
        False, "--status", help="Show current progress status and exit"
    ),
) -> None:
    """Reorganize raw timeseries data from date-partitioned to parcel-chunk-partitioned format."""

    if status:
        show_reorganization_status(input_dir, output_dir, delta_dir)
        return

    typer.echo("=== TIMESERIES DATA REORGANIZATION ===")
    typer.echo(f"Input directory: {input_dir}")
    typer.echo(f"Output directory: {output_dir}")
    typer.echo(f"Delta directory: {delta_dir}")
    typer.echo(f"Chunk size: {chunk_size:,} parcels per chunk")
    typer.echo(f"Phase: {phase}")

    if dry_run:
        typer.echo("DRY RUN MODE - No files will be created")
    if force:
        typer.echo("FORCE MODE - Existing files will be overwritten")

    # Phase 1: Reorganization
    if phase in ["reorg", "all"]:
        typer.echo("\n" + "=" * 60)
        phase1_stats = reorganize_by_parcel_chunks(
            input_dir=input_dir,
            output_dir=output_dir,
            chunk_size=chunk_size,
            dry_run=dry_run,
            force=force
        )

        if not dry_run and phase1_stats["failed"] > 0:
            typer.echo(
                f"⚠ Phase 1 had {phase1_stats['failed']} failures. Consider running with --force to retry.")

    # Phase 2: Delta Lake conversion
    if phase in ["delta", "all"]:
        typer.echo("\n" + "=" * 60)
        phase2_stats = convert_to_delta_lake(
            input_dir=output_dir,
            delta_dir=delta_dir,
            dry_run=dry_run,
            force=force
        )

        if not dry_run and phase2_stats["failed"] > 0:
            typer.echo(
                f"⚠ Phase 2 had {phase2_stats['failed']} failures. Consider running with --force to retry.")

    # Phase 3: Optimization (optional)
    if phase in ["optimize", "all"]:
        typer.echo("\n" + "=" * 60)
        optimize_stats = optimize_all_delta_tables(
            delta_dir=delta_dir,
            dry_run=dry_run
        )

        if not dry_run and optimize_stats["failed"] > 0:
            typer.echo(f"⚠ Optimization had {optimize_stats['failed']} failures.")

    if not dry_run:
        typer.echo("\n" + "=" * 60)
        typer.echo("FINAL STATUS:")
        show_reorganization_status(input_dir, output_dir, delta_dir)


def show_reorganization_status(input_dir: str, output_dir: str, delta_dir: str) -> None:
    """Show current status of reorganization process."""

    typer.echo("=== REORGANIZATION STATUS ===")

    # Check Phase 1 progress
    reorg_progress = get_reorganization_progress(output_dir)
    typer.echo(
        f"Phase 1 (Reorganization): {reorg_progress.existing_chunks} chunks completed")

    if reorg_progress.existing_chunks > 0:
        total_rows = sum(chunk.row_count for chunk in reorg_progress.chunk_files)
        typer.echo(f"  Total rows in organized chunks: {total_rows:,}")

    # Check Phase 2 progress
    delta_progress = get_delta_conversion_progress(output_dir, delta_dir)
    typer.echo(
        f"Phase 2 (Delta Conversion): {delta_progress.existing_delta_tables}/{delta_progress.total_chunks} Delta tables completed")

    if delta_progress.existing_delta_tables > 0:
        total_files = sum(
            table.file_count for table in delta_progress.delta_tables)
        typer.echo(f"  Total Delta files: {total_files}")

    # Recommendations
    if reorg_progress.existing_chunks == 0:
        typer.echo(
            "\nRun: lake-sandbox reorg --phase reorg  (to start reorganization)")
    elif delta_progress.existing_delta_tables < delta_progress.total_chunks:
        missing = delta_progress.total_chunks - delta_progress.existing_delta_tables
        typer.echo(
            f"\nRun: lake-sandbox reorg --phase delta  (to convert {missing} remaining chunks)")
    elif delta_progress.existing_delta_tables > 0:
        typer.echo(
            "\nRun: lake-sandbox reorg --phase optimize  (to optimize Delta tables)")
        typer.echo("✓ Reorganization appears complete!")

    typer.echo("\nData locations:")
    typer.echo(f"  Raw data: {input_dir}")
    typer.echo(f"  Organized: {output_dir}")
    typer.echo(f"  Delta Lake: {delta_dir}")
