import typer

from lake_sandbox.validator.delta import validate_delta_tables
from lake_sandbox.validator.organized import validate_organized_chunks
from lake_sandbox.validator.raw import validate_raw_timeseries


def validate(
    target: str = typer.Option(
        "both",
        "--target",
        "-t",
        help="What to validate: 'raw', 'organized', 'delta', or 'both' (organized+delta)"
    ),
    organized_dir: str = typer.Option(
        "./output/timeseries-organized",
        "--organized-dir",
        help="Directory with organized parquet chunks"
    ),
    delta_dir: str = typer.Option(
        "./output/timeseries-delta",
        "--delta-dir",
        help="Directory with Delta Lake tables"
    ),
    raw_dir: str = typer.Option(
        "./output/timeseries-raw",
        "--raw-dir",
        help="Directory with raw timeseries data partitioned by date"
    ),
    expected_total_parcels: int = typer.Option(
        500_000,
        "--total-parcels",
        help="Expected total unique parcels across all dates"
    ),
    expected_chunk_size: int = typer.Option(
        10_000,
        "--chunk-size",
        help="Expected number of parcels per chunk"
    ),
    expected_tiles: int = typer.Option(
        2,
        "--tiles",
        help="Expected number of UTM tiles (affects duplicate detection)"
    ),
    expected_dates: int | None = typer.Option(
        None,
        "--expected-dates",
        help="Expected number of dates per parcel (None = auto-detect from data)"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed validation information"
    ),
) -> None:
    """Validate organized chunks and/or Delta tables for data integrity."""

    typer.echo("=== DATA VALIDATION ===")
    typer.echo(f"Target: {target}")

    results = {}

    if target == "raw":
        results["raw"] = validate_raw_timeseries(
            raw_dir=raw_dir,
            expected_total_parcels=expected_total_parcels,
            verbose=verbose
        )
    elif target in ["organized", "both"]:
        results["organized"] = validate_organized_chunks(
            organized_dir=organized_dir,
            expected_chunk_size=expected_chunk_size,
            expected_tiles=expected_tiles,
            expected_dates=expected_dates,
            raw_dir=raw_dir,
            verbose=verbose
        )

    if target in ["delta", "both"]:
        results["delta"] = validate_delta_tables(
            delta_dir=delta_dir,
            verbose=verbose,
            organized_dir=organized_dir
        )

    # Overall summary
    if target == "both":
        typer.echo("\n=== OVERALL SUMMARY ===")
        organized_result = results.get("organized")
        delta_result = results.get("delta")
        organized_valid = organized_result.valid if organized_result else False
        delta_valid = delta_result.valid if delta_result else False

        if organized_valid and delta_valid:
            typer.echo("✓ All validations passed!")
        else:
            typer.echo("Some validations failed:")
            if not organized_valid:
                typer.echo("  • Organized chunks have issues")
            if not delta_valid:
                typer.echo("  • Delta tables have issues")
    elif target == "raw":
        raw_result = results.get("raw")
        if raw_result and raw_result.valid:
            typer.echo("\n✓ Raw timeseries validation passed!")
        else:
            typer.echo("\n✗ Raw timeseries validation failed!")
