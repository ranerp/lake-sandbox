from pathlib import Path

import duckdb
import typer

from lake_sandbox.utils.performance import monitor_performance
from lake_sandbox.validator.models import ChunkDetail, OrganizedValidationResult


@monitor_performance()
def validate_organized_chunks(
    organized_dir: str,
    expected_chunk_size: int,
    expected_tiles: int,
    expected_dates: int | None = None,
    raw_dir: str | None = None,
    verbose: bool = False,
) -> OrganizedValidationResult:
    """Validate organized parcel chunks have correct structure and parcel counts.

    Args:
        organized_dir: Directory with reorganized parquet files partitioned by parcel_chunk
        expected_chunk_size: Expected number of parcels per chunk
        expected_tiles: Expected number of UTM tiles (affects duplicate count)
        expected_dates: Expected dates need to be in the organized chunks
        raw_dir: Directory of raw data that was used to generate the organized data
        verbose: Show detailed validation info

    Returns:
        Dictionary with validation results and statistics
    """

    typer.echo("=== VALIDATING ORGANIZED CHUNKS ===")
    typer.echo(f"Directory: {organized_dir}")
    typer.echo(f"Expected chunk size: {expected_chunk_size:,} parcels")
    typer.echo(f"Expected tiles: {expected_tiles}")

    # Auto-detect expected dates from raw directory if not provided
    if expected_dates is None and raw_dir is not None:
        typer.echo(f"Auto-detecting expected dates from raw directory: {raw_dir}")
        raw_path = Path(raw_dir)
        if raw_path.exists():
            try:
                conn_temp = duckdb.connect()
                raw_files_pattern = str(raw_path / "**/*.parquet")
                dates_result = conn_temp.execute(
                    f"SELECT COUNT(DISTINCT date) FROM read_parquet('{raw_files_pattern}')"
                ).fetchone()
                if dates_result and dates_result[0] > 0:
                    expected_dates = dates_result[0]
                    typer.echo(f"Found {expected_dates} unique dates in raw data")
                else:
                    typer.echo("⚠ Could not detect dates from raw data")
                conn_temp.close()
            except Exception as e:
                typer.echo(f"⚠ Failed to read raw data for date detection: {e}")
        else:
            typer.echo(f"⚠ Raw directory {raw_dir} does not exist")

    if expected_dates is not None:
        typer.echo(f"Expected dates per parcel: {expected_dates}")

    organized_path = Path(organized_dir)
    if not organized_path.exists():
        typer.echo(f"Error: Directory {organized_dir} does not exist")
        return OrganizedValidationResult(
            valid=False,
            total_chunks=0,
            chunk_details=[],
            total_unique_parcels=0,
            total_records=0,
            issues=[],
            error="Directory not found",
        )

    # Find all parcel chunk directories
    chunk_dirs = [
        d
        for d in organized_path.iterdir()
        if d.is_dir() and d.name.startswith("parcel_chunk=")
    ]
    if not chunk_dirs:
        typer.echo("Error: No parcel_chunk directories found")
        return OrganizedValidationResult(
            valid=False,
            total_chunks=0,
            chunk_details=[],
            total_unique_parcels=0,
            total_records=0,
            issues=[],
            error="No chunks found",
        )

    typer.echo(f"Found {len(chunk_dirs)} parcel chunks")

    conn = duckdb.connect()
    chunk_details: list[ChunkDetail] = []
    issues: list[str] = []

    unique_parcels_overall: set[str] = set()
    total_records = 0

    for chunk_dir in sorted(chunk_dirs):
        chunk_name = chunk_dir.name
        data_file = chunk_dir / "data.parquet"

        if not data_file.exists():
            issue = f"Missing data.parquet in {chunk_name}"
            issues.append(issue)
            if verbose:
                typer.echo(f"  ✗ {issue}")
            continue

        try:
            # Get chunk statistics
            stats_query = f"""
                WITH combo_count AS (
                    SELECT COUNT(*) as unique_combinations
                    FROM (
                        SELECT DISTINCT parcel_id, date
                        FROM read_parquet('{data_file}')
                    )
                )
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT parcel_id) as unique_parcels,
                    COUNT(DISTINCT date) as unique_dates,
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(*) - (SELECT unique_combinations FROM combo_count) as duplicate_records
                FROM read_parquet('{data_file}')
            """

            result = conn.execute(stats_query).fetchone()
            if result is None:
                raise ValueError("Query returned no results")
            (
                chunk_total_records,
                unique_parcels,
                unique_dates,
                min_date,
                max_date,
                duplicates,
            ) = result

            # Get sample parcel IDs to check for overlaps
            sample_parcels_query = (
                f"SELECT DISTINCT parcel_id FROM read_parquet('{data_file}') LIMIT 10"
            )
            sample_parcels = [
                row[0] for row in conn.execute(sample_parcels_query).fetchall()
            ]

            chunk_detail = ChunkDetail(
                chunk_name=chunk_name,
                total_records=chunk_total_records,
                unique_parcels=unique_parcels,
                unique_dates=unique_dates,
                date_range=f"{min_date} to {max_date}",
                duplicate_records=duplicates,
                sample_parcels=sample_parcels[:5],  # First 5 for display
            )

            chunk_details.append(chunk_detail)
            total_records += chunk_total_records

            # Check for expected duplicates based on tiles
            expected_duplicates = unique_parcels * unique_dates * (expected_tiles - 1)

            # Check data completeness: each parcel should have all expected dates
            expected_date_count = (
                expected_dates if expected_dates is not None else unique_dates
            )

            incomplete_parcels_query = f"""
                WITH parcel_date_counts AS (
                    SELECT parcel_id, COUNT(DISTINCT date) as date_count
                    FROM read_parquet('{data_file}')
                    GROUP BY parcel_id
                )
                SELECT COUNT(*) as incomplete_count
                FROM parcel_date_counts
                WHERE date_count != {expected_date_count}
            """

            incomplete_result = conn.execute(incomplete_parcels_query).fetchone()
            incomplete_parcels = incomplete_result[0] if incomplete_result else 0

            if incomplete_parcels > 0:
                issue = f"{chunk_name}: {incomplete_parcels} parcels missing dates (expected {expected_date_count} dates per parcel, found {unique_dates} unique dates)"
                issues.append(issue)

            # Check for parcel overlaps between chunks
            chunk_parcels = set(
                conn.execute(
                    f"SELECT DISTINCT parcel_id FROM read_parquet('{data_file}')"
                ).fetchall()
            )
            chunk_parcels = {p[0] for p in chunk_parcels}

            overlap = unique_parcels_overall.intersection(chunk_parcels)
            if overlap:
                issue = f"{chunk_name}: Parcel overlap with other chunks ({len(overlap)} parcels)"
                issues.append(issue)
                if verbose:
                    typer.echo(f"  ⚠ Overlapping parcels: {list(overlap)[:5]}...")

            unique_parcels_overall.update(chunk_parcels)

            if verbose:
                chunk_size_info = (
                    f"~{expected_chunk_size:,}"
                    if unique_parcels != expected_chunk_size
                    else f"{expected_chunk_size:,}"
                )
                typer.echo(
                    f"  ✓ {chunk_name}: {unique_parcels:,} parcels (target: {chunk_size_info}), {unique_dates} dates, {chunk_total_records:,} records"
                )
                if duplicates > 0:
                    typer.echo(
                        f"    • {duplicates:,} duplicate records (expected ~{expected_duplicates:,} from {expected_tiles} tiles)"
                    )
                if incomplete_parcels > 0:
                    typer.echo(
                        f"    • {incomplete_parcels:,} parcels with incomplete dates"
                    )

        except Exception as e:
            issue = f"{chunk_name}: Failed to read ({e})"
            issues.append(issue)
            if verbose:
                typer.echo(f"  ✗ {issue}")

    conn.close()

    total_unique_parcels = len(unique_parcels_overall)

    # Summary
    typer.echo("\n=== VALIDATION SUMMARY ===")
    typer.echo(f"Total chunks: {len(chunk_dirs)}")
    typer.echo(f"Total unique parcels: {total_unique_parcels:,}")
    typer.echo(f"Total records: {total_records:,}")

    # Determine overall validity
    valid = len(issues) == 0

    if valid:
        typer.echo("✓ All chunks are valid!")
    else:
        typer.echo(f"✗ Found {len(issues)} issues:")
        for issue in issues:
            typer.echo(f"  • {issue}")

    return OrganizedValidationResult(
        valid=valid,
        total_chunks=len(chunk_dirs),
        chunk_details=chunk_details,
        total_unique_parcels=total_unique_parcels,
        total_records=total_records,
        issues=issues,
    )
