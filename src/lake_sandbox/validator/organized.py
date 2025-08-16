from pathlib import Path

import duckdb
import typer

from lake_sandbox.utils.performance import monitor_performance
from lake_sandbox.validator.models import OrganizedValidationResult, ChunkDetail


@monitor_performance()
def validate_organized_chunks(
    organized_dir: str,
    expected_chunk_size: int,
    expected_tiles: int,
    verbose: bool = False,
) -> OrganizedValidationResult:
    """Validate organized parcel chunks have correct structure and parcel counts.

    Args:
        organized_dir: Directory with reorganized parquet files partitioned by parcel_chunk
        expected_chunk_size: Expected number of parcels per chunk
        expected_tiles: Expected number of UTM tiles (affects duplicate count)
        verbose: Show detailed validation info

    Returns:
        Dictionary with validation results and statistics
    """

    typer.echo("=== VALIDATING ORGANIZED CHUNKS ===")
    typer.echo(f"Directory: {organized_dir}")
    typer.echo(f"Expected chunk size: {expected_chunk_size:,} parcels")
    typer.echo(f"Expected tiles: {expected_tiles}")

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
            error="Directory not found"
        )

    # Find all parcel chunk directories
    chunk_dirs = [d for d in organized_path.iterdir() if
                  d.is_dir() and d.name.startswith("parcel_chunk=")]
    if not chunk_dirs:
        typer.echo("Error: No parcel_chunk directories found")
        return OrganizedValidationResult(
            valid=False,
            total_chunks=0,
            chunk_details=[],
            total_unique_parcels=0,
            total_records=0,
            issues=[],
            error="No chunks found"
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
            chunk_total_records, unique_parcels, unique_dates, min_date, max_date, duplicates = result

            # Get sample parcel IDs to check for overlaps
            sample_parcels_query = f"SELECT DISTINCT parcel_id FROM read_parquet('{data_file}') LIMIT 10"
            sample_parcels = [row[0] for row in
                              conn.execute(sample_parcels_query).fetchall()]

            chunk_detail = ChunkDetail(
                chunk_name=chunk_name,
                total_records=chunk_total_records,
                unique_parcels=unique_parcels,
                unique_dates=unique_dates,
                date_range=f"{min_date} to {max_date}",
                duplicate_records=duplicates,
                sample_parcels=sample_parcels[:5]  # First 5 for display
            )

            chunk_details.append(chunk_detail)
            total_records += chunk_total_records

            # Check for expected duplicates based on tiles
            expected_duplicates = unique_parcels * unique_dates * (expected_tiles - 1)

            # Check chunk size expectations
            if unique_parcels > expected_chunk_size:
                issue = f"{chunk_name}: Too many parcels ({unique_parcels} > {expected_chunk_size})"
                issues.append(issue)
    
            # Check for parcel overlaps between chunks
            chunk_parcels = set(conn.execute(
                f"SELECT DISTINCT parcel_id FROM read_parquet('{data_file}')").fetchall())
            chunk_parcels = {p[0] for p in chunk_parcels}

            overlap = unique_parcels_overall.intersection(chunk_parcels)
            if overlap:
                issue = f"{chunk_name}: Parcel overlap with other chunks ({len(overlap)} parcels)"
                issues.append(issue)
                if verbose:
                    typer.echo(f"  ⚠ Overlapping parcels: {list(overlap)[:5]}...")

            unique_parcels_overall.update(chunk_parcels)

            if verbose:
                typer.echo(
                    f"  ✓ {chunk_name}: {unique_parcels:,} parcels, {unique_dates} dates, {chunk_total_records:,} records")
                if duplicates > 0:
                    typer.echo(
                        f"    • {duplicates:,} duplicate records (expected ~{expected_duplicates:,} from {expected_tiles} tiles)")

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
        issues=issues
    )
