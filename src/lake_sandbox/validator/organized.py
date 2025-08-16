from pathlib import Path
from typing import Any, cast

import duckdb
import typer

from lake_sandbox.utils.performance import monitor_performance


@monitor_performance()
def validate_organized_chunks(
    organized_dir: str,
    expected_chunk_size: int,
    expected_tiles: int,
    verbose: bool = False,
) -> dict[str, Any]:
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
        return {"valid": False, "error": "Directory not found"}

    # Find all parcel chunk directories
    chunk_dirs = [d for d in organized_path.iterdir() if
                  d.is_dir() and d.name.startswith("parcel_chunk=")]
    if not chunk_dirs:
        typer.echo("Error: No parcel_chunk directories found")
        return {"valid": False, "error": "No chunks found"}

    typer.echo(f"Found {len(chunk_dirs)} parcel chunks")

    conn = duckdb.connect()
    validation_results = {
        "valid": True,
        "total_chunks": len(chunk_dirs),
        "chunk_details": [],
        "total_unique_parcels": 0,
        "total_records": 0,
        "issues": []
    }

    unique_parcels_overall: set[str] = set()

    for chunk_dir in sorted(chunk_dirs):
        chunk_name = chunk_dir.name
        data_file = chunk_dir / "data.parquet"

        if not data_file.exists():
            issue = f"Missing data.parquet in {chunk_name}"
            cast(list[str], validation_results["issues"]).append(issue)
            validation_results["valid"] = False
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
            total_records, unique_parcels, unique_dates, min_date, max_date, duplicates = result

            # Get sample parcel IDs to check for overlaps
            sample_parcels_query = f"SELECT DISTINCT parcel_id FROM read_parquet('{data_file}') LIMIT 10"
            sample_parcels = [row[0] for row in
                              conn.execute(sample_parcels_query).fetchall()]

            chunk_detail = {
                "chunk_name": chunk_name,
                "total_records": total_records,
                "unique_parcels": unique_parcels,
                "unique_dates": unique_dates,
                "date_range": f"{min_date} to {max_date}",
                "duplicate_records": duplicates,
                "sample_parcels": sample_parcels[:5]  # First 5 for display
            }

            cast(list[dict[str, Any]], validation_results["chunk_details"]).append(chunk_detail)
            validation_results["total_records"] += total_records

            # Check for expected duplicates based on tiles
            expected_duplicates = unique_parcels * unique_dates * (expected_tiles - 1)

            # Check chunk size expectations
            if unique_parcels > expected_chunk_size:
                issue = f"{chunk_name}: Too many parcels ({unique_parcels} > {expected_chunk_size})"
                cast(list[str], validation_results["issues"]).append(issue)
                validation_results["valid"] = False

            # Check for parcel overlaps between chunks
            chunk_parcels = set(conn.execute(
                f"SELECT DISTINCT parcel_id FROM read_parquet('{data_file}')").fetchall())
            chunk_parcels = {p[0] for p in chunk_parcels}

            overlap = unique_parcels_overall.intersection(chunk_parcels)
            if overlap:
                issue = f"{chunk_name}: Parcel overlap with other chunks ({len(overlap)} parcels)"
                cast(list[str], validation_results["issues"]).append(issue)
                validation_results["valid"] = False
                if verbose:
                    typer.echo(f"  ⚠ Overlapping parcels: {list(overlap)[:5]}...")

            unique_parcels_overall.update(chunk_parcels)

            if verbose:
                typer.echo(
                    f"  ✓ {chunk_name}: {unique_parcels:,} parcels, {unique_dates} dates, {total_records:,} records")
                if duplicates > 0:
                    typer.echo(
                        f"    • {duplicates:,} duplicate records (expected ~{expected_duplicates:,} from {expected_tiles} tiles)")

        except Exception as e:
            issue = f"{chunk_name}: Failed to read ({e})"
            cast(list[str], validation_results["issues"]).append(issue)
            validation_results["valid"] = False
            if verbose:
                typer.echo(f"  ✗ {issue}")

    conn.close()

    validation_results["total_unique_parcels"] = len(unique_parcels_overall)

    # Summary
    typer.echo("\n=== VALIDATION SUMMARY ===")
    typer.echo(f"Total chunks: {validation_results['total_chunks']}")
    typer.echo(f"Total unique parcels: {validation_results['total_unique_parcels']:,}")
    typer.echo(f"Total records: {validation_results['total_records']:,}")

    if validation_results["valid"]:
        typer.echo("✓ All chunks are valid!")
    else:
        typer.echo(f"✗ Found {len(cast(list[str], validation_results['issues']))} issues:")
        for issue in cast(list[str], validation_results["issues"]):
            typer.echo(f"  • {issue}")

    return validation_results
