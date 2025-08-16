from pathlib import Path
from typing import Any, cast

import duckdb
import typer

from lake_sandbox.utils.performance import monitor_performance


@monitor_performance()
def validate_raw_timeseries(
    raw_dir: str,
    expected_total_parcels: int,
    verbose: bool = False,
) -> dict[str, Any]:
    """Validate raw timeseries data has correct date structure and parcel counts.

    Args:
        raw_dir: Directory with raw timeseries data partitioned by date
        expected_total_parcels: Expected total unique parcels across all dates
        verbose: Show detailed validation info

    Returns:
        Dictionary with validation results and statistics
    """

    typer.echo("=== VALIDATING RAW TIMESERIES ===")
    typer.echo(f"Directory: {raw_dir}")
    typer.echo(f"Expected total parcels: {expected_total_parcels:,}")

    raw_path = Path(raw_dir)
    if not raw_path.exists():
        typer.echo(f"Error: Directory {raw_dir} does not exist")
        return {"valid": False, "error": "Directory not found"}

    # Find all parquet files in the directory structure
    parquet_files = list(raw_path.glob("**/*.parquet"))

    if not parquet_files:
        typer.echo("Error: No data.parquet files found")
        return {"valid": False, "error": "No parquet files found"}

    typer.echo(f"Found {len(parquet_files)} parquet files")

    conn = duckdb.connect()
    validation_results = {
        "valid": True,
        "total_files": len(parquet_files),
        "file_details": [],
        "total_unique_parcels": 0,
        "total_records": 0,
        "issues": []
    }

    unique_parcels_overall: set[str] = set()
    parcel_counts_per_file: list[int] = []

    for data_file in sorted(parquet_files):
        parts = data_file.parts

        utm_tile = next((p for p in parts if p.startswith("utm_tile=")), "unknown")
        year = next((p for p in parts if p.startswith("year=")), "unknown")
        date_name = next((p for p in parts if p.startswith("date=")), "unknown")
        date_value = date_name.replace("date=",
                                       "") if date_name != "unknown" else "unknown"

        relative_path = str(data_file.relative_to(raw_path))

        try:
            stats_query = f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT parcel_id) as unique_parcels,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM read_parquet('{data_file}')
            """

            result = conn.execute(stats_query).fetchone()
            if result is None:
                raise ValueError("Query returned no results")
            total_records, unique_parcels, min_date, max_date = result

            # Check date consistency within partition
            if date_value != "unknown" and (
                str(min_date) != date_value or str(max_date) != date_value):
                issue = f"{relative_path}: Date inconsistency - partition={date_value}, data range={min_date} to {max_date}"
                cast(list[str], validation_results["issues"]).append(issue)
                validation_results["valid"] = False

            # Get parcel IDs for this file
            parcels_query = f"SELECT DISTINCT parcel_id FROM read_parquet('{data_file}')"
            file_parcels = {row[0] for row in conn.execute(parcels_query).fetchall()}

            file_detail = {
                "file_path": relative_path,
                "utm_tile": utm_tile.replace("utm_tile=",
                                             "") if utm_tile != "unknown" else utm_tile,
                "year": year.replace("year=", "") if year != "unknown" else year,
                "date_partition": date_name,
                "date_value": date_value,
                "total_records": total_records,
                "unique_parcels": unique_parcels,
                "date_range": f"{min_date} to {max_date}",
                "data_consistent": date_value == "unknown" or (
                        str(min_date) == date_value and str(max_date) == date_value)
            }

            cast(list[dict[str, Any]], validation_results["file_details"]).append(file_detail)
            validation_results["total_records"] += total_records
            parcel_counts_per_file.append(unique_parcels)

            # Track unique parcels across all files
            unique_parcels_overall.update(file_parcels)

            if verbose:
                status = "✓" if file_detail["data_consistent"] else "✗"
                typer.echo(
                    f"  {status} {relative_path}: {unique_parcels:,} parcels, {total_records:,} records")
                if not file_detail["data_consistent"]:
                    typer.echo(f"    ⚠ Date range mismatch: {min_date} to {max_date}")

        except Exception as e:
            issue = f"{relative_path}: Failed to read ({e})"
            validation_results["issues"].append(issue)
            validation_results["valid"] = False
            if verbose:
                typer.echo(f"  ✗ {issue}")

    conn.close()

    validation_results["total_unique_parcels"] = len(unique_parcels_overall)

    # Check if we have the expected number of total unique parcels
    if validation_results["total_unique_parcels"] != expected_total_parcels:
        issue = f"Total unique parcels mismatch: expected {expected_total_parcels:,}, found {validation_results['total_unique_parcels']:,}"
        validation_results["issues"].append(issue)
        validation_results["valid"] = False

    # Check parcel count consistency across files
    if parcel_counts_per_file:
        min_parcels = min(parcel_counts_per_file)
        max_parcels = max(parcel_counts_per_file)
        avg_parcels = sum(parcel_counts_per_file) / len(parcel_counts_per_file)

        validation_results["parcel_distribution"] = {
            "min_parcels_per_file": min_parcels,
            "max_parcels_per_file": max_parcels,
            "avg_parcels_per_file": round(avg_parcels),
            "consistent_across_files": min_parcels == max_parcels
        }

        # Check if all files have the same number of parcels (they should for properly generated data)
        if min_parcels != max_parcels:
            issue = f"Inconsistent parcel counts across files: min={min_parcels:,}, max={max_parcels:,}"
            validation_results["issues"].append(issue)
            # Don't mark as invalid - this might be expected in some cases

    # Summary
    typer.echo("\n=== VALIDATION SUMMARY ===")
    typer.echo(f"Total parquet files: {validation_results['total_files']}")
    typer.echo(f"Total unique parcels: {validation_results['total_unique_parcels']:,}")
    typer.echo(f"Total records: {validation_results['total_records']:,}")

    if parcel_counts_per_file:
        typer.echo(
            f"Parcels per file: {cast(dict[str, Any], validation_results['parcel_distribution'])['min_parcels_per_file']:,} - {cast(dict[str, Any], validation_results['parcel_distribution'])['max_parcels_per_file']:,} (avg: {cast(dict[str, Any], validation_results['parcel_distribution'])['avg_parcels_per_file']:,})")

    if validation_results["valid"]:
        typer.echo("✓ Raw timeseries data is valid!")
    else:
        typer.echo(f"✗ Found {len(cast(list[str], validation_results['issues']))} issues:")
        for issue in cast(list[str], validation_results["issues"]):
            typer.echo(f"  • {issue}")

    return validation_results
