from pathlib import Path
from typing import Any, cast

import duckdb
import typer
from deltalake import DeltaTable

from lake_sandbox.utils.performance import monitor_performance


@monitor_performance()
def validate_delta_tables(
    delta_dir: str,
    verbose: bool = False,
) -> dict[str, Any]:
    """Validate Delta tables contain complete timeseries data for each parcel.

    Args:
        delta_dir: Directory containing Delta Lake tables
        verbose: Show detailed validation info

    Returns:
        Dictionary with validation results and statistics
    """

    typer.echo("=== VALIDATING DELTA TABLES ===")
    typer.echo(f"Delta directory: {delta_dir}")

    delta_path = Path(delta_dir)
    if not delta_path.exists():
        typer.echo(f"Error: Directory {delta_dir} does not exist")
        return {"valid": False, "error": "Directory not found"}

    # Find all Delta table directories
    delta_dirs = [d for d in delta_path.iterdir() if
                  d.is_dir() and d.name.startswith("parcel_chunk=")]
    if not delta_dirs:
        typer.echo("Error: No Delta table directories found")
        return {"valid": False, "error": "No Delta tables found"}

    typer.echo(f"Found {len(delta_dirs)} Delta tables")

    conn = duckdb.connect()
    validation_results = {
        "valid": True,
        "total_tables": len(delta_dirs),
        "table_details": [],
        "total_unique_parcels": 0,
        "total_records": 0,
        "issues": []
    }

    unique_parcels_overall: set[str] = set()
    expected_date_ranges: dict[tuple[str, str], list[str]] = {}

    for delta_table_dir in sorted(delta_dirs):
        table_name = delta_table_dir.name

        try:
            # Load Delta table
            dt = DeltaTable(str(delta_table_dir))
            version = dt.version()
            file_count = len(dt.files())

            if file_count == 0:
                issue = f"{table_name}: No data files"
                cast(list[str], validation_results["issues"]).append(issue)
                validation_results["valid"] = False
                if verbose:
                    typer.echo(f"  ✗ {issue}")
                continue

            # Query Delta table statistics
            stats_query = f"""
                WITH combo_count AS (
                    SELECT COUNT(*) as unique_combinations
                    FROM (
                        SELECT DISTINCT parcel_id, date
                        FROM delta_scan('{delta_table_dir}')
                    )
                )
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT parcel_id) as unique_parcels,
                    COUNT(DISTINCT date) as unique_dates,
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    (SELECT unique_combinations FROM combo_count) as unique_combinations
                FROM delta_scan('{delta_table_dir}')
            """

            result = conn.execute(stats_query).fetchone()
            if result is None:
                raise ValueError("Query returned no results")
            total_records, unique_parcels, unique_dates, min_date, max_date, unique_combinations = result

            # Check for missing dates per parcel (should be unique_parcels * unique_dates = unique_combinations)
            expected_combinations = unique_parcels * unique_dates
            missing_combinations = expected_combinations - unique_combinations

            table_detail = {
                "table_name": table_name,
                "version": version,
                "file_count": file_count,
                "total_records": total_records,
                "unique_parcels": unique_parcels,
                "unique_dates": unique_dates,
                "date_range": f"{min_date} to {max_date}",
                "missing_combinations": missing_combinations,
                "completeness_pct": (
                        unique_combinations / expected_combinations * 100) if expected_combinations > 0 else 0
            }

            cast(list[dict[str, Any]], validation_results["table_details"]).append(table_detail)
            validation_results["total_records"] += total_records

            # Check for missing dates
            if missing_combinations > 0:
                issue = f"{table_name}: Missing {missing_combinations:,} parcel-date combinations ({table_detail['completeness_pct']:.1f}% complete)"
                cast(list[str], validation_results["issues"]).append(issue)
                # Don't mark as invalid for missing combinations - this might be expected

            # Check for parcel overlaps between tables
            table_parcels = set(conn.execute(
                f"SELECT DISTINCT parcel_id FROM delta_scan('{delta_table_dir}')").fetchall())
            table_parcels = {p[0] for p in table_parcels}

            overlap = unique_parcels_overall.intersection(table_parcels)
            if overlap:
                issue = f"{table_name}: Parcel overlap with other tables ({len(overlap)} parcels)"
                cast(list[str], validation_results["issues"]).append(issue)
                validation_results["valid"] = False
                if verbose:
                    typer.echo(f"  ⚠ Overlapping parcels: {list(overlap)[:5]}...")

            unique_parcels_overall.update(table_parcels)

            # Track date ranges for consistency
            date_range = (min_date, max_date)
            if date_range not in expected_date_ranges:
                expected_date_ranges[date_range] = []
            expected_date_ranges[date_range].append(table_name)

            if verbose:
                typer.echo(
                    f"  ✓ {table_name}: {unique_parcels:,} parcels, {unique_dates} dates, {total_records:,} records ({table_detail['completeness_pct']:.1f}% complete)")

        except Exception as e:
            issue = f"{table_name}: Failed to read ({e})"
            validation_results["issues"].append(issue)
            validation_results["valid"] = False
            if verbose:
                typer.echo(f"  ✗ {issue}")

    conn.close()

    validation_results["total_unique_parcels"] = len(unique_parcels_overall)

    # Check date range consistency
    if len(expected_date_ranges) > 1:
        issue = f"Inconsistent date ranges across tables: {list(expected_date_ranges.keys())}"
        validation_results["issues"].append(issue)
        typer.echo(f"  ⚠ {issue}")

    # Summary
    typer.echo("\n=== VALIDATION SUMMARY ===")
    typer.echo(f"Total Delta tables: {validation_results['total_tables']}")
    typer.echo(f"Total unique parcels: {validation_results['total_unique_parcels']:,}")
    typer.echo(f"Total records: {validation_results['total_records']:,}")

    if validation_results["valid"]:
        typer.echo("✓ All Delta tables are valid!")
    else:
        typer.echo(f"✗ Found {len(cast(list[str], validation_results['issues']))} issues:")
        for issue in cast(list[str], validation_results["issues"]):
            typer.echo(f"  • {issue}")

    return validation_results
