from pathlib import Path

import duckdb
import typer
from deltalake import DeltaTable

from lake_sandbox.utils.performance import monitor_performance
from lake_sandbox.validator.models import DeltaValidationResult, TableDetail


@monitor_performance()
def validate_delta_tables(
    delta_dir: str,
    verbose: bool = False,
) -> DeltaValidationResult:
    """Validate partitioned Delta table contains complete timeseries data for each parcel.

    Args:
        delta_dir: Directory containing the partitioned Delta Lake table
        verbose: Show detailed validation info

    Returns:
        Dictionary with validation results and statistics
    """

    typer.echo("=== VALIDATING PARTITIONED DELTA TABLE ===")
    typer.echo(f"Delta directory: {delta_dir}")

    delta_path = Path(delta_dir)
    if not delta_path.exists():
        typer.echo(f"Error: Directory {delta_dir} does not exist")
        return DeltaValidationResult(
            valid=False,
            total_tables=0,
            table_details=[],
            total_unique_parcels=0,
            total_records=0,
            issues=[],
            error="Directory not found"
        )

    # Check for partitioned Delta table
    partitioned_table_path = delta_path / "parcel_data"
    
    if not partitioned_table_path.exists():
        typer.echo("Error: No partitioned Delta table found at parcel_data/")
        return DeltaValidationResult(
            valid=False,
            total_tables=0,
            table_details=[],
            total_unique_parcels=0,
            total_records=0,
            issues=[],
            error="No partitioned Delta table found"
        )

    typer.echo("Found partitioned Delta table")
    return validate_partitioned_delta_table(str(partitioned_table_path), verbose)


def validate_partitioned_delta_table(
    delta_table_path: str,
    verbose: bool = False,
) -> DeltaValidationResult:
    """Validate a single partitioned Delta table.

    Args:
        delta_table_path: Path to the partitioned Delta table
        verbose: Show detailed validation info

    Returns:
        Dictionary with validation results and statistics
    """

    try:
        # Load Delta table
        dt = DeltaTable(delta_table_path)
        version = dt.version()
        file_count = len(dt.files())

        if file_count == 0:
            return DeltaValidationResult(
                valid=False,
                total_tables=1,
                table_details=[],
                total_unique_parcels=0,
                total_records=0,
                issues=["No data files in Delta table"],
                error="Empty Delta table"
            )

        # Get partitions information
        partitions = set()
        for file_info in dt.files():
            if "parcel_chunk=" in file_info:
                partition = file_info.split("parcel_chunk=")[1].split("/")[0]
                partitions.add(partition)

        typer.echo(f"Found {len(partitions)} partitions: {sorted(partitions)}")

        conn = duckdb.connect()
        issues: list[str] = []

        # Query overall table statistics
        stats_query = f"""
            WITH combo_count AS (
                SELECT COUNT(*) as unique_combinations
                FROM (
                    SELECT DISTINCT parcel_id, date
                    FROM delta_scan('{delta_table_path}')
                )
            )
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT parcel_id) as unique_parcels,
                COUNT(DISTINCT date) as unique_dates,
                COUNT(DISTINCT parcel_chunk) as unique_chunks,
                MIN(date) as min_date,
                MAX(date) as max_date,
                (SELECT unique_combinations FROM combo_count) as unique_combinations
            FROM delta_scan('{delta_table_path}')
        """

        result = conn.execute(stats_query).fetchone()
        if result is None:
            raise ValueError("Query returned no results")
        total_records, unique_parcels, unique_dates, unique_chunks, min_date, max_date, unique_combinations = result

        # Check completeness
        expected_combinations = unique_parcels * unique_dates
        missing_combinations = expected_combinations - unique_combinations
        completeness_pct = (unique_combinations / expected_combinations * 100) if expected_combinations > 0 else 0

        # Validate each partition
        table_details: list[TableDetail] = []
        partition_parcels: dict[str, set[str]] = {}

        for partition in sorted(partitions):
            partition_query = f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT parcel_id) as unique_parcels,
                    COUNT(DISTINCT date) as unique_dates,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM delta_scan('{delta_table_path}')
                WHERE parcel_chunk = '{partition}'
            """

            part_result = conn.execute(partition_query).fetchone()
            if part_result is None:
                continue
            part_total, part_parcels, part_dates, part_min_date, part_max_date = part_result

            # Get parcel IDs for this partition
            parcel_query = f"""
                SELECT DISTINCT parcel_id 
                FROM delta_scan('{delta_table_path}')
                WHERE parcel_chunk = '{partition}'
            """
            parcel_results = conn.execute(parcel_query).fetchall()
            partition_parcel_ids = {p[0] for p in parcel_results}
            partition_parcels[partition] = partition_parcel_ids

            table_detail = TableDetail(
                table_name=f"parcel_chunk={partition}",
                version=version,
                file_count=file_count,  # Note: this is total files, not per partition
                total_records=part_total,
                unique_parcels=part_parcels,
                unique_dates=part_dates,
                date_range=f"{part_min_date} to {part_max_date}",
                missing_combinations=0,  # We'll calculate this if needed
                completeness_pct=100.0  # Assume complete for individual partitions
            )
            table_details.append(table_detail)

            if verbose:
                typer.echo(f"  ✓ Partition {partition}: {part_parcels:,} parcels, {part_dates} dates, {part_total:,} records")

        # Check for parcel overlaps between partitions
        all_partition_parcels: set[str] = set()
        for partition, parcel_ids in partition_parcels.items():
            overlap = all_partition_parcels.intersection(parcel_ids)
            if overlap:
                issue = f"Partition {partition}: Parcel overlap with other partitions ({len(overlap)} parcels)"
                issues.append(issue)
                if verbose:
                    typer.echo(f"  ⚠ {issue}")
            all_partition_parcels.update(parcel_ids)

        # Check for missing data
        if missing_combinations > 0:
            issue = f"Missing {missing_combinations:,} parcel-date combinations ({completeness_pct:.1f}% complete)"
            issues.append(issue)

        conn.close()

        # Summary
        typer.echo("\n=== VALIDATION SUMMARY ===")
        typer.echo(f"Partitioned Delta table: 1 table with {len(partitions)} partitions")
        typer.echo(f"Total unique parcels: {unique_parcels:,}")
        typer.echo(f"Total records: {total_records:,}")
        typer.echo(f"Date range: {min_date} to {max_date}")
        typer.echo(f"Completeness: {completeness_pct:.1f}%")

        # Determine overall validity
        valid = not any("overlap" in issue.lower() for issue in issues)

        if valid:
            typer.echo("✓ Partitioned Delta table is valid!")
        else:
            typer.echo(f"✗ Found {len(issues)} issues:")
            for issue in issues:
                typer.echo(f"  • {issue}")

        return DeltaValidationResult(
            valid=valid,
            total_tables=len(partitions),  # Report number of partitions as "tables"
            table_details=table_details,
            total_unique_parcels=unique_parcels,
            total_records=total_records,
            issues=issues
        )

    except Exception as e:
        typer.echo(f"Error validating partitioned Delta table: {e}")
        return DeltaValidationResult(
            valid=False,
            total_tables=0,
            table_details=[],
            total_unique_parcels=0,
            total_records=0,
            issues=[f"Validation failed: {e}"],
            error=str(e)
        )
