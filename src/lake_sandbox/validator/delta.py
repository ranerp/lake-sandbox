from pathlib import Path

import duckdb
import typer

from lake_sandbox.reorg_pattern.delta.delta_partitions import get_delta_partitions
from lake_sandbox.reorg_pattern.delta.validation import validate_delta_table
from lake_sandbox.utils.performance import monitor_performance
from lake_sandbox.validator.cross_validation import (
    cross_validate_partitions_with_organized,
)
from lake_sandbox.validator.models import DeltaValidationResult, TableDetail


@monitor_performance()
def validate_delta_tables(
    delta_dir: str,
    verbose: bool = False,
    organized_dir: str | None = None,
) -> DeltaValidationResult:
    """Validate partitioned Delta table contains complete timeseries data for each parcel.

    Args:
        delta_dir: Directory containing the partitioned Delta Lake table
        verbose: Show detailed validation info
        organized_dir: Optional directory with organized chunks for cross-validation

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
            error="Directory not found",
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
            error="No partitioned Delta table found",
        )

    typer.echo("Found partitioned Delta table")
    return validate_partitioned_delta_table(
        str(partitioned_table_path), verbose, organized_dir
    )


def validate_partitioned_delta_table(
    delta_table_path: str,
    verbose: bool = False,
    organized_dir: str | None = None,
) -> DeltaValidationResult:
    """Validate a single partitioned Delta table.

    Args:
        delta_table_path: Path to the partitioned Delta table
        verbose: Show detailed validation info
        organized_dir: Optional directory with organized chunks for cross-validation

    Returns:
        Dictionary with validation results and statistics
    """

    try:
        # Validate Delta table
        is_valid, dt, error = validate_delta_table(Path(delta_table_path))

        if not is_valid or dt is None:
            return DeltaValidationResult(
                valid=False,
                total_tables=1,
                table_details=[],
                total_unique_parcels=0,
                total_records=0,
                issues=[error] if error is not None else [],
                error=error,
            )

        version = dt.version()
        file_count = len(dt.files())

        # Get partitions information
        partitions = get_delta_partitions(dt)

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
        (
            total_records,
            unique_parcels,
            unique_dates,
            unique_chunks,
            min_date,
            max_date,
            unique_combinations,
        ) = result

        # Check completeness
        expected_combinations = unique_parcels * unique_dates
        missing_combinations = expected_combinations - unique_combinations
        completeness_pct = (
            (unique_combinations / expected_combinations * 100)
            if expected_combinations > 0
            else 0
        )

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
            part_total, part_parcels, part_dates, part_min_date, part_max_date = (
                part_result
            )

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
                completeness_pct=100.0,  # Assume complete for individual partitions
            )
            table_details.append(table_detail)

            if verbose:
                typer.echo(
                    f"  ✓ Partition {partition}: {part_parcels:,} parcels, {part_dates} dates, {part_total:,} records"
                )

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

        # Cross-validate with organized data if available
        if organized_dir:
            cross_validation_issues = cross_validate_partitions_with_organized(
                Path(delta_table_path), partitions, organized_dir, conn, verbose
            )
            issues.extend(cross_validation_issues)

        conn.close()

        # Summary
        typer.echo("\n=== VALIDATION SUMMARY ===")
        typer.echo(
            f"Partitioned Delta table: 1 table with {len(partitions)} partitions"
        )
        typer.echo(f"Total unique parcels: {unique_parcels:,}")
        typer.echo(f"Total records: {total_records:,}")
        typer.echo(f"Date range: {min_date} to {max_date}")
        typer.echo(f"Completeness: {completeness_pct:.1f}%")

        # Determine overall validity
        valid = len(issues) <= 0

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
            issues=issues,
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
            error=str(e),
        )


def _cross_validate_with_organized_data(
    delta_table_path: str, partitions: set[str], organized_dir: str, conn, verbose: bool
) -> list[str]:
    """Cross-validate Delta partitions with organized chunks.

    Args:
        delta_table_path: Path to the Delta table
        partitions: Set of Delta partition IDs
        organized_dir: Directory containing organized chunks
        conn: DuckDB connection
        verbose: Whether to show detailed output

    Returns:
        List of validation issues found
    """
    typer.echo("\n=== CROSS-VALIDATING WITH ORGANIZED DATA ===")
    issues: list[str] = []

    organized_path = Path(organized_dir)
    if not organized_path.exists():
        typer.echo(f"⚠ Organized directory {organized_dir} not found")
        return issues

    organized_chunk_dirs = [
        d
        for d in organized_path.iterdir()
        if d.is_dir() and d.name.startswith("parcel_chunk=")
    ]

    if not organized_chunk_dirs:
        typer.echo("⚠ No organized chunks found for cross-validation")
        return issues

    typer.echo(f"Found {len(organized_chunk_dirs)} organized chunks to compare")

    # Compare record counts for each partition
    for partition in sorted(partitions):
        partition_issues = _validate_partition_vs_chunk(
            delta_table_path, partition, organized_path, conn, verbose
        )
        issues.extend(partition_issues)

    # Check for missing or extra partitions
    organized_partitions = {d.name.split("=")[1] for d in organized_chunk_dirs}
    delta_partitions = set(partitions)

    missing_in_delta = organized_partitions - delta_partitions
    if missing_in_delta:
        issue = f"Organized chunks missing in Delta table: {sorted(missing_in_delta)}"
        issues.append(issue)
        if verbose:
            typer.echo(f"  ⚠ {issue}")

    extra_in_delta = delta_partitions - organized_partitions
    if extra_in_delta:
        issue = (
            f"Delta partitions not found in organized chunks: {sorted(extra_in_delta)}"
        )
        issues.append(issue)
        if verbose:
            typer.echo(f"  ⚠ {issue}")

    return issues


def _validate_partition_vs_chunk(
    delta_table_path: str, partition: str, organized_path: Path, conn, verbose: bool
) -> list[str]:
    """Validate a single Delta partition against its organized chunk.

    Args:
        delta_table_path: Path to the Delta table
        partition: Partition ID to validate
        organized_path: Path to organized data directory
        conn: DuckDB connection
        verbose: Whether to show detailed output

    Returns:
        List of validation issues for this partition
    """
    issues: list[str] = []

    chunk_dir = organized_path / f"parcel_chunk={partition}"
    chunk_file = chunk_dir / "data.parquet"

    if not chunk_file.exists():
        issue = f"Partition {partition}: Corresponding organized chunk not found at {chunk_file}"
        issues.append(issue)
        if verbose:
            typer.echo(f"  ⚠ {issue}")
        return issues

    try:
        # Get organized chunk record count (raw)
        organized_raw_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{chunk_file}')
        """).fetchone()[0]

        # Get organized chunk deduplicated count (same as Delta should have)
        organized_dedup_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT parcel_id, date
                FROM read_parquet('{chunk_file}')
            )
        """).fetchone()[0]

        # Get Delta partition record count
        delta_count = conn.execute(f"""
            SELECT COUNT(*) FROM delta_scan('{delta_table_path}')
            WHERE parcel_chunk = '{partition}'
        """).fetchone()[0]

        if organized_dedup_count != delta_count:
            issue = f"Partition {partition}: Record count mismatch - organized (deduplicated): {organized_dedup_count:,}, delta: {delta_count:,}"
            issues.append(issue)
            if verbose:
                typer.echo(f"  ✗ {issue}")
                typer.echo(
                    f"    📊 Raw organized count: {organized_raw_count:,}, deduplicated: {organized_dedup_count:,}"
                )
        else:
            if verbose:
                dedup_pct = (
                    (organized_dedup_count / organized_raw_count * 100)
                    if organized_raw_count > 0
                    else 0
                )
                typer.echo(
                    f"  ✓ Partition {partition}: Record counts match ({organized_dedup_count:,} records)"
                )
                typer.echo(
                    f"    📊 Deduplication: {organized_raw_count:,} → {organized_dedup_count:,} ({dedup_pct:.1f}% kept)"
                )

            # Perform deeper per-parcel validation
            if verbose:
                typer.echo(
                    f"    🔍 Validating per-parcel data points for partition {partition}..."
                )
            per_parcel_issues = _validate_per_parcel_data_points(
                delta_table_path, partition, chunk_file, conn, verbose
            )
            issues.extend(per_parcel_issues)

    except Exception as e:
        issue = f"Partition {partition}: Failed to compare with organized chunk ({e})"
        issues.append(issue)
        if verbose:
            typer.echo(f"  ✗ {issue}")

    return issues


def _validate_per_parcel_data_points(
    delta_table_path: str, partition: str, chunk_file: Path, conn, verbose: bool
) -> list[str]:
    """Validate that each parcel has the same number of data points in both organized and Delta.

    Args:
        delta_table_path: Path to the Delta table
        partition: Partition ID being validated
        chunk_file: Path to the organized chunk file
        conn: DuckDB connection
        verbose: Whether to show detailed output

    Returns:
        List of validation issues for per-parcel data points
    """
    issues: list[str] = []

    try:
        # Get data points per parcel from organized chunk (deduplicated)
        organized_parcel_counts = conn.execute(f"""
            SELECT parcel_id, COUNT(*) as count
            FROM (
                SELECT DISTINCT parcel_id, date
                FROM read_parquet('{chunk_file}')
            )
            GROUP BY parcel_id
            ORDER BY parcel_id
        """).fetchdf()

        # Get data points per parcel from Delta partition
        delta_parcel_counts = conn.execute(f"""
            SELECT parcel_id, COUNT(*) as count
            FROM delta_scan('{delta_table_path}')
            WHERE parcel_chunk = '{partition}'
            GROUP BY parcel_id
            ORDER BY parcel_id
        """).fetchdf()

        # Convert to dictionaries for easier comparison
        organized_counts = dict(
            zip(
                organized_parcel_counts["parcel_id"],
                organized_parcel_counts["count"],
                strict=False,
            )
        )
        delta_counts = dict(
            zip(
                delta_parcel_counts["parcel_id"],
                delta_parcel_counts["count"],
                strict=False,
            )
        )

        # Check for missing parcels in either direction
        organized_parcels = set(organized_counts.keys())
        delta_parcels = set(delta_counts.keys())

        missing_in_delta = organized_parcels - delta_parcels
        if missing_in_delta:
            issue = f"Partition {partition}: {len(missing_in_delta)} parcels missing in Delta table"
            issues.append(issue)
            if verbose:
                sample_missing = list(missing_in_delta)[:3]
                typer.echo(
                    f"    ✗ Missing parcels in Delta: {sample_missing}{'...' if len(missing_in_delta) > 3 else ''}"
                )

        extra_in_delta = delta_parcels - organized_parcels
        if extra_in_delta:
            issue = f"Partition {partition}: {len(extra_in_delta)} extra parcels in Delta table"
            issues.append(issue)
            if verbose:
                sample_extra = list(extra_in_delta)[:3]
                typer.echo(
                    f"    ✗ Extra parcels in Delta: {sample_extra}{'...' if len(extra_in_delta) > 3 else ''}"
                )

        # Compare data point counts for common parcels
        common_parcels = organized_parcels & delta_parcels
        mismatched_parcels = []

        for parcel_id in common_parcels:
            organized_count = organized_counts[parcel_id]
            delta_count = delta_counts[parcel_id]

            if organized_count != delta_count:
                mismatched_parcels.append((parcel_id, organized_count, delta_count))

        if mismatched_parcels:
            issue = f"Partition {partition}: {len(mismatched_parcels)} parcels have different data point counts"
            issues.append(issue)
            if verbose:
                # Show first few mismatches as examples
                for parcel_id, org_count, delta_count in mismatched_parcels[:3]:
                    typer.echo(
                        f"    ✗ Parcel {parcel_id}: organized={org_count}, delta={delta_count}"
                    )
                if len(mismatched_parcels) > 3:
                    typer.echo(
                        f"    ... and {len(mismatched_parcels) - 3} more parcels with mismatches"
                    )

        # Success message if everything matches
        if not issues and verbose:
            typer.echo(
                f"    ✓ All {len(common_parcels)} parcels have matching data point counts"
            )

    except Exception as e:
        issue = (
            f"Partition {partition}: Failed to validate per-parcel data points ({e})"
        )
        issues.append(issue)
        if verbose:
            typer.echo(f"    ✗ {issue}")

    return issues
