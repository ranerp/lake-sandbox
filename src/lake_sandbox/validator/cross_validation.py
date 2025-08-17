"""Cross-validation utilities for Delta tables and organized data."""

from pathlib import Path

import duckdb
import typer

from lake_sandbox.reorg_pattern.reorganize.validation import validate_parquet_file


def cross_validate_organized_chunk(delta_table_path: Path,
                                   partition: str,
                                   organized_chunk_file: Path,
                                   conn: duckdb.DuckDBPyConnection,
                                   verbose: bool = False) -> tuple[bool, str | None]:
    """Cross-validate Delta partition against organized chunk.

    Args:
        delta_table_path: Path to the Delta table
        partition: Partition identifier
        organized_chunk_file: Path to the organized chunk file
        conn: DuckDB connection
        verbose: Whether to show detailed output

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Validate organized chunk
    is_valid, org_raw_count, error = validate_parquet_file(organized_chunk_file, conn)
    if not is_valid:
        return False, f"Organized chunk validation failed: {error}"

    try:
        # Get organized chunk deduplicated count
        org_dedup_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT parcel_id, date
                FROM read_parquet('{organized_chunk_file}')
            )
        """).fetchone()[0]

        # Get Delta partition record count
        delta_count = conn.execute(f"""
            SELECT COUNT(*) FROM delta_scan('{delta_table_path}')
            WHERE parcel_chunk = '{partition}'
        """).fetchone()[0]

        if org_dedup_count != delta_count:
            return False, f"Record count mismatch - organized (deduplicated): {org_dedup_count:,}, delta: {delta_count:,}"

        if verbose:
            dedup_pct = (
                    org_dedup_count / org_raw_count * 100) if org_raw_count > 0 else 0
            typer.echo(
                f"  ✓ Partition {partition}: Record counts match ({org_dedup_count:,} records)")
            typer.echo(
                f"    📊 Deduplication: {org_raw_count:,} → {org_dedup_count:,} ({dedup_pct:.1f}% kept)")

        return True, None

    except Exception as e:
        return False, f"Cross-validation query failed: {e}"


def cross_validate_partitions_with_organized(delta_table_path: Path,
                                             partitions: set[str],
                                             organized_dir: str,
                                             conn: duckdb.DuckDBPyConnection,
                                             verbose: bool = False) -> list:
    """Cross-validate all Delta partitions with organized chunks.

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
    issues = []

    organized_path = Path(organized_dir)
    if not organized_path.exists():
        typer.echo(f"⚠ Organized directory {organized_dir} not found")
        return issues

    organized_chunk_dirs = [
        d for d in organized_path.iterdir()
        if d.is_dir() and d.name.startswith("parcel_chunk=")
    ]

    if not organized_chunk_dirs:
        typer.echo("⚠ No organized chunks found for cross-validation")
        return issues

    typer.echo(f"Found {len(organized_chunk_dirs)} organized chunks to compare")

    # Compare record counts for each partition
    for partition in sorted(partitions):
        chunk_file = organized_path / f"parcel_chunk={partition}" / "data.parquet"

        if not chunk_file.exists():
            issue = f"Partition {partition}: Corresponding organized chunk not found"
            issues.append(issue)
            if verbose:
                typer.echo(f"  ⚠ {issue}")
            continue

        is_valid, error = cross_validate_organized_chunk(
            delta_table_path, partition, chunk_file, conn, verbose
        )

        if not is_valid:
            issues.append(f"Partition {partition}: {error}")
            if verbose:
                typer.echo(f"  ✗ Partition {partition}: {error}")

    # Check for missing or extra partitions
    organized_partitions = {d.name.split('=')[1] for d in organized_chunk_dirs}
    delta_partitions = set(partitions)

    missing_in_delta = organized_partitions - delta_partitions
    if missing_in_delta:
        issue = f"Organized chunks missing in Delta table: {sorted(missing_in_delta)}"
        issues.append(issue)
        if verbose:
            typer.echo(f"  ⚠ {issue}")

    extra_in_delta = delta_partitions - organized_partitions
    if extra_in_delta:
        issue = f"Delta partitions not found in organized chunks: {sorted(extra_in_delta)}"
        issues.append(issue)
        if verbose:
            typer.echo(f"  ⚠ {issue}")

    return issues
