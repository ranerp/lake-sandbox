from pathlib import Path
from typing import Literal

import duckdb
import typer
from deltalake import DeltaTable, write_deltalake

from lake_sandbox.reorg_pattern.delta.partition_manager import (
    DeltaTableState,
    check_skip_partition,
    extract_partition_id,
    get_table_info,
)
from lake_sandbox.reorg_pattern.delta.validation import (
    verify_delta_streaming,
)
from lake_sandbox.reorg_pattern.reorganize.validation import update_stats
from lake_sandbox.utils.performance import monitor_performance
from lake_sandbox.validator.models import DeltaConversionProgress, DeltaTableInfo


@monitor_performance()
def convert_to_delta_lake(
    input_dir: str,
    delta_dir: str,
    dry_run: bool = False,
    force: bool = False,
) -> dict[str, int]:
    """Phase 2: Convert reorganized parquet chunks to a single partitioned Delta Lake table.

    Args:
        input_dir: Directory with reorganized parquet files partitioned by parcel_chunk
        delta_dir: Output directory for the single partitioned Delta Lake table
        dry_run: Show what would be done without making changes
        force: Force reprocessing of existing Delta table

    Returns:
        Dictionary with statistics: {'total_chunks': int, 'processed': int, 'skipped': int, 'failed': int}
    """

    typer.echo("=== PHASE 2: CONVERTING TO PARTITIONED DELTA LAKE ===")
    typer.echo(f"Input directory: {input_dir}")
    typer.echo(f"Delta directory: {delta_dir}")

    if dry_run:
        typer.echo("DRY RUN MODE - No Delta table will be created")
    if force:
        typer.echo("FORCE MODE - Existing Delta table will be overwritten")

    input_path = Path(input_dir)
    if not input_path.exists():
        typer.echo(f"Error: Input directory {input_dir} does not exist")
        raise typer.Exit(1)

    # Find all parcel chunk directories
    chunk_dirs = [
        d
        for d in input_path.iterdir()
        if d.is_dir() and d.name.startswith("parcel_chunk=")
    ]
    if not chunk_dirs:
        typer.echo(f"Error: No parcel_chunk directories found in {input_dir}")
        raise typer.Exit(1)

    typer.echo(f"Found {len(chunk_dirs)} parcel chunks to convert")

    if dry_run:
        typer.echo("\nChunks that would be processed:")
        for chunk_dir in sorted(chunk_dirs)[:5]:  # Show first 5
            data_file = chunk_dir / "data.parquet"
            status = "✓" if data_file.exists() else "✗"
            typer.echo(f"  {status} {chunk_dir.name}")
        if len(chunk_dirs) > 5:
            typer.echo(f"  ... and {len(chunk_dirs) - 5} more chunks")
        return {
            "total_chunks": len(chunk_dirs),
            "processed": 0,
            "skipped": 0,
            "failed": 0,
        }

    # Create Delta directory
    delta_path = Path(delta_dir)
    delta_path.mkdir(parents=True, exist_ok=True)

    # Single partitioned Delta table path
    delta_table_path = delta_path / "parcel_data"

    conn = duckdb.connect()

    # Statistics tracking
    stats = {"total_chunks": len(chunk_dirs), "processed": 0, "skipped": 0, "failed": 0}

    # Load Delta table state
    table_state = DeltaTableState.from_path(delta_table_path)

    # Check if Delta table already exists
    if not force and table_state.exists:
        table_info = get_table_info(table_state)
        if table_info:
            version, file_count = table_info
            typer.echo(f"✓ Delta table exists (version {version}, {file_count} files)")
            typer.echo(
                f"Found {len(table_state.existing_partitions)} existing partitions"
            )
        else:
            typer.echo("⚠ Delta table exists but could not get info, recreating...")

    # Process each chunk and stream to Delta table
    first_chunk = True

    for chunk_dir in sorted(chunk_dirs):
        chunk_name = chunk_dir.name
        typer.echo(f"Processing {chunk_name}")

        data_file = chunk_dir / "data.parquet"
        if not data_file.exists():
            typer.echo(f"  ✗ Skipping {chunk_name} - no data.parquet found")
            update_stats(stats, {"failed": 1})
            continue

        # Skip if partition already exists and not forcing
        should_skip, skip_stats = check_skip_partition(table_state, chunk_name, force)
        if should_skip:
            update_stats(stats, skip_stats)
            continue

        try:
            # Read and deduplicate the parquet data
            # Add parcel_chunk column and remove duplicates
            partition_id = extract_partition_id(chunk_name)
            df = conn.execute(f"""
                SELECT
                    DISTINCT ON (parcel_id, date) *,
                    '{partition_id}' as parcel_chunk
                FROM read_parquet('{data_file}')
                ORDER BY parcel_id, date
            """).fetchdf()

            if df.empty:
                typer.echo(f"  ✗ Skipping {chunk_name} - empty dataset")
                update_stats(stats, {"failed": 1})
                continue

            # Determine write mode for this chunk
            if first_chunk:
                write_mode: Literal["overwrite", "append"] = (
                    "overwrite" if not table_state.exists or force else "append"
                )
                first_chunk = False
            else:
                write_mode = "append"

            # Stream this chunk directly to Delta table
            write_deltalake(
                table_or_uri=str(delta_table_path),
                data=df,
                mode=write_mode,
                partition_by=["parcel_chunk"],  # Partition by parcel_chunk
            )

            streaming_stats = verify_delta_streaming(
                chunk_name, len(df), delta_table_path
            )
            update_stats(stats, streaming_stats)

        except Exception as e:
            typer.echo(f"  ✗ Failed to stream {chunk_name} to Delta table: {e}")
            update_stats(stats, {"failed": 1})

    # Verify final Delta table state
    if stats["processed"] > 0:
        try:
            dt = DeltaTable(str(delta_table_path))
            version = dt.version()
            file_count = len(dt.files())

            typer.echo("✓ Successfully created/updated partitioned Delta table")
            typer.echo(f"  Path: {delta_table_path}")
            typer.echo(f"  Version: {version}, Files: {file_count}")
            typer.echo("  Partitioned by: parcel_chunk")
            typer.echo(f"  Processed chunks: {stats['processed']}")

        except Exception as e:
            typer.echo(f"⚠ Warning: Could not verify final Delta table state: {e}")
    elif stats["skipped"] == 0:
        typer.echo("✗ No chunks were successfully processed")

    conn.close()

    typer.echo("\n=== PHASE 2 COMPLETE ===")
    typer.echo(f"✓ Total chunks: {stats['total_chunks']}")
    typer.echo(f"✓ Processed: {stats['processed']}")
    typer.echo(f"✓ Skipped (existing): {stats['skipped']}")
    typer.echo(f"✗ Failed: {stats['failed']}")

    return stats


def get_delta_conversion_progress(
    input_dir: str, delta_dir: str
) -> DeltaConversionProgress:
    """Check progress of Delta conversion by comparing parquet chunks to partitioned Delta table.

    Args:
        input_dir: Directory with reorganized parquet files
        delta_dir: Directory to check for existing partitioned Delta table

    Returns:
        Dictionary with progress info: {'total_chunks': int, 'existing_delta_tables': int, 'delta_tables': list}
    """
    input_path = Path(input_dir)
    delta_path = Path(delta_dir)

    if not input_path.exists():
        return DeltaConversionProgress(
            total_chunks=0, existing_delta_tables=0, delta_tables=[]
        )

    # Count total parcel chunks available for conversion
    chunk_dirs = [
        d
        for d in input_path.iterdir()
        if d.is_dir() and d.name.startswith("parcel_chunk=")
    ]
    total_chunks = len(chunk_dirs)

    if not delta_path.exists():
        return DeltaConversionProgress(
            total_chunks=total_chunks, existing_delta_tables=0, delta_tables=[]
        )

    # Check for the single partitioned Delta table
    delta_table_path = delta_path / "parcel_data"

    if not delta_table_path.exists():
        return DeltaConversionProgress(
            total_chunks=total_chunks, existing_delta_tables=0, delta_tables=[]
        )

    try:
        # Verify partitioned Delta table is valid
        dt = DeltaTable(str(delta_table_path))
        version = dt.version()
        file_count = len(dt.files())

        if file_count > 0:
            # Count existing partitions
            existing_partitions = set()
            for file_info in dt.files():
                if "parcel_chunk=" in file_info:
                    partition = file_info.split("parcel_chunk=")[1].split("/")[0]
                    existing_partitions.add(partition)

            # Create table info for the partitioned table
            delta_table_info = DeltaTableInfo(
                chunk_id="parcel_data (partitioned)",
                path=str(delta_table_path),
                version=version,
                file_count=file_count,
            )

            return DeltaConversionProgress(
                total_chunks=total_chunks,
                existing_delta_tables=len(existing_partitions),
                # Number of partitions, not tables
                delta_tables=[delta_table_info],
            )
        else:
            return DeltaConversionProgress(
                total_chunks=total_chunks, existing_delta_tables=0, delta_tables=[]
            )

    except Exception:
        # Directory exists but is not a valid Delta table
        return DeltaConversionProgress(
            total_chunks=total_chunks, existing_delta_tables=0, delta_tables=[]
        )


def optimize_delta_table(delta_table_path: str, dry_run: bool = False) -> bool:
    """Optimize a Delta table by compacting small files.

    Args:
        delta_table_path: Path to the Delta table to optimize
        dry_run: Show what would be done without making changes

    Returns:
        True if optimization was successful, False otherwise
    """
    try:
        dt = DeltaTable(str(delta_table_path))

        if dry_run:
            typer.echo(f"Would optimize Delta table: {delta_table_path}")
            typer.echo(f"  Current version: {dt.version()}")
            typer.echo(f"  Current files: {len(dt.files())}")
            return True

        # Perform optimization (compaction)
        dt.optimize.compact()

        # Get updated stats
        new_version = dt.version()
        new_file_count = len(dt.files())

        typer.echo(f"  ✓ Optimized {Path(delta_table_path).name}")
        typer.echo(f"    New version: {new_version}, Files: {new_file_count}")

        return True

    except Exception as e:
        typer.echo(f"  ✗ Failed to optimize {Path(delta_table_path).name}: {e}")
        return False


@monitor_performance()
def optimize_all_delta_tables(delta_dir: str, dry_run: bool = False) -> dict[str, int]:
    """Optimize the partitioned Delta table in the directory.

    Args:
        delta_dir: Directory containing the partitioned Delta Lake table
        dry_run: Show what would be done without making changes

    Returns:
        Dictionary with optimization statistics
    """
    typer.echo("=== OPTIMIZING PARTITIONED DELTA TABLE ===")
    typer.echo(f"Delta directory: {delta_dir}")

    if dry_run:
        typer.echo("DRY RUN MODE - No optimizations will be performed")

    delta_path = Path(delta_dir)
    if not delta_path.exists():
        typer.echo(f"Error: Delta directory {delta_dir} does not exist")
        return {"total_tables": 0, "optimized": 0, "failed": 0}

    # Check for the single partitioned Delta table
    delta_table_path = delta_path / "parcel_data"

    if not delta_table_path.exists():
        typer.echo(f"No partitioned Delta table found in {delta_dir}")
        return {"total_tables": 0, "optimized": 0, "failed": 0}

    stats = {"total_tables": 1, "optimized": 0, "failed": 0}

    if optimize_delta_table(str(delta_table_path), dry_run):
        stats["optimized"] = 1
    else:
        stats["failed"] = 1

    typer.echo("\n=== OPTIMIZATION COMPLETE ===")
    typer.echo(f"✓ Total tables: {stats['total_tables']}")
    typer.echo(f"✓ Optimized: {stats['optimized']}")
    typer.echo(f"✗ Failed: {stats['failed']}")

    return stats
