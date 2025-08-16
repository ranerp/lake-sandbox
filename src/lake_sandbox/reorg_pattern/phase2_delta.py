from pathlib import Path

import duckdb
import typer
from deltalake import DeltaTable, write_deltalake

from lake_sandbox.utils.performance import monitor_performance
from lake_sandbox.validator.models import DeltaConversionProgress, DeltaTableInfo


@monitor_performance()
def convert_to_delta_lake(
    input_dir: str,
    delta_dir: str,
    dry_run: bool = False,
    force: bool = False,
) -> dict[str, int]:
    """Phase 2: Convert reorganized parquet chunks to Delta Lake format.

    Args:
        input_dir: Directory with reorganized parquet files partitioned by parcel_chunk
        delta_dir: Output directory for Delta Lake tables
        dry_run: Show what would be done without making changes
        force: Force reprocessing of existing Delta tables

    Returns:
        Dictionary with statistics: {'total_chunks': int, 'created': int, 'skipped': int, 'failed': int}
    """

    typer.echo("=== PHASE 2: CONVERTING TO DELTA LAKE ===")
    typer.echo(f"Input directory: {input_dir}")
    typer.echo(f"Delta directory: {delta_dir}")

    if dry_run:
        typer.echo("DRY RUN MODE - No Delta tables will be created")
    if force:
        typer.echo("FORCE MODE - Existing Delta tables will be overwritten")

    input_path = Path(input_dir)
    if not input_path.exists():
        typer.echo(f"Error: Input directory {input_dir} does not exist")
        raise typer.Exit(1)

    # Find all parcel chunk directories
    chunk_dirs = [d for d in input_path.iterdir() if
                  d.is_dir() and d.name.startswith("parcel_chunk=")]
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
        return {"total_chunks": len(chunk_dirs), "created": 0, "skipped": 0,
                "failed": 0}

    # Create Delta directory
    delta_path = Path(delta_dir)
    delta_path.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()

    # Statistics tracking
    stats = {"total_chunks": len(chunk_dirs), "created": 0, "skipped": 0, "failed": 0}

    for chunk_dir in sorted(chunk_dirs):
        chunk_name = chunk_dir.name
        typer.echo(f"Converting {chunk_name} to Delta format")

        data_file = chunk_dir / "data.parquet"
        if not data_file.exists():
            typer.echo(f"  ✗ Skipping {chunk_name} - no data.parquet found")
            stats["failed"] += 1
            continue

        # Check if Delta table already exists and is valid
        delta_table_path = delta_path / chunk_name

        if delta_table_path.exists() and not force:
            try:
                # Verify existing Delta table is valid
                dt = DeltaTable(str(delta_table_path))
                version = dt.version()
                file_count = len(dt.files())

                if file_count > 0:
                    typer.echo(
                        f"  ✓ Skipping existing Delta table {chunk_name} (version {version}, {file_count} files)")
                    stats["skipped"] += 1
                    continue
                else:
                    typer.echo(
                        f"  ⚠ Existing Delta table {chunk_name} has no files, recreating...")
            except Exception as e:
                typer.echo(
                    f"  ⚠ Existing Delta table {chunk_name} is corrupted ({e}), recreating...")

        try:
            # Read and deduplicate the parquet data
            # Remove duplicates by keeping first occurrence of each parcel_id + date combination
            df = conn.execute(f"""
                SELECT DISTINCT ON (parcel_id, date) *
                FROM read_parquet('{data_file}')
                ORDER BY parcel_id, date
            """).fetchdf()

            if df.empty:
                typer.echo(f"  ✗ Skipping {chunk_name} - empty dataset")
                stats["failed"] += 1
                continue

            # Create or overwrite Delta table for this chunk
            # Store all dates together for each parcel chunk (no partitioning by date)
            write_deltalake(
                str(delta_table_path),
                df,
                mode="overwrite"
                # No partition_by - store all dates together for optimal file size
            )

            # Verify Delta table was created successfully
            dt = DeltaTable(str(delta_table_path))
            version = dt.version()
            file_count = len(dt.files())

            typer.echo(f"  ✓ Created Delta table at {delta_table_path.name}")
            typer.echo(
                f"    Version: {version}, Files: {file_count}, Rows: {len(df):,}")

            stats["created"] += 1

        except Exception as e:
            typer.echo(f"  ✗ Failed to create Delta table for {chunk_name}: {e}")
            stats["failed"] += 1

    conn.close()

    typer.echo("\n=== PHASE 2 COMPLETE ===")
    typer.echo(f"✓ Total chunks: {stats['total_chunks']}")
    typer.echo(f"✓ Created: {stats['created']}")
    typer.echo(f"✓ Skipped (existing): {stats['skipped']}")
    typer.echo(f"✗ Failed: {stats['failed']}")

    return stats


def get_delta_conversion_progress(input_dir: str, delta_dir: str) -> DeltaConversionProgress:
    """Check progress of Delta conversion by comparing parquet chunks to Delta tables.

    Args:
        input_dir: Directory with reorganized parquet files
        delta_dir: Directory to check for existing Delta tables

    Returns:
        Dictionary with progress info: {'total_chunks': int, 'existing_delta_tables': int, 'delta_tables': list}
    """
    input_path = Path(input_dir)
    delta_path = Path(delta_dir)

    if not input_path.exists():
        return DeltaConversionProgress(total_chunks=0, existing_delta_tables=0, delta_tables=[])

    # Count total parcel chunks available for conversion
    chunk_dirs = [d for d in input_path.iterdir() if
                  d.is_dir() and d.name.startswith("parcel_chunk=")]
    total_chunks = len(chunk_dirs)

    if not delta_path.exists():
        return DeltaConversionProgress(total_chunks=total_chunks, existing_delta_tables=0, delta_tables=[])

    # Count existing valid Delta tables
    delta_dirs = [d for d in delta_path.iterdir() if
                  d.is_dir() and d.name.startswith("parcel_chunk=")]
    valid_delta_tables: list[DeltaTableInfo] = []

    for delta_table_dir in sorted(delta_dirs):
        try:
            # Verify Delta table is valid
            dt = DeltaTable(str(delta_table_dir))
            version = dt.version()
            file_count = len(dt.files())

            if file_count > 0:
                valid_delta_tables.append(DeltaTableInfo(
                    chunk_id=delta_table_dir.name,
                    path=str(delta_table_dir),
                    version=version,
                    file_count=file_count
                ))
        except Exception:
            # Directory exists but is not a valid Delta table
            pass

    return DeltaConversionProgress(
        total_chunks=total_chunks,
        existing_delta_tables=len(valid_delta_tables),
        delta_tables=valid_delta_tables
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
    """Optimize all Delta tables in the directory.

    Args:
        delta_dir: Directory containing Delta Lake tables
        dry_run: Show what would be done without making changes

    Returns:
        Dictionary with optimization statistics
    """
    typer.echo("=== OPTIMIZING DELTA TABLES ===")
    typer.echo(f"Delta directory: {delta_dir}")

    if dry_run:
        typer.echo("DRY RUN MODE - No optimizations will be performed")

    delta_path = Path(delta_dir)
    if not delta_path.exists():
        typer.echo(f"Error: Delta directory {delta_dir} does not exist")
        return {"total_tables": 0, "optimized": 0, "failed": 0}

    delta_dirs = [d for d in delta_path.iterdir() if
                  d.is_dir() and d.name.startswith("parcel_chunk=")]

    if not delta_dirs:
        typer.echo(f"No Delta tables found in {delta_dir}")
        return {"total_tables": 0, "optimized": 0, "failed": 0}

    stats = {"total_tables": len(delta_dirs), "optimized": 0, "failed": 0}

    for delta_table_dir in sorted(delta_dirs):
        if optimize_delta_table(str(delta_table_dir), dry_run):
            stats["optimized"] += 1
        else:
            stats["failed"] += 1

    typer.echo("\n=== OPTIMIZATION COMPLETE ===")
    typer.echo(f"✓ Total tables: {stats['total_tables']}")
    typer.echo(f"✓ Optimized: {stats['optimized']}")
    typer.echo(f"✗ Failed: {stats['failed']}")

    return stats
