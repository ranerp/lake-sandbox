from pathlib import Path

import duckdb
import typer

from lake_sandbox.utils.performance import monitor_performance
from lake_sandbox.validator.models import ChunkFile, ReorganizationProgress


@monitor_performance()
def reorganize_by_parcel_chunks(
    input_dir: str,
    output_dir: str,
    chunk_size: int = 10_000,
    dry_run: bool = False,
    force: bool = False,
) -> dict[str, int]:
    """Phase 1: Reorganize raw timeseries data from date-partitioned to parcel-chunk-partitioned format.

    Args:
        input_dir: Input directory with raw parquet files partitioned by utm_tile/year/date
        output_dir: Output directory for reorganized parquet files partitioned by parcel_chunk
        chunk_size: Number of parcels per chunk
        dry_run: Show what would be done without making changes
        force: Force reprocessing of existing chunks

    Returns:
        Dictionary with statistics: {'total_chunks': int, 'created': int, 'skipped': int, 'failed': int}
    """

    typer.echo("=== PHASE 1: REORGANIZING BY PARCEL CHUNKS ===")
    typer.echo(f"Input directory: {input_dir}")
    typer.echo(f"Output directory: {output_dir}")
    typer.echo(f"Chunk size: {chunk_size:,} parcels per chunk")

    if dry_run:
        typer.echo("DRY RUN MODE - No files will be created")
    if force:
        typer.echo("FORCE MODE - Existing chunks will be overwritten")

    input_path = Path(input_dir)
    if not input_path.exists():
        typer.echo(f"Error: Input directory {input_dir} does not exist")
        raise typer.Exit(1)

    # Find all parquet files in the input directory
    parquet_files = list(input_path.rglob("*.parquet"))
    if not parquet_files:
        typer.echo(f"Error: No parquet files found in {input_dir}")
        raise typer.Exit(1)

    typer.echo(f"Found {len(parquet_files)} parquet files to process")

    if dry_run:
        typer.echo("\nFiles that would be processed:")
        for file in parquet_files[:5]:  # Show first 5
            typer.echo(f"  {file}")
        if len(parquet_files) > 5:
            typer.echo(f"  ... and {len(parquet_files) - 5} more files")
        return {"total_chunks": 0, "created": 0, "skipped": 0, "failed": 0}

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()

    # Calculate total number of chunks needed
    # First, get sample data to estimate parcels per file
    sample_file = str(parquet_files[0])
    sample_result = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{sample_file}')").fetchone()
    if sample_result is None:
        raise ValueError("Sample query returned no results")
    sample_count = sample_result[0]
    total_chunks = (sample_count + chunk_size - 1) // chunk_size  # Round up

    typer.echo(f"Sample file has {sample_count:,} parcels")
    typer.echo(f"Will create {total_chunks} parcel chunks")

    # Read all raw data pattern
    all_files_pattern = str(input_path / "**/*.parquet")
    typer.echo(f"Reading pattern: {all_files_pattern}")

    # Statistics tracking
    stats = {"total_chunks": total_chunks, "created": 0, "skipped": 0, "failed": 0}

    for chunk_id in range(total_chunks):
        typer.echo(f"Processing chunk {chunk_id:02d}/{total_chunks - 1:02d}")

        # Create chunk directory
        chunk_dir = output_path / f"parcel_chunk={chunk_id:02d}"
        chunk_dir.mkdir(parents=True, exist_ok=True)

        # Check if chunk already exists and is valid
        chunk_file = chunk_dir / "data.parquet"

        if chunk_file.exists() and not force:
            try:
                # Verify existing file is valid and has data
                existing_result = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{chunk_file}')").fetchone()
                if existing_result is None:
                    raise ValueError("Existing chunk query returned no results")
                existing_count = existing_result[0]
                if existing_count > 0:
                    typer.echo(
                        f"  ✓ Skipping existing chunk {chunk_id:02d} ({existing_count:,} rows)")
                    stats["skipped"] += 1
                    continue
                else:
                    typer.echo(
                        f"  ⚠ Existing chunk {chunk_id:02d} is empty, recreating...")
            except Exception as e:
                typer.echo(
                    f"  ⚠ Existing chunk {chunk_id:02d} is corrupted ({e}), recreating...")

        try:
            # Extract data for this specific parcel chunk
            conn.execute(f"""
                COPY (
                    SELECT
                        parcel_id,
                        date,
                        ndvi,
                        evi,
                        red,
                        nir,
                        blue,
                        green,
                        swir1,
                        swir2,
                        temperature,
                        precipitation,
                        cloud_cover,
                        geometry_area
                    FROM read_parquet('{all_files_pattern}')
                    WHERE HASH(parcel_id) % {total_chunks} = {chunk_id}
                    ORDER BY parcel_id, date
                ) TO '{chunk_file}' (FORMAT PARQUET)
            """)

            # Verify file was created and get row count
            if chunk_file.exists():
                row_result = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{chunk_file}')").fetchone()
                if row_result is None:
                    raise ValueError("Row count query returned no results")
                row_count = row_result[0]
                if row_count > 0:
                    typer.echo(f"  ✓ Created {chunk_file.name} with {row_count:,} rows")
                    stats["created"] += 1
                else:
                    typer.echo(f"  ⚠ Created {chunk_file.name} but it's empty")
                    stats["failed"] += 1
            else:
                typer.echo(f"  ✗ Failed to create {chunk_file.name}")
                stats["failed"] += 1

        except Exception as e:
            typer.echo(f"  ✗ Failed to process chunk {chunk_id:02d}: {e}")
            stats["failed"] += 1

    conn.close()

    typer.echo("\n=== PHASE 1 COMPLETE ===")
    typer.echo(f"✓ Total chunks: {stats['total_chunks']}")
    typer.echo(f"✓ Created: {stats['created']}")
    typer.echo(f"✓ Skipped (existing): {stats['skipped']}")
    typer.echo(f"✗ Failed: {stats['failed']}")

    return stats


def get_reorganization_progress(output_dir: str) -> ReorganizationProgress:
    """Check progress of reorganization by counting existing chunks.

    Args:
        output_dir: Directory to check for existing parcel chunks

    Returns:
        Dictionary with progress info: {'existing_chunks': int, 'chunk_files': list}
    """
    output_path = Path(output_dir)
    if not output_path.exists():
        return ReorganizationProgress(existing_chunks=0, chunk_files=[])

    chunk_dirs = [d for d in output_path.iterdir() if
                  d.is_dir() and d.name.startswith("parcel_chunk=")]
    valid_chunks: list[ChunkFile] = []

    conn = duckdb.connect()

    for chunk_dir in sorted(chunk_dirs):
        data_file = chunk_dir / "data.parquet"
        if data_file.exists():
            try:
                # Verify file is valid
                count_result = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{data_file}')").fetchone()
                if count_result is None:
                    continue
                count = count_result[0]
                if count > 0:
                    valid_chunks.append(ChunkFile(
                        chunk_id=chunk_dir.name,
                        file_path=str(data_file),
                        row_count=count
                    ))
            except Exception:
                # File exists but is corrupted
                pass

    conn.close()

    return ReorganizationProgress(
        existing_chunks=len(valid_chunks),
        chunk_files=valid_chunks
    )
