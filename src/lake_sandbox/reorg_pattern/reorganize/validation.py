"""Functional utilities for file validation and verification in reorganization processes."""

from pathlib import Path

import duckdb
import typer


def validate_parquet_file(
    file_path: Path, conn: duckdb.DuckDBPyConnection
) -> tuple[bool, int, str | None]:
    """Validate a parquet file exists and contains data.

    Args:
        file_path: Path to the parquet file to validate
        conn: DuckDB connection to use for validation

    Returns:
        Tuple of (is_valid, row_count, error_message)
        - is_valid: True if file exists and has data
        - row_count: Number of rows in the file (0 if invalid)
        - error_message: Error description if validation failed, None if success
    """
    if not file_path.exists():
        return False, 0, "File does not exist"

    try:
        result = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{file_path}')"
        ).fetchone()
        if result is None:
            return False, 0, "Query returned no results"

        row_count = result[0]
        if row_count > 0:
            return True, row_count, None
        else:
            return False, 0, "File is empty"

    except Exception as e:
        return False, 0, str(e)


def check_existing_chunk(
    chunk_file: Path,
    chunk_id: str,
    conn: duckdb.DuckDBPyConnection,
    force: bool = False,
) -> tuple[bool, dict[str, int]]:
    """Check if an existing chunk should be skipped or recreated.

    Args:
        chunk_file: Path to the chunk file to check
        chunk_id: Identifier for the chunk (for logging)
        conn: DuckDB connection for validation
        force: Whether to force recreation regardless of existing state

    Returns:
        Tuple of (should_skip, stats_update)
        - should_skip: True if chunk should be skipped, False if it should be processed
        - stats_update: Dictionary with stats updates to apply
    """
    if not chunk_file.exists() or force:
        return False, {}

    is_valid, row_count, error = validate_parquet_file(chunk_file, conn)

    if is_valid:
        typer.echo(f"  ✓ Skipping existing chunk {chunk_id} ({row_count:,} rows)")
        return True, {"skipped": 1}

    if error == "File does not exist":
        return False, {}
    elif error == "File is empty":
        typer.echo(f"  ⚠ Existing chunk {chunk_id} is empty, recreating...")
    else:
        typer.echo(
            f"  ⚠ Existing chunk {chunk_id} is corrupted ({error}), recreating..."
        )

    return False, {}


def verify_file_creation(
    chunk_file: Path, chunk_id: str, conn: duckdb.DuckDBPyConnection
) -> dict[str, int]:
    """Verify that a file was created successfully and return statistics update.

    Args:
        chunk_file: Path to the chunk file that should have been created
        chunk_id: Identifier for the chunk (for logging)
        conn: DuckDB connection for validation

    Returns:
        Dictionary with stats updates to apply
    """
    is_valid, row_count, error = validate_parquet_file(chunk_file, conn)

    if not chunk_file.exists():
        typer.echo(f"  ✗ Failed to create {chunk_file.name}")
        return {"failed": 1}
    elif is_valid:
        typer.echo(f"  ✓ Created {chunk_file.name} with {row_count:,} rows")
        return {"created": 1}
    else:
        typer.echo(f"  ⚠ Created {chunk_file.name} but it's {error}")
        return {"failed": 1}


def update_stats(stats: dict[str, int], updates: dict[str, int]) -> None:
    """Update statistics dictionary with new values.

    Args:
        stats: Main statistics dictionary to update
        updates: Dictionary of updates to apply
    """
    for key, value in updates.items():
        stats[key] = stats.get(key, 0) + value


def get_valid_chunks(output_dir: str, conn: duckdb.DuckDBPyConnection) -> list:
    """Get list of valid chunk files in the output directory.

    Args:
        output_dir: Directory to scan for chunk files
        conn: DuckDB connection for validation

    Returns:
        List of dictionaries with chunk information
    """
    output_path = Path(output_dir)
    if not output_path.exists():
        return []

    chunk_dirs = [
        d
        for d in output_path.iterdir()
        if d.is_dir() and d.name.startswith("parcel_chunk=")
    ]
    valid_chunks = []

    for chunk_dir in sorted(chunk_dirs):
        data_file = chunk_dir / "data.parquet"
        is_valid, row_count, _ = validate_parquet_file(data_file, conn)

        if is_valid:
            valid_chunks.append(
                {
                    "chunk_id": chunk_dir.name,
                    "file_path": str(data_file),
                    "row_count": row_count,
                }
            )

    return valid_chunks
