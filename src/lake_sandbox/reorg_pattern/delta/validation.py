"""Delta Lake specific validation utilities."""

from pathlib import Path

import typer
from deltalake import DeltaTable


def validate_delta_table(
    delta_table_path: Path,
) -> tuple[bool, DeltaTable | None, str | None]:
    """Validate a Delta Lake table exists and is accessible.

    Args:
        delta_table_path: Path to the Delta table

    Returns:
        Tuple of (is_valid, delta_table, error_message)
        - is_valid: True if table exists and is valid
        - delta_table: DeltaTable object if valid, None if invalid
        - error_message: Error description if validation failed, None if success
    """
    if not delta_table_path.exists():
        return False, None, "Delta table directory does not exist"

    try:
        dt = DeltaTable(str(delta_table_path))
        file_count = len(dt.files())

        if file_count == 0:
            return False, None, "Delta table has no data files"

        return True, dt, None

    except Exception as e:
        return False, None, f"Delta table is corrupted: {e}"


def get_delta_partitions(dt: DeltaTable) -> set[str]:
    """Extract partition names from Delta table files.

    Args:
        dt: DeltaTable object

    Returns:
        Set of partition identifiers
    """
    partitions = set()
    for file_info in dt.files():
        if "parcel_chunk=" in file_info:
            partition = file_info.split("parcel_chunk=")[1].split("/")[0]
            partitions.add(partition)
    return partitions


def verify_delta_streaming(
    chunk_name: str, records_streamed: int, delta_table_path: Path
) -> dict[str, int]:
    """Verify that streaming to Delta table was successful.

    Args:
        chunk_name: Name of the chunk that was streamed
        records_streamed: Number of records that should have been streamed
        delta_table_path: Path to the Delta table

    Returns:
        Dictionary with stats updates to apply
    """
    is_valid, dt, error = validate_delta_table(delta_table_path)

    if not is_valid:
        typer.echo(f"  ✗ Failed to verify {chunk_name} - {error}")
        return {"failed": 1}

    if records_streamed > 0:
        typer.echo(
            f"  ✓ Streamed {records_streamed:,} rows from {chunk_name} to Delta table"
        )
        return {"processed": 1}
    else:
        typer.echo(f"  ⚠ Streamed {chunk_name} but no rows were added")
        return {"failed": 1}
