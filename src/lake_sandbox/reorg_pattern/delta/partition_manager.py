"""Delta Lake partition management utilities."""

from dataclasses import dataclass
from pathlib import Path

import typer
from deltalake import DeltaTable

from lake_sandbox.reorg_pattern.delta.validation import validate_delta_table


def extract_partition_id(chunk_name: str) -> str:
    """Extract partition ID from chunk name.

    Args:
        chunk_name: Chunk name in format 'parcel_chunk=XX'

    Returns:
        Partition ID (e.g., '00', '01', etc.)
    """
    if "=" not in chunk_name:
        raise ValueError(f"Invalid chunk name format: {chunk_name}")
    return chunk_name.split("=")[1]


def format_partition_name(partition_id: str) -> str:
    """Format partition ID into full partition name.

    Args:
        partition_id: Partition ID (e.g., '00', '01')

    Returns:
        Full partition name (e.g., 'parcel_chunk=00')
    """
    return f"parcel_chunk={partition_id}"


def get_chunk_directory_name(chunk_id: int) -> str:
    """Get standardized chunk directory name.
    
    Args:
        chunk_id: Numeric chunk ID
        
    Returns:
        Formatted directory name (e.g., 'parcel_chunk=00')
    """
    return f"parcel_chunk={chunk_id:02d}"


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


@dataclass(frozen=True)
class DeltaTableState:
    """Immutable state representing Delta table and partitions."""

    exists: bool
    delta_table: DeltaTable | None
    existing_partitions: set[str]
    version: int | None = None
    file_count: int | None = None

    @classmethod
    def from_path(cls, delta_table_path: Path) -> "DeltaTableState":
        """Create DeltaTableState from path by validating and loading partitions.

        Args:
            delta_table_path: Path to Delta table

        Returns:
            Immutable DeltaTableState
        """
        is_valid, dt, error = validate_delta_table(delta_table_path)

        if not is_valid or dt is None:
            return cls(
                exists=False,
                delta_table=None,
                existing_partitions=set(),
            )

        partitions = get_delta_partitions(dt)
        version = dt.version()
        file_count = len(dt.files())

        return cls(
            exists=True,
            delta_table=dt,
            existing_partitions=partitions,
            version=version,
            file_count=file_count,
        )


def check_skip_partition(
    table_state: DeltaTableState, chunk_name: str, force: bool = False
) -> tuple[bool, dict[str, int]]:
    """Check if partition should be skipped during processing.

    Args:
        table_state: Current Delta table state
        chunk_name: Chunk name in format 'parcel_chunk=XX'
        force: Whether to force recreation regardless of existing state

    Returns:
        Tuple of (should_skip, stats_update)
    """
    if force or not table_state.exists:
        return False, {}

    partition_id = extract_partition_id(chunk_name)

    if partition_id in table_state.existing_partitions:
        partition_name = format_partition_name(partition_id)
        typer.echo(f"  ✓ Skipping {partition_name} - partition already exists")
        return True, {"skipped": 1}

    return False, {}


def get_table_info(table_state: DeltaTableState) -> tuple[int, int] | None:
    """Get Delta table version and file count from state.

    Args:
        table_state: Delta table state

    Returns:
        Tuple of (version, file_count) or None if table doesn't exist
    """
    if (
        not table_state.exists
        or table_state.version is None
        or table_state.file_count is None
    ):
        return None
    return table_state.version, table_state.file_count
