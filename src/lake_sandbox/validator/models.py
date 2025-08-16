from dataclasses import dataclass
from typing import Optional


@dataclass
class FileDetail:
    """Details about a single parquet file in raw timeseries validation."""
    file_path: str
    utm_tile: str
    year: str
    date_partition: str
    date_value: str
    total_records: int
    unique_parcels: int
    date_range: str
    data_consistent: bool


@dataclass
class ParcelDistribution:
    """Statistics about parcel distribution across files."""
    min_parcels_per_file: int
    max_parcels_per_file: int
    avg_parcels_per_file: int
    consistent_across_files: bool


@dataclass
class RawValidationResult:
    """Result of raw timeseries validation."""
    valid: bool
    total_files: int
    file_details: list[FileDetail]
    total_unique_parcels: int
    total_records: int
    issues: list[str]
    parcel_distribution: Optional[ParcelDistribution] = None
    error: Optional[str] = None


@dataclass
class ChunkDetail:
    """Details about a single parcel chunk in organized validation."""
    chunk_name: str
    total_records: int
    unique_parcels: int
    unique_dates: int
    date_range: str
    duplicate_records: int
    sample_parcels: list[str]


@dataclass
class OrganizedValidationResult:
    """Result of organized chunks validation."""
    valid: bool
    total_chunks: int
    chunk_details: list[ChunkDetail]
    total_unique_parcels: int
    total_records: int
    issues: list[str]
    error: Optional[str] = None


@dataclass
class TableDetail:
    """Details about a single Delta table in delta validation."""
    table_name: str
    version: int
    file_count: int
    total_records: int
    unique_parcels: int
    unique_dates: int
    date_range: str
    missing_combinations: int
    completeness_pct: float


@dataclass
class DeltaValidationResult:
    """Result of Delta tables validation."""
    valid: bool
    total_tables: int
    table_details: list[TableDetail]
    total_unique_parcels: int
    total_records: int
    issues: list[str]
    error: Optional[str] = None