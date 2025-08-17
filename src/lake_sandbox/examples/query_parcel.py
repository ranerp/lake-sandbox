"""Example: Query and display a single parcel timeseries from Delta Lake data."""

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from lake_sandbox.utils.performance import monitor_performance


def connect_to_deltalake(delta_dir: str = "./output/timeseries-delta") -> duckdb.DuckDBPyConnection:
    """Connect to DeltaLake data using DuckDB."""
    conn = duckdb.connect()

    # Install and load delta extension
    conn.execute("INSTALL delta")
    conn.execute("LOAD delta")

    return conn


@monitor_performance()
def get_dataset_summary(conn: duckdb.DuckDBPyConnection, delta_dir: str) -> dict:
    """Get overall dataset summary statistics."""
    delta_path = Path(delta_dir).absolute()
    delta_table_path = delta_path / "parcel_data"

    if not delta_table_path.exists():
        raise FileNotFoundError(f"Delta table not found at: {delta_table_path}")

    query = f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT parcel_id) as total_parcels,
        COUNT(DISTINCT date) as total_dates,
        COUNT(DISTINCT parcel_chunk) as total_partitions,
        MIN(date) as min_date,
        MAX(date) as max_date,
        AVG(ndvi) as avg_ndvi,
        AVG(evi) as avg_evi,
        AVG(temperature) as avg_temperature,
        AVG(precipitation) as avg_precipitation
    FROM delta_scan('{delta_table_path}')
    """

    result = conn.execute(query).fetchone()

    return {
        "total_records": result[0],
        "total_parcels": result[1],
        "total_dates": result[2],
        "total_partitions": result[3],
        "min_date": result[4],
        "max_date": result[5],
        "avg_ndvi": result[6],
        "avg_evi": result[7],
        "avg_temperature": result[8],
        "avg_precipitation": result[9]
    }


@monitor_performance()
def get_random_parcel_timeseries(conn: duckdb.DuckDBPyConnection,
                                delta_dir: str,
                                parcel_id: Optional[str] = None) -> pd.DataFrame:
    """Get timeseries data for a specific or random parcel."""
    delta_path = Path(delta_dir).absolute()
    delta_table_path = delta_path / "parcel_data"

    if parcel_id:
        # Query specific parcel
        query = f"""
        SELECT
            parcel_id,
            date,
            parcel_chunk,
            ndvi,
            evi,
            temperature,
            precipitation,
            cloud_cover,
            geometry_area
        FROM delta_scan('{delta_table_path}')
        WHERE parcel_id = '{parcel_id}'
        ORDER BY date
        """
    else:
        # Get a random parcel
        query = f"""
        WITH random_parcel AS (
            SELECT parcel_id
            FROM delta_scan('{delta_table_path}')
            GROUP BY parcel_id
            ORDER BY RANDOM()
            LIMIT 1
        )
        SELECT
            p.parcel_id,
            p.date,
            p.parcel_chunk,
            p.ndvi,
            p.evi,
            p.temperature,
            p.precipitation,
            p.cloud_cover,
            p.geometry_area
        FROM delta_scan('{delta_table_path}') p
        INNER JOIN random_parcel r ON p.parcel_id = r.parcel_id
        ORDER BY p.date
        """

    df = conn.execute(query).df()

    if df.empty:
        if parcel_id:
            raise ValueError(f"No data found for parcel ID: {parcel_id}")
        else:
            raise ValueError("No data found in Delta table")

    return df


def display_dataset_summary(summary: dict, console: Console):
    """Display dataset summary in a formatted table."""
    table = Table(title="Dataset Summary", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan", width=20)
    table.add_column("Value", style="green", width=20)

    table.add_row("Total Records", f"{summary['total_records']:,}")
    table.add_row("Total Parcels", f"{summary['total_parcels']:,}")
    table.add_row("Total Dates", f"{summary['total_dates']}")
    table.add_row("Total Partitions", f"{summary['total_partitions']}")
    table.add_row("Date Range", f"{summary['min_date']} to {summary['max_date']}")
    table.add_row("Avg NDVI", f"{summary['avg_ndvi']:.3f}")
    table.add_row("Avg EVI", f"{summary['avg_evi']:.3f}")
    table.add_row("Avg Temperature", f"{summary['avg_temperature']:.1f}°C")
    table.add_row("Avg Precipitation", f"{summary['avg_precipitation']:.1f}mm")

    console.print(table)


def display_parcel_timeseries(df: pd.DataFrame, console: Console):
    """Display parcel timeseries data in a formatted table."""
    parcel_id = df['parcel_id'].iloc[0]
    parcel_chunk = df['parcel_chunk'].iloc[0]

    # Parcel info
    info_text = f"Parcel ID: {parcel_id}\nPartition: {parcel_chunk}\nObservations: {len(df)}"
    console.print(Panel(info_text, title="Parcel Information", style="blue"))

    # Timeseries table
    table = Table(title=f"Timeseries Data for Parcel {parcel_id}", show_header=True, header_style="bold green")
    table.add_column("Date", style="cyan", width=12)
    table.add_column("NDVI", style="green", width=8)
    table.add_column("EVI", style="blue", width=8)
    table.add_column("Temp (°C)", style="red", width=10)
    table.add_column("Precip (mm)", style="blue", width=12)
    table.add_column("Cloud (%)", style="white", width=10)
    table.add_column("Area (ha)", style="yellow", width=10)

    for _, row in df.iterrows():
        table.add_row(
            str(row['date'].date()) if pd.notna(row['date']) else "N/A",
            f"{row['ndvi']:.3f}",
            f"{row['evi']:.3f}",
            f"{row['temperature']:.1f}",
            f"{row['precipitation']:.1f}",
            f"{row['cloud_cover']:.1f}",
            f"{row['geometry_area']:.1f}"
        )

    console.print(table)

    # Summary statistics for this parcel
    stats_table = Table(title="Parcel Statistics", show_header=True, header_style="bold yellow")
    stats_table.add_column("Metric", style="cyan")
    stats_table.add_column("Value", style="green")

    stats_table.add_row("NDVI Range", f"{df['ndvi'].min():.3f} - {df['ndvi'].max():.3f}")
    stats_table.add_row("NDVI Mean", f"{df['ndvi'].mean():.3f}")
    stats_table.add_row("EVI Range", f"{df['evi'].min():.3f} - {df['evi'].max():.3f}")
    stats_table.add_row("EVI Mean", f"{df['evi'].mean():.3f}")
    stats_table.add_row("Temp Range", f"{df['temperature'].min():.1f}°C - {df['temperature'].max():.1f}°C")
    stats_table.add_row("Total Precipitation", f"{df['precipitation'].sum():.1f}mm")
    stats_table.add_row("Avg Cloud Cover", f"{df['cloud_cover'].mean():.1f}%")

    console.print(stats_table)


def query_parcel(
    delta_dir: str = typer.Option(
        "output/timeseries-delta", "--delta-dir",
        help="Path to Delta Lake data directory"
    ),
    parcel_id: Optional[str] = typer.Option(
        None, "--parcel-id",
        help="Specific parcel ID to query (if not provided, random parcel will be selected)"
    ),
    show_summary: bool = typer.Option(
        True, "--show-summary/--no-summary",
        help="Show dataset summary statistics"
    )
) -> None:
    """Query and display a single parcel timeseries from Delta Lake data.

    This command connects to the Delta Lake table and:
    1. Shows overall dataset statistics
    2. Queries a specific parcel (or random if not specified)
    3. Displays the complete timeseries for that parcel
    4. Shows parcel-specific statistics

    Examples:
        # Show random parcel
        uv run lake-sandbox query-parcel

        # Show specific parcel
        uv run lake-sandbox query-parcel --parcel-id parcel_123456

        # Skip dataset summary
        uv run lake-sandbox query-parcel --no-summary
    """
    console = Console()

    console.print(Panel("Querying Delta Lake Parcel Data", style="bold blue"))

    try:
        # Connect to Delta Lake
        console.print(f"Connecting to Delta Lake at: {delta_dir}")
        conn = connect_to_deltalake(delta_dir)

        # Show dataset summary if requested
        if show_summary:
            console.print("\n" + "="*50)
            summary = get_dataset_summary(conn, delta_dir)
            display_dataset_summary(summary, console)

        # Query parcel timeseries
        console.print("\n" + "="*50)
        if parcel_id:
            console.print(f"Querying specific parcel: {parcel_id}")
        else:
            console.print("Selecting random parcel...")

        df = get_random_parcel_timeseries(conn, delta_dir, parcel_id)
        display_parcel_timeseries(df, console)

        # Close connection
        conn.close()

        console.print("\nQuery completed successfully!")

    except Exception as e:
        console.print(f"Error during query: {e}", style="bold red")
        raise typer.Exit(1)


if __name__ == "__main__":
    typer.run(query_parcel)
