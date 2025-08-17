"""Example: Query random parcels within a random time window from Delta Lake data."""

from pathlib import Path

import duckdb
import pandas as pd
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from lake_sandbox.utils.performance import monitor_performance


def connect_to_deltalake() -> duckdb.DuckDBPyConnection:
    """Connect to DeltaLake data using DuckDB."""
    conn = duckdb.connect()

    # Install and load delta extension
    conn.execute("INSTALL delta")
    conn.execute("LOAD delta")

    return conn


@monitor_performance()
def get_random_time_window(
    conn: duckdb.DuckDBPyConnection, delta_dir: str, window_days: int = 10
) -> tuple[str, str]:
    """Get a random time window of specified days from the dataset."""
    delta_path = Path(delta_dir).absolute()
    delta_table_path = delta_path / "parcel_data"

    if not delta_table_path.exists():
        raise FileNotFoundError(f"Delta table not found at: {delta_table_path}")

    # Get all available dates and select random consecutive window
    query = f"""
    WITH date_list AS (
        SELECT DISTINCT date
        FROM delta_scan('{delta_table_path}')
        ORDER BY date
    ),
    numbered_dates AS (
        SELECT date, ROW_NUMBER() OVER (ORDER BY date) - 1 as row_num
        FROM date_list
    ),
    max_start AS (
        SELECT MAX(row_num) - {window_days - 1} as max_start_row
        FROM numbered_dates
    ),
    random_start AS (
        SELECT FLOOR(RANDOM() * (max_start_row + 1)) as start_row
        FROM max_start
    )
    SELECT
        MIN(nd.date) as start_date,
        MAX(nd.date) as end_date
    FROM numbered_dates nd
    CROSS JOIN random_start rs
    WHERE nd.row_num BETWEEN rs.start_row AND rs.start_row + {window_days - 1}
    """

    result = conn.execute(query).fetchone()
    if result is None:
        raise ValueError("Failed to query time window")
    start_date, end_date = result

    if not start_date or not end_date:
        raise ValueError(
            f"Could not select a {window_days}-day window from available data"
        )

    return str(start_date), str(end_date)


@monitor_performance()
def get_random_parcels_in_window(
    conn: duckdb.DuckDBPyConnection,
    delta_dir: str,
    start_date: str,
    end_date: str,
    num_parcels: int = 10,
) -> pd.DataFrame:
    """Get random parcels within the specified time window."""
    delta_path = Path(delta_dir).absolute()
    delta_table_path = delta_path / "parcel_data"

    query = f"""
    WITH available_parcels AS (
        SELECT DISTINCT parcel_id
        FROM delta_scan('{delta_table_path}')
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ),
    random_parcels AS (
        SELECT parcel_id
        FROM available_parcels
        ORDER BY RANDOM()
        LIMIT {num_parcels}
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
    INNER JOIN random_parcels r ON p.parcel_id = r.parcel_id
    WHERE p.date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY p.parcel_id, p.date
    """

    df = conn.execute(query).df()

    if df.empty:
        raise ValueError(
            f"No data found for the time window {start_date} to {end_date}"
        )

    return df


def get_sample_statistics(df: pd.DataFrame) -> dict:
    """Calculate statistics for the sample data."""
    return {
        "total_observations": len(df),
        "unique_parcels": df["parcel_id"].nunique(),
        "unique_dates": df["date"].nunique(),
        "unique_partitions": df["parcel_chunk"].nunique(),
        "avg_ndvi": df["ndvi"].mean(),
        "avg_evi": df["evi"].mean(),
        "avg_temperature": df["temperature"].mean(),
        "avg_precipitation": df["precipitation"].mean(),
        "avg_cloud_cover": df["cloud_cover"].mean(),
        "ndvi_range": (df["ndvi"].min(), df["ndvi"].max()),
        "temp_range": (df["temperature"].min(), df["temperature"].max()),
        "total_precipitation": df["precipitation"].sum(),
    }


def display_sample_info(
    start_date: str, end_date: str, num_parcels: int, window_days: int, console: Console
):
    """Display sample selection information."""
    info_text = Text()
    info_text.append("Time Window: ", style="bold cyan")
    info_text.append(f"{start_date} to {end_date}\n", style="green")
    info_text.append("Window Size: ", style="bold cyan")
    info_text.append(f"{window_days} days\n", style="green")
    info_text.append("Parcels: ", style="bold cyan")
    info_text.append(f"{num_parcels} random parcels", style="green")

    console.print(Panel(info_text, title="Sample Selection", style="blue"))


def display_sample_statistics(stats: dict, console: Console):
    """Display sample statistics in a formatted table."""
    table = Table(
        title="Sample Statistics", show_header=True, header_style="bold magenta"
    )
    table.add_column("Metric", style="cyan", width=25)
    table.add_column("Value", style="green", width=20)

    table.add_row("Total Observations", f"{stats['total_observations']:,}")
    table.add_row("Unique Parcels", f"{stats['unique_parcels']}")
    table.add_row("Unique Dates", f"{stats['unique_dates']}")
    table.add_row("Unique Partitions", f"{stats['unique_partitions']}")
    table.add_row("Average NDVI", f"{stats['avg_ndvi']:.3f}")
    table.add_row("Average EVI", f"{stats['avg_evi']:.3f}")
    table.add_row("Average Temperature", f"{stats['avg_temperature']:.1f}°C")
    table.add_row("Average Precipitation", f"{stats['avg_precipitation']:.1f}mm")
    table.add_row("Average Cloud Cover", f"{stats['avg_cloud_cover']:.1f}%")
    table.add_row(
        "NDVI Range", f"{stats['ndvi_range'][0]:.3f} - {stats['ndvi_range'][1]:.3f}"
    )
    table.add_row(
        "Temperature Range",
        f"{stats['temp_range'][0]:.1f}°C - {stats['temp_range'][1]:.1f}°C",
    )
    table.add_row("Total Precipitation", f"{stats['total_precipitation']:.1f}mm")

    console.print(table)


def display_sample_data(df: pd.DataFrame, console: Console):
    """Display the sample data in a formatted table."""
    table = Table(title="Sample Data", show_header=True, header_style="bold green")
    table.add_column("Parcel ID", style="yellow", width=15)
    table.add_column("Date", style="cyan", width=12)
    table.add_column("Partition", style="magenta", width=10)
    table.add_column("NDVI", style="green", width=8)
    table.add_column("EVI", style="blue", width=8)
    table.add_column("Temp", style="red", width=8)
    table.add_column("Precip", style="blue", width=10)
    table.add_column("Cloud", style="white", width=8)
    table.add_column("Area", style="yellow", width=8)

    # Limit display to reasonable number of rows
    display_df = df.head(50) if len(df) > 50 else df

    for _, row in display_df.iterrows():
        table.add_row(
            row["parcel_id"],
            str(row["date"].date()) if pd.notna(row["date"]) else "N/A",
            row["parcel_chunk"],
            f"{row['ndvi']:.3f}",
            f"{row['evi']:.3f}",
            f"{row['temperature']:.1f}",
            f"{row['precipitation']:.1f}",
            f"{row['cloud_cover']:.1f}",
            f"{row['geometry_area']:.1f}",
        )

    if len(df) > 50:
        console.print(
            f"\n[dim]Note: Showing first 50 rows of {len(df)} total observations[/dim]"
        )

    console.print(table)


def display_parcel_summary(df: pd.DataFrame, console: Console):
    """Display per-parcel summary statistics."""
    parcel_stats = (
        df.groupby("parcel_id")
        .agg(
            {
                "date": "count",
                "ndvi": ["mean", "min", "max"],
                "evi": "mean",
                "temperature": "mean",
                "precipitation": "sum",
            }
        )
        .round(3)
    )

    # Flatten column names
    parcel_stats.columns = [
        "observations",
        "ndvi_mean",
        "ndvi_min",
        "ndvi_max",
        "evi_mean",
        "temp_mean",
        "precip_total",
    ]
    parcel_stats = parcel_stats.reset_index()

    table = Table(
        title="Per-Parcel Summary", show_header=True, header_style="bold cyan"
    )
    table.add_column("Parcel ID", style="yellow", width=15)
    table.add_column("Obs", style="white", width=5)
    table.add_column("NDVI Mean", style="green", width=10)
    table.add_column("NDVI Range", style="green", width=15)
    table.add_column("EVI", style="blue", width=8)
    table.add_column("Temp", style="red", width=8)
    table.add_column("Precip", style="blue", width=10)

    for _, row in parcel_stats.iterrows():
        ndvi_range = f"{row['ndvi_min']:.3f}-{row['ndvi_max']:.3f}"
        table.add_row(
            row["parcel_id"],
            str(int(row["observations"])),
            f"{row['ndvi_mean']:.3f}",
            ndvi_range,
            f"{row['evi_mean']:.3f}",
            f"{row['temp_mean']:.1f}",
            f"{row['precip_total']:.1f}",
        )

    console.print(table)


def query_sample(
    delta_dir: str = typer.Option(
        "output/timeseries-delta",
        "--delta-dir",
        help="Path to Delta Lake data directory",
    ),
    num_parcels: int = typer.Option(
        10, "--num-parcels", help="Number of random parcels to query"
    ),
    window_days: int = typer.Option(
        10, "--window-days", help="Number of consecutive days for the time window"
    ),
    show_data: bool = typer.Option(
        True,
        "--show-data/--no-data",
        help="Show the actual sample data (disable for large samples)",
    ),
) -> None:
    """Query random parcels within a random time window from Delta Lake data.

    This command:
    1. Selects a random time window of specified days
    2. Finds random parcels that have data in that window
    3. Displays the sample data and statistics
    4. Shows per-parcel summaries

    Examples:
        # Default: 10 parcels, 10-day window
        uv run lake-sandbox query-sample

        # More parcels, shorter window
        uv run lake-sandbox query-sample --num-parcels 20 --window-days 5

        # Large sample without showing raw data
        uv run lake-sandbox query-sample --num-parcels 50 --no-data
    """
    console = Console()

    console.print(Panel("Random Sample Query from Delta Lake", style="bold blue"))

    try:
        # Connect to Delta Lake
        console.print(f"Connecting to Delta Lake at: {delta_dir}")
        conn = connect_to_deltalake()

        # Get random time window
        console.print(f"Selecting random {window_days}-day time window...")
        start_date, end_date = get_random_time_window(conn, delta_dir, window_days)

        # Display sample selection info
        console.print("\n" + "=" * 60)
        display_sample_info(start_date, end_date, num_parcels, window_days, console)

        # Query random parcels in the window
        console.print(f"\nQuerying {num_parcels} random parcels in time window...")
        df = get_random_parcels_in_window(
            conn, delta_dir, start_date, end_date, num_parcels
        )

        # Calculate and display statistics
        console.print("\n" + "=" * 60)
        stats = get_sample_statistics(df)
        display_sample_statistics(stats, console)

        # Display sample data if requested
        if show_data:
            console.print("\n" + "=" * 60)
            display_sample_data(df, console)

        # Display per-parcel summary
        console.print("\n" + "=" * 60)
        display_parcel_summary(df, console)

        # Close connection
        conn.close()

        console.print("\nSample query completed successfully!")

    except Exception as e:
        console.print(f"Error during sample query: {e}", style="bold red")
        raise typer.Exit(1) from e


if __name__ == "__main__":
    typer.run(query_sample)
