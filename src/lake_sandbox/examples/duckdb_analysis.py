"""Example: Analyzing DeltaLake data with DuckDB and creating visualizations."""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import typer
from pathlib import Path
from datetime import datetime

from lake_sandbox.utils.performance import monitor_performance


def connect_to_deltalake(delta_dir: str = "./output/timeseries-delta") -> duckdb.DuckDBPyConnection:
    """Connect to DeltaLake data using DuckDB."""
    conn = duckdb.connect()

    # Install and load delta extension
    conn.execute("INSTALL delta")
    conn.execute("LOAD delta")

    return conn

@monitor_performance()
def query_parcel_data(conn: duckdb.DuckDBPyConnection, delta_dir: str) -> pd.DataFrame:
    """Query all parcel data from DeltaLake tables."""
    delta_path = Path(delta_dir).absolute()

    # Check if delta directory exists
    if not delta_path.exists():
        raise FileNotFoundError(f"DeltaLake directory not found: {delta_path}")

    # Find all parcel chunk directories
    chunk_dirs = list(delta_path.glob("parcel_chunk=*"))
    if not chunk_dirs:
        raise FileNotFoundError(f"No parcel chunk directories found in: {delta_path}")

    typer.echo(f"Found {len(chunk_dirs)} parcel chunks to query")

    # Query each chunk and combine
    dataframes = []
    for chunk_dir in sorted(chunk_dirs):
        try:
            query = f"""
            SELECT
                parcel_id,
                date,
                ndvi,
                evi,
                cloud_cover,
            FROM delta_scan('{chunk_dir}')
            """
            df = conn.execute(query).df()
            if not df.empty:
                dataframes.append(df)
                typer.echo(f"Loaded {len(df):,} records from {chunk_dir.name}")
        except Exception as e:
            typer.echo(f"Warning: Failed to read {chunk_dir.name}: {e}")
            continue

    if not dataframes:
        raise ValueError("No data could be loaded from any DeltaLake tables")

    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df = combined_df.sort_values(['parcel_id', 'date']).reset_index(drop=True)

    return combined_df


def analyze_timeseries_stats(df: pd.DataFrame) -> dict:
    """Calculate basic statistics about the timeseries data."""
    stats = {
        "total_parcels": df["parcel_id"].nunique(),
        "date_range": (df["date"].min(), df["date"].max()),
        "avg_ndvi": df["ndvi"].mean(),
        "avg_cloud_cover": df["cloud_cover"].mean(),
    }

    print("=== TIMESERIES DATA STATISTICS ===")
    print(f"Total parcels: {stats['total_parcels']:,}")
    print(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
    print(f"Average NDVI: {stats['avg_ndvi']:.3f}")
    print(f"Average cloud cover: {stats['avg_cloud_cover']:.1f}%")

    return stats


def create_ndvi_timeseries_plot(df: pd.DataFrame, sample_parcels: int = 10):
    """Create NDVI timeseries plot for sample parcels."""
    plt.figure(figsize=(12, 8))

    # Sample random parcels
    sample_ids = df["parcel_id"].unique()[:sample_parcels]
    sample_data = df[df["parcel_id"].isin(sample_ids)]

    # Convert date to datetime
    sample_data = sample_data.copy()
    sample_data["date"] = pd.to_datetime(sample_data["date"])

    # Plot each parcel's NDVI timeseries
    for parcel_id in sample_ids:
        parcel_data = sample_data[sample_data["parcel_id"] == parcel_id]
        plt.plot(parcel_data["date"], parcel_data["ndvi"],
                marker='o', markersize=3, alpha=0.7, label=f"Parcel {parcel_id}")

    plt.title(f"NDVI Timeseries for {sample_parcels} Sample Parcels")
    plt.xlabel("Date")
    plt.ylabel("NDVI")
    plt.grid(True, alpha=0.3)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig("ndvi_timeseries.png", dpi=300, bbox_inches='tight')
    plt.close()


def create_vegetation_index_distribution(df: pd.DataFrame):
    """Create distribution plots for vegetation indices."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # NDVI distribution
    axes[0, 0].hist(df["ndvi"], bins=50, alpha=0.7, color='green')
    axes[0, 0].set_title("NDVI Distribution")
    axes[0, 0].set_xlabel("NDVI")
    axes[0, 0].set_ylabel("Frequency")

    # EVI distribution
    axes[0, 1].hist(df["evi"], bins=50, alpha=0.7, color='blue')
    axes[0, 1].set_title("EVI Distribution")
    axes[0, 1].set_xlabel("EVI")
    axes[0, 1].set_ylabel("Frequency")

    # Cloud cover distribution
    axes[1, 1].hist(df["cloud_cover"], bins=30, alpha=0.7, color='gray')
    axes[1, 1].set_title("Cloud Cover Distribution")
    axes[1, 1].set_xlabel("Cloud Cover (%)")
    axes[1, 1].set_ylabel("Frequency")

    plt.tight_layout()
    plt.savefig("vegetation_distributions.png", dpi=300, bbox_inches='tight')
    plt.close()


def create_correlation_heatmap(df: pd.DataFrame):
    """Create correlation heatmap between vegetation indices."""
    # Select numeric columns for correlation
    numeric_cols = ["ndvi", "evi", "cloud_cover"]
    corr_data = df[numeric_cols].corr()

    plt.figure(figsize=(8, 6))
    sns.heatmap(corr_data, annot=True, cmap='coolwarm', center=0,
                square=True, fmt='.3f')
    plt.title("Correlation Matrix: Vegetation Indices and Cloud Cover")
    plt.tight_layout()
    plt.savefig("correlation_heatmap.png", dpi=300, bbox_inches='tight')
    plt.close()


def create_monthly_trends(df: pd.DataFrame):
    """Analyze monthly trends in vegetation indices."""
    df_copy = df.copy()
    df_copy["date"] = pd.to_datetime(df_copy["date"])
    df_copy["month"] = df_copy["date"].dt.month
    df_copy["month_name"] = df_copy["date"].dt.strftime("%B")

    monthly_stats = df_copy.groupby("month_name").agg({
        "ndvi": "mean",
        "evi": "mean",
        "cloud_cover": "mean"
    }).round(3)

    print("\n=== MONTHLY TRENDS ===")
    print(monthly_stats)

    # Create monthly trends plot
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    month_order = ["January", "February", "March", "April"]

    # Filter data to only include months we have
    available_months = df_copy["month_name"].unique()
    month_order = [m for m in month_order if m in available_months]

    # NDVI trend
    monthly_ndvi = df_copy.groupby("month_name")["ndvi"].mean().reindex(month_order)
    axes[0, 0].plot(month_order, monthly_ndvi, marker='o', linewidth=2, color='green')
    axes[0, 0].set_title("Monthly NDVI Trend")
    axes[0, 0].set_ylabel("Average NDVI")
    axes[0, 0].tick_params(axis='x', rotation=45)

    # EVI trend
    monthly_evi = df_copy.groupby("month_name")["evi"].mean().reindex(month_order)
    axes[0, 1].plot(month_order, monthly_evi, marker='o', linewidth=2, color='blue')
    axes[0, 1].set_title("Monthly EVI Trend")
    axes[0, 1].set_ylabel("Average EVI")
    axes[0, 1].tick_params(axis='x', rotation=45)

    # Cloud cover trend
    monthly_cloud = df_copy.groupby("month_name")["cloud_cover"].mean().reindex(month_order)
    axes[1, 1].plot(month_order, monthly_cloud, marker='o', linewidth=2, color='gray')
    axes[1, 1].set_title("Monthly Cloud Cover Trend")
    axes[1, 1].set_ylabel("Average Cloud Cover (%)")
    axes[1, 1].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig("monthly_trends.png", dpi=300, bbox_inches='tight')
    plt.close()


def analyze_data(
    delta_dir: str = typer.Option(
        "output/timeseries-delta", "--delta-dir",
        help="Path to DeltaLake data directory"
    ),
    sample_parcels: int = typer.Option(
        10, "--sample-parcels",
        help="Number of parcels to show in timeseries plot"
    ),
    output_dir: str = typer.Option(
        "output/duckdb_analysis", "--output-dir",
        help="Directory to save generated plots"
    ),
    show_plots: bool = typer.Option(
        False, "--show-plots",
        help="Display plots interactively"
    )
) -> None:
    """Analyze DeltaLake timeseries data using DuckDB and create visualizations."""
    typer.echo("=== DELTALAKE DATA ANALYSIS WITH DUCKDB ===")

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Connect to DeltaLake
    typer.echo(f"Connecting to DeltaLake data at: {delta_dir}")

    try:
        conn = connect_to_deltalake(delta_dir)

        # Query data
        typer.echo("Querying parcel data...")
        df = query_parcel_data(conn, delta_dir)

        # Basic statistics
        stats = analyze_timeseries_stats(df)

        # Create visualizations
        typer.echo("\nCreating visualizations...")

        # Set matplotlib backend based on show_plots
        if not show_plots:
            import matplotlib
            matplotlib.use('Agg')  # Non-interactive backend

        # Change to output directory for saving plots
        import os
        original_dir = os.getcwd()
        os.chdir(output_dir)

        try:
            create_ndvi_timeseries_plot(df, sample_parcels=sample_parcels)
            create_vegetation_index_distribution(df)
            create_correlation_heatmap(df)
            create_monthly_trends(df)

            typer.echo(f"\nAnalysis complete! Generated plots saved to: {Path(output_dir).absolute()}")
            typer.echo("Generated files:")
            typer.echo("- ndvi_timeseries.png")
            typer.echo("- vegetation_distributions.png")
            typer.echo("- correlation_heatmap.png")
            typer.echo("- monthly_trends.png")

        finally:
            # Return to original directory
            os.chdir(original_dir)

        # Close connection
        conn.close()

    except Exception as e:
        typer.echo(f"Error during analysis: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    typer.run(analyze_data)
