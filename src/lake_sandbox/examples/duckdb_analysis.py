"""Example: Analyzing DeltaLake data with DuckDB and creating visualizations."""

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import typer

from lake_sandbox.utils.performance import monitor_performance


def connect_to_deltalake(
    delta_dir: str = "./output/timeseries-delta") -> duckdb.DuckDBPyConnection:
    """Connect to DeltaLake data using DuckDB."""
    conn = duckdb.connect()

    # Install and load delta extension
    conn.execute("INSTALL delta")
    conn.execute("LOAD delta")

    return conn


@monitor_performance()
def query_parcel_data(conn: duckdb.DuckDBPyConnection, delta_dir: str) -> pd.DataFrame:
    """Query all parcel data from the single partitioned DeltaLake table."""
    delta_path = Path(delta_dir).absolute()

    # Check if delta directory exists
    if not delta_path.exists():
        raise FileNotFoundError(f"DeltaLake directory not found: {delta_path}")

    # Check for the single partitioned Delta table
    delta_table_path = delta_path / "parcel_data"
    if not delta_table_path.exists():
        raise FileNotFoundError(f"Partitioned Delta table not found at: {delta_table_path}")

    typer.echo(f"Querying partitioned Delta table at: {delta_table_path}")

    try:
        # Query the single partitioned Delta table
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
            red,
            nir,
            blue,
            green,
            swir1,
            swir2,
            geometry_area
        FROM delta_scan('{delta_table_path}')
        ORDER BY parcel_chunk, parcel_id, date
        """
        df = conn.execute(query).df()
        
        if df.empty:
            raise ValueError("No data found in the Delta table")
        
        typer.echo(f"Loaded {len(df):,} records for {df['parcel_id'].nunique():,} parcels")
        typer.echo(f"Data spans {df['parcel_chunk'].nunique()} partitions")
        
        # Show partition distribution
        partition_stats = df['parcel_chunk'].value_counts().sort_index()
        typer.echo(f"Records per partition:")
        for partition, count in partition_stats.head(5).items():
            typer.echo(f"  Partition {partition}: {count:,} records")
        if len(partition_stats) > 5:
            typer.echo(f"  ... and {len(partition_stats) - 5} more partitions")
        
        return df
        
    except Exception as e:
        raise ValueError(f"Failed to query Delta table: {e}")


def analyze_timeseries_stats(df: pd.DataFrame) -> dict:
    """Calculate basic statistics about the timeseries data."""
    stats = {
        "total_parcels": df["parcel_id"].nunique(),
        "total_partitions": df["parcel_chunk"].nunique(),
        "date_range": (df["date"].min(), df["date"].max()),
        "avg_ndvi": df["ndvi"].mean(),
        "avg_evi": df["evi"].mean(),
        "avg_temperature": df["temperature"].mean(),
        "avg_precipitation": df["precipitation"].mean(),
        "avg_cloud_cover": df["cloud_cover"].mean(),
    }

    print("=== TIMESERIES DATA STATISTICS ===")
    print(f"Total parcels: {stats['total_parcels']:,}")
    print(f"Total partitions: {stats['total_partitions']}")
    print(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
    print(f"Average NDVI: {stats['avg_ndvi']:.3f}")
    print(f"Average EVI: {stats['avg_evi']:.3f}")
    print(f"Average temperature: {stats['avg_temperature']:.1f}°C")
    print(f"Average precipitation: {stats['avg_precipitation']:.1f}mm")
    print(f"Average cloud cover: {stats['avg_cloud_cover']:.1f}%")
    
    # Partition statistics
    print("\n=== PARTITION STATISTICS ===")
    partition_stats = df.groupby('parcel_chunk').agg({
        'parcel_id': ['count', 'nunique'],
        'ndvi': 'mean',
        'temperature': 'mean'
    }).round(3)
    partition_stats.columns = ['total_records', 'unique_parcels', 'avg_ndvi', 'avg_temp']
    print("Top 5 partitions by record count:")
    print(partition_stats.sort_values('total_records', ascending=False).head())

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
    """Create distribution plots for vegetation indices and environmental data."""
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))

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

    # Temperature distribution
    axes[0, 2].hist(df["temperature"], bins=50, alpha=0.7, color='red')
    axes[0, 2].set_title("Temperature Distribution")
    axes[0, 2].set_xlabel("Temperature (°C)")
    axes[0, 2].set_ylabel("Frequency")

    # Precipitation distribution
    axes[1, 0].hist(df["precipitation"], bins=50, alpha=0.7, color='blue')
    axes[1, 0].set_title("Precipitation Distribution")
    axes[1, 0].set_xlabel("Precipitation (mm)")
    axes[1, 0].set_ylabel("Frequency")

    # Cloud cover distribution
    axes[1, 1].hist(df["cloud_cover"], bins=30, alpha=0.7, color='gray')
    axes[1, 1].set_title("Cloud Cover Distribution")
    axes[1, 1].set_xlabel("Cloud Cover (%)")
    axes[1, 1].set_ylabel("Frequency")

    # Partition distribution
    partition_counts = df['parcel_chunk'].value_counts().sort_index()
    axes[1, 2].bar(range(len(partition_counts)), partition_counts.values, alpha=0.7, color='purple')
    axes[1, 2].set_title("Records per Partition")
    axes[1, 2].set_xlabel("Partition Index")
    axes[1, 2].set_ylabel("Record Count")

    plt.tight_layout()
    plt.savefig("vegetation_distributions.png", dpi=300, bbox_inches='tight')
    plt.close()


def create_correlation_heatmap(df: pd.DataFrame):
    """Create correlation heatmap between vegetation indices and environmental data."""
    # Select numeric columns for correlation
    numeric_cols = ["ndvi", "evi", "temperature", "precipitation", "cloud_cover", "red", "nir", "blue", "green", "swir1", "swir2", "geometry_area"]
    available_cols = [col for col in numeric_cols if col in df.columns]
    corr_data = df[available_cols].corr()

    plt.figure(figsize=(12, 10))
    mask = plt.np.triu(plt.np.ones_like(corr_data, dtype=bool))
    sns.heatmap(corr_data, mask=mask, annot=True, cmap='coolwarm', center=0,
                square=True, fmt='.2f', cbar_kws={"shrink": .8})
    plt.title("Correlation Matrix: Vegetation Indices, Environmental & Spectral Data")
    plt.tight_layout()
    plt.savefig("correlation_heatmap.png", dpi=300, bbox_inches='tight')
    plt.close()


def create_monthly_trends(df: pd.DataFrame):
    """Analyze monthly trends in vegetation indices and environmental data."""
    df_copy = df.copy()
    df_copy["date"] = pd.to_datetime(df_copy["date"])
    df_copy["month"] = df_copy["date"].dt.month
    df_copy["month_name"] = df_copy["date"].dt.strftime("%B")

    monthly_stats = df_copy.groupby("month_name").agg({
        "ndvi": "mean",
        "evi": "mean",
        "temperature": "mean",
        "precipitation": "mean",
        "cloud_cover": "mean"
    }).round(3)

    print("\n=== MONTHLY TRENDS ===")
    print(monthly_stats)

    # Create monthly trends plot
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))

    # Get all available months in chronological order
    available_months = df_copy["month_name"].unique()
    month_order = ["January", "February", "March", "April", "May", "June", 
                   "July", "August", "September", "October", "November", "December"]
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

    # Temperature trend
    monthly_temp = df_copy.groupby("month_name")["temperature"].mean().reindex(month_order)
    axes[0, 2].plot(month_order, monthly_temp, marker='o', linewidth=2, color='red')
    axes[0, 2].set_title("Monthly Temperature Trend")
    axes[0, 2].set_ylabel("Average Temperature (°C)")
    axes[0, 2].tick_params(axis='x', rotation=45)

    # Precipitation trend
    monthly_precip = df_copy.groupby("month_name")["precipitation"].mean().reindex(month_order)
    axes[1, 0].plot(month_order, monthly_precip, marker='o', linewidth=2, color='blue')
    axes[1, 0].set_title("Monthly Precipitation Trend")
    axes[1, 0].set_ylabel("Average Precipitation (mm)")
    axes[1, 0].tick_params(axis='x', rotation=45)

    # Cloud cover trend
    monthly_cloud = df_copy.groupby("month_name")["cloud_cover"].mean().reindex(month_order)
    axes[1, 1].plot(month_order, monthly_cloud, marker='o', linewidth=2, color='gray')
    axes[1, 1].set_title("Monthly Cloud Cover Trend")
    axes[1, 1].set_ylabel("Average Cloud Cover (%)")
    axes[1, 1].tick_params(axis='x', rotation=45)

    # Partition data quality by month
    monthly_partitions = df_copy.groupby("month_name")["parcel_chunk"].nunique().reindex(month_order)
    axes[1, 2].plot(month_order, monthly_partitions, marker='o', linewidth=2, color='purple')
    axes[1, 2].set_title("Active Partitions by Month")
    axes[1, 2].set_ylabel("Number of Partitions")
    axes[1, 2].tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig("monthly_trends.png", dpi=300, bbox_inches='tight')
    plt.close()


def create_partition_analysis(df: pd.DataFrame):
    """Create partition-specific analysis plots."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # Sample first 20 partitions for visualization
    sample_partitions = sorted(df['parcel_chunk'].unique())[:20]
    partition_subset = df[df['parcel_chunk'].isin(sample_partitions)]
    
    # NDVI by partition (box plot)
    partition_subset.boxplot(column='ndvi', by='parcel_chunk', ax=axes[0, 0])
    axes[0, 0].set_title("NDVI Distribution by Partition (First 20)")
    axes[0, 0].set_xlabel("Partition")
    axes[0, 0].set_ylabel("NDVI")
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # Records per partition
    partition_counts = df['parcel_chunk'].value_counts().sort_index()
    axes[0, 1].bar(range(len(partition_counts.head(20))), partition_counts.head(20).values, alpha=0.7)
    axes[0, 1].set_title("Records per Partition (First 20)")
    axes[0, 1].set_xlabel("Partition Index")
    axes[0, 1].set_ylabel("Record Count")
    
    # Average NDVI by partition
    partition_ndvi = df.groupby('parcel_chunk')['ndvi'].mean().head(20)
    axes[1, 0].bar(range(len(partition_ndvi)), partition_ndvi.values, alpha=0.7, color='green')
    axes[1, 0].set_title("Average NDVI by Partition (First 20)")
    axes[1, 0].set_xlabel("Partition Index")
    axes[1, 0].set_ylabel("Average NDVI")
    
    # Unique parcels per partition
    parcel_counts = df.groupby('parcel_chunk')['parcel_id'].nunique().head(20)
    axes[1, 1].bar(range(len(parcel_counts)), parcel_counts.values, alpha=0.7, color='orange')
    axes[1, 1].set_title("Unique Parcels per Partition (First 20)")
    axes[1, 1].set_xlabel("Partition Index")
    axes[1, 1].set_ylabel("Unique Parcels")
    
    plt.suptitle("")  # Remove default title
    plt.tight_layout()
    plt.savefig("partition_analysis.png", dpi=300, bbox_inches='tight')
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
            create_partition_analysis(df)

            typer.echo(
                f"\nAnalysis complete! Generated plots saved to: {Path(output_dir).absolute()}")
            typer.echo("Generated files:")
            typer.echo("- ndvi_timeseries.png")
            typer.echo("- vegetation_distributions.png")
            typer.echo("- correlation_heatmap.png")
            typer.echo("- monthly_trends.png")
            typer.echo("- partition_analysis.png")

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
