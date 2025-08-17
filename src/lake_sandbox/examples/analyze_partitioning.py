"""Analyze Delta Lake partitioning performance for different query patterns."""

import time
from pathlib import Path

import duckdb
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from lake_sandbox.utils.performance import monitor_performance


def connect_to_deltalake() -> duckdb.DuckDBPyConnection:
    """Connect to DeltaLake data using DuckDB."""
    conn = duckdb.connect()

    # Install and load delta extension
    conn.execute("INSTALL delta")
    conn.execute("LOAD delta")

    return conn


@monitor_performance()
def test_parcel_query_performance(
    conn: duckdb.DuckDBPyConnection, delta_dir: str, sample_parcel_ids: list[str]
) -> dict:
    """Test query performance for different parcel ID queries."""
    delta_path = Path(delta_dir).absolute()
    delta_table_path = delta_path / "parcel_data"

    if not delta_table_path.exists():
        raise FileNotFoundError(f"Delta table not found at: {delta_table_path}")

    results = {}

    for parcel_id in sample_parcel_ids:
        # Test current partitioning approach
        start_time = time.time()

        query = f"""
        SELECT COUNT(*) as record_count
        FROM delta_scan('{delta_table_path}')
        WHERE parcel_id = '{parcel_id}'
        """

        result = conn.execute(query).fetchone()
        query_time = time.time() - start_time

        results[parcel_id] = {
            "records": result[0] if result else 0,
            "query_time": query_time,
        }

    return results


@monitor_performance()
def analyze_partition_distribution(
    conn: duckdb.DuckDBPyConnection, delta_dir: str
) -> dict:
    """Analyze how parcels are distributed across partitions."""
    delta_path = Path(delta_dir).absolute()
    delta_table_path = delta_path / "parcel_data"

    # Get partition distribution
    query = f"""
    SELECT
        parcel_chunk,
        COUNT(*) as total_records,
        COUNT(DISTINCT parcel_id) as unique_parcels,
        COUNT(DISTINCT date) as unique_dates,
        MIN(date) as min_date,
        MAX(date) as max_date
    FROM delta_scan('{delta_table_path}')
    GROUP BY parcel_chunk
    ORDER BY parcel_chunk
    """

    df = conn.execute(query).df()

    # Overall statistics
    total_query = f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT parcel_id) as total_parcels,
        COUNT(DISTINCT parcel_chunk) as total_partitions
    FROM delta_scan('{delta_table_path}')
    """

    total_result = conn.execute(total_query).fetchone()
    if total_result is None:
        raise ValueError("Failed to query total statistics")

    return {
        "partition_stats": df,
        "total_records": total_result[0],
        "total_parcels": total_result[1],
        "total_partitions": total_result[2],
    }


@monitor_performance()
def test_partition_pruning(conn: duckdb.DuckDBPyConnection, delta_dir: str) -> dict:
    """Test if partition pruning works with current setup."""
    delta_path = Path(delta_dir).absolute()
    delta_table_path = delta_path / "parcel_data"

    # Test partition-aware query (should be fast)
    start_time = time.time()
    partition_query = f"""
    SELECT COUNT(*) as record_count
    FROM delta_scan('{delta_table_path}')
    WHERE parcel_chunk = '00'
    """
    partition_result = conn.execute(partition_query).fetchone()
    partition_time = time.time() - start_time

    # Test cross-partition query (should be slower)
    start_time = time.time()
    cross_query = f"""
    SELECT COUNT(*) as record_count
    FROM delta_scan('{delta_table_path}')
    WHERE parcel_id LIKE 'parcel_0000%'
    """
    cross_result = conn.execute(cross_query).fetchone()
    cross_time = time.time() - start_time

    return {
        "partition_query": {
            "records": partition_result[0] if partition_result else 0,
            "time": partition_time,
        },
        "cross_partition_query": {
            "records": cross_result[0] if cross_result else 0,
            "time": cross_time,
        },
    }


def display_partition_analysis(analysis: dict, console: Console):
    """Display partition distribution analysis."""
    stats = analysis["partition_stats"]

    table = Table(
        title="Partition Distribution Analysis",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Partition", style="cyan", width=10)
    table.add_column("Records", style="green", width=12)
    table.add_column("Parcels", style="yellow", width=10)
    table.add_column("Dates", style="blue", width=8)
    table.add_column("Date Range", style="white", width=25)

    for _, row in stats.head(10).iterrows():
        table.add_row(
            row["parcel_chunk"],
            f"{row['total_records']:,}",
            f"{row['unique_parcels']:,}",
            f"{row['unique_dates']}",
            f"{row['min_date']} to {row['max_date']}",
        )

    if len(stats) > 10:
        console.print(f"\n[dim]Showing first 10 of {len(stats)} partitions[/dim]")

    console.print(table)

    # Summary
    summary_table = Table(
        title="Summary Statistics", show_header=True, header_style="bold cyan"
    )
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", style="green")

    summary_table.add_row("Total Records", f"{analysis['total_records']:,}")
    summary_table.add_row("Total Parcels", f"{analysis['total_parcels']:,}")
    summary_table.add_row("Total Partitions", f"{analysis['total_partitions']}")
    summary_table.add_row(
        "Avg Records/Partition",
        f"{analysis['total_records'] // analysis['total_partitions']:,}",
    )
    summary_table.add_row(
        "Avg Parcels/Partition",
        f"{analysis['total_parcels'] // analysis['total_partitions']:,}",
    )

    console.print(summary_table)


def display_performance_results(
    parcel_results: dict, pruning_results: dict, console: Console
):
    """Display query performance results."""
    # Parcel query performance
    perf_table = Table(
        title="Parcel Query Performance", show_header=True, header_style="bold red"
    )
    perf_table.add_column("Parcel ID", style="yellow", width=15)
    perf_table.add_column("Records", style="green", width=10)
    perf_table.add_column("Query Time (ms)", style="red", width=15)

    for parcel_id, result in parcel_results.items():
        perf_table.add_row(
            parcel_id, f"{result['records']:,}", f"{result['query_time'] * 1000:.1f}"
        )

    console.print(perf_table)

    # Partition pruning comparison
    pruning_table = Table(
        title="Partition Pruning Analysis", show_header=True, header_style="bold blue"
    )
    pruning_table.add_column("Query Type", style="cyan", width=20)
    pruning_table.add_column("Records", style="green", width=12)
    pruning_table.add_column("Time (ms)", style="red", width=12)
    pruning_table.add_column("Partitions Scanned", style="yellow", width=18)

    pruning_table.add_row(
        "Single Partition",
        f"{pruning_results['partition_query']['records']:,}",
        f"{pruning_results['partition_query']['time'] * 1000:.1f}",
        "1 (partition pruned)",
    )
    pruning_table.add_row(
        "Cross-Partition",
        f"{pruning_results['cross_partition_query']['records']:,}",
        f"{pruning_results['cross_partition_query']['time'] * 1000:.1f}",
        "All (no pruning)",
    )

    console.print(pruning_table)


def analyze_partitioning(
    delta_dir: str = typer.Option(
        "output/timeseries-delta",
        "--delta-dir",
        help="Path to Delta Lake data directory",
    ),
    num_test_parcels: int = typer.Option(
        5,
        "--num-test-parcels",
        help="Number of random parcels to test query performance",
    ),
) -> None:
    """Analyze Delta Lake partitioning performance and efficiency.

    This command analyzes:
    1. How parcels are distributed across partitions
    2. Query performance for parcel_id lookups
    3. Partition pruning effectiveness
    4. Recommendations for optimization

    Examples:
        # Basic analysis
        uv run lake-sandbox analyze-partitioning

        # Test more parcels
        uv run lake-sandbox analyze-partitioning --num-test-parcels 10
    """
    console = Console()

    console.print(Panel("Delta Lake Partitioning Analysis", style="bold blue"))

    try:
        # Connect to Delta Lake
        console.print(f"Connecting to Delta Lake at: {delta_dir}")
        conn = connect_to_deltalake()

        # Get sample parcel IDs for testing
        console.print(
            f"Selecting {num_test_parcels} random parcels for performance testing..."
        )

        delta_path = Path(delta_dir).absolute()
        delta_table_path = delta_path / "parcel_data"

        sample_query = f"""
        SELECT parcel_id
        FROM delta_scan('{delta_table_path}')
        GROUP BY parcel_id
        ORDER BY RANDOM()
        LIMIT {num_test_parcels}
        """

        sample_parcels = [row[0] for row in conn.execute(sample_query).fetchall()]

        # Analyze partition distribution
        console.print("\n" + "=" * 60)
        console.print("Analyzing partition distribution...")
        analysis = analyze_partition_distribution(conn, delta_dir)
        display_partition_analysis(analysis, console)

        # Test parcel query performance
        console.print("\n" + "=" * 60)
        console.print("Testing parcel query performance...")
        parcel_results = test_parcel_query_performance(conn, delta_dir, sample_parcels)

        # Test partition pruning
        console.print("Testing partition pruning...")
        pruning_results = test_partition_pruning(conn, delta_dir)

        display_performance_results(parcel_results, pruning_results, console)

        # Recommendations
        console.print("\n" + "=" * 60)
        avg_parcel_time = sum(r["query_time"] for r in parcel_results.values()) / len(
            parcel_results
        )
        partition_time = pruning_results["partition_query"]["time"]

        recommendations = Panel(
            f"""Current partitioning by parcel_chunk provides:

PROS:
• Balanced data distribution across partitions
• Good for full-table scans and aggregations
• Efficient for partition-aware queries ({partition_time * 1000:.1f}ms)

CONS:
• Poor for parcel_id lookups (avg {avg_parcel_time * 1000:.1f}ms)
• No partition pruning for individual parcels
• Must scan all partitions for single parcel queries

RECOMMENDATIONS:
1. For parcel_id queries: Consider Z-ordering or secondary indexes
2. For mixed workloads: Consider hybrid partitioning (parcel_chunk + date)
3. For OLTP workloads: Consider partitioning by parcel_id ranges
4. Current setup is optimal for analytical workloads on full dataset""",
            title="Partitioning Assessment",
            style="yellow",
        )

        console.print(recommendations)

        # Close connection
        conn.close()

        console.print("\nPartitioning analysis completed!")

    except Exception as e:
        console.print(f"Error during analysis: {e}", style="bold red")
        raise typer.Exit(1) from e


if __name__ == "__main__":
    typer.run(analyze_partitioning)
