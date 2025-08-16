import itertools
from pathlib import Path

import duckdb
import typer

from lake_sandbox.timeseries_generator.date_utils import (
    group_dates_by_year,
    parse_date_range,
)
from lake_sandbox.utils.performance import monitor_performance


@monitor_performance()
def generate_timeseries(
    output_dir: str = typer.Option(
        "./output/timeseries-raw",
        "--output-dir",
        "-o",
        help="Output directory for parquet files",
    ),
    utm_tiles: str = typer.Option(
        "32TNR,32TPR", "--utm-tiles", help="Comma-separated UTM tile identifiers"
    ),
    start_date: str = typer.Option(
        "2023-01-01", "--start-date", help="Start date (YYYY-MM-DD)"
    ),
    end_date: str = typer.Option(
        "2023-12-31", "--end-date", help="End date (YYYY-MM-DD)"
    ),
    num_parcels: int = typer.Option(
        500_000, "--num-parcels", help="Number of parcels to generate per tile"
    ),
) -> None:
    """Generate parquet files with timeseries data for multiple UTM tiles (parcel_id, ndvi, bands, etc.)."""
    typer.echo("Generating timeseries parquet files...")

    tiles = [tile.strip() for tile in utm_tiles.split(",")]
    typer.echo(f"UTM Tiles: {tiles}")
    typer.echo(f"Date range: {start_date} to {end_date}")
    typer.echo(f"Number of parcels per tile: {num_parcels}")
    typer.echo(f"Output directory: {output_dir}")

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Parse and group dates
    dates = parse_date_range(start_date, end_date)
    dates_by_year = group_dates_by_year(dates)

    # Generate data for each UTM tile and date combination (separate files per date)
    total_files = 0
    for tile, (year, year_dates) in itertools.product(tiles, dates_by_year.items()):
        typer.echo(f"Processing tile: {tile}, year: {year}")

        for date in year_dates:
            date_str = date.strftime("%Y-%m-%d")

            # Create partitioned directory structure: utm_tile=XXX/year=YYYY/date=YYYY-MM-DD/
            partition_path = output_path / f"utm_tile={tile}" / f"year={year}" / f"date={date_str}"
            partition_path.mkdir(parents=True, exist_ok=True)

            filename = f"{tile}_{date_str}.parquet"
            filepath = partition_path / filename

            conn = duckdb.connect()

            # Set deterministic seed based on tile and date for consistent data
            seed = abs(hash(f"{tile}_{date_str}")) % 2 ** 31
            conn.execute(f"SELECT setseed({seed / 2 ** 31})")

            # Generate synthetic timeseries data using DuckDB's vectorized functions
            conn.execute(f"""
                CREATE TEMP TABLE timeseries AS
                SELECT
                    'parcel_' || lpad((row_number() OVER () - 1)::VARCHAR, 6, '0') as parcel_id,
                    '{date_str}'::DATE as date,
                    0.1 + 0.8 * random() as ndvi,
                    0.8 * random() as evi,
                    0.05 + 0.25 * random() as red,
                    0.3 + 0.4 * random() as nir,
                    0.03 + 0.17 * random() as blue,
                    0.04 + 0.21 * random() as green,
                    0.1 + 0.3 * random() as swir1,
                    0.05 + 0.25 * random() as swir2,
                    280 + 40 * random() as temperature,
                    -ln(random()) * 5 as precipitation,
                    100 * random() as cloud_cover,
                    0.1 + 49.9 * random() as geometry_area
                FROM generate_series(1, {num_parcels})
            """)

            # Export directly to parquet
            conn.execute(f"""
                COPY timeseries TO '{filepath}' (FORMAT PARQUET)
            """)

            conn.close()

            typer.echo(
                f"Generated: {partition_path.relative_to(output_path)}/{filename} ({num_parcels} records)"
            )
            total_files += 1

    typer.echo(f"Generated {total_files} partitioned files in {output_dir}")
