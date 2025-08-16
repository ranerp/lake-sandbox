import itertools
from pathlib import Path

import numpy as np
import pandas as pd
import typer

from lake_sandbox.timeseries_generator.date_utils import (
    group_dates_by_year,
    parse_date_range,
)


def generate_timeseries(
    output_dir: str = typer.Option(
        "./output/timeseries-raw",
        "--output-dir",
        "-o",
        help="Output directory for parquet files",
    ),
    utm_tiles: str = typer.Option(
        "32TNR,32TNS,32TPR", "--utm-tiles", help="Comma-separated UTM tile identifiers"
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

    # Generate data for each UTM tile and year combination
    total_files = 0
    for tile, (year, year_dates) in itertools.product(tiles, dates_by_year.items()):
        typer.echo(f"Processing tile: {tile}, year: {year}")

        # Create partitioned directory structure: utm_tile=XXX/year=YYYY/
        partition_path = output_path / f"utm_tile={tile}" / f"year={year}"
        partition_path.mkdir(parents=True, exist_ok=True)

        filename = "data.parquet"
        filepath = partition_path / filename

        # Generate all timeseries data for this tile/year combination
        all_data = []
        for date in year_dates:
            date_str = date.strftime("%Y-%m-%d")

            # Generate synthetic timeseries data for this date
            np.random.seed(
                hash(f"{tile}_{date_str}") % 2 ** 32
            )  # Consistent seed per date

            data = {
                "parcel_id": [f"{tile}_{i:06d}" for i in range(num_parcels)],
                "date": [date] * num_parcels,
                "ndvi": np.random.uniform(0.1, 0.9, num_parcels),
                "evi": np.random.uniform(0.0, 0.8, num_parcels),
                "red": np.random.uniform(0.05, 0.3, num_parcels),
                "nir": np.random.uniform(0.3, 0.7, num_parcels),
                "blue": np.random.uniform(0.03, 0.2, num_parcels),
                "green": np.random.uniform(0.04, 0.25, num_parcels),
                "swir1": np.random.uniform(0.1, 0.4, num_parcels),
                "swir2": np.random.uniform(0.05, 0.3, num_parcels),
                "temperature": np.random.uniform(280, 320, num_parcels),  # Kelvin
                "precipitation": np.random.exponential(5, num_parcels),  # mm
                "cloud_cover": np.random.uniform(0, 100, num_parcels),  # percentage
                "geometry_area": np.random.uniform(0.1, 50.0, num_parcels),  # hectares
            }

            df_date = pd.DataFrame(data)
            all_data.append(df_date)

        # Combine all dates for this tile/year into one file
        df_combined = pd.concat(all_data, ignore_index=True)
        df_combined.to_parquet(filepath, index=False)

        total_records = len(df_combined)
        num_dates = len(year_dates)
        typer.echo(
            f"Generated: {partition_path.relative_to(output_path)}/data.parquet ({total_records} records, {num_dates} dates)"
        )
        total_files += 1

    typer.echo(f"Generated {total_files} partitioned files in {output_dir}")
