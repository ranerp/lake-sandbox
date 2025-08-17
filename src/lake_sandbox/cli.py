import typer

from lake_sandbox.examples.duckdb_analysis import analyze_data
from lake_sandbox.examples.query_parcel import query_parcel
from lake_sandbox.examples.query_sample import query_sample
from lake_sandbox.examples.analyze_partitioning import analyze_partitioning
from lake_sandbox.pipeline.main_pipeline import run_pipeline_cli
from lake_sandbox.reorg_pattern.reorganization import reorg
from lake_sandbox.streaming_assembly.assembly import streaming_assembly
from lake_sandbox.timeseries_generator.generator import generate_timeseries
from lake_sandbox.validator.validation import validate

app = typer.Typer(help="Lake Sandbox CLI - Various utilities and tools")

# Register subcommands
app.command("reorg",
            help="Reorganize raw timeseries data from date-partitioned to parcel-chunk-partitioned format and convert to Delta Lake")(
    reorg)
app.command("streaming-assembly")(streaming_assembly)
app.command("generate-timeseries")(generate_timeseries)
app.command("validate",
            help="Validate organized chunks and Delta tables for data integrity")(
    validate)
app.command("pipeline",
            help="Run the complete data pipeline: generate -> reorganize -> validate")(
    run_pipeline_cli)
app.command("analyze",
            help="Analyze DeltaLake timeseries data using DuckDB and create visualizations")(
    analyze_data)
app.command("query-parcel",
            help="Query and display a single parcel timeseries from Delta Lake data")(
    query_parcel)
app.command("query-sample",
            help="Query random parcels within a random time window from Delta Lake data")(
    query_sample)
app.command("analyze-partitioning",
            help="Analyze Delta Lake partitioning performance and efficiency")(
    analyze_partitioning)


def main() -> None:
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()
