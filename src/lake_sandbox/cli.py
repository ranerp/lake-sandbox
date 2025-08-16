import typer

from lake_sandbox.reorg_pattern.reorganization import reorg
from lake_sandbox.streaming_assembly.assembly import streaming_assembly
from lake_sandbox.timeseries_generator.generator import generate_timeseries
from lake_sandbox.validator.validation import validate
from lake_sandbox.pipeline.main_pipeline import run_pipeline_cli

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


def main() -> None:
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()
