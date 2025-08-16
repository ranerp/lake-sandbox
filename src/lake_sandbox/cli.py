import typer

from lake_sandbox.reorg_pattern.reorganization import reorg
from lake_sandbox.streaming_assembly.assembly import streaming_assembly
from lake_sandbox.timeseries_generator.generator import generate_timeseries

app = typer.Typer(help="Lake Sandbox CLI - Various utilities and tools")

# Register subcommands
app.command("reorg")(reorg)
app.command("streaming-assembly")(streaming_assembly)
app.command("generate-timeseries")(generate_timeseries)


def main() -> None:
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()
