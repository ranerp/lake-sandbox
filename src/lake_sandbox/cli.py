import typer

from lake_sandbox.reorg_pattern.reorganization import reorg
from lake_sandbox.streaming_assembly.assembly import streaming_assembly

app = typer.Typer(help="Lake Sandbox CLI - Various utilities and tools")

# Register subcommands
app.command("reorg")(reorg)
app.command("streaming-assembly")(streaming_assembly)


def main() -> None:
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()
