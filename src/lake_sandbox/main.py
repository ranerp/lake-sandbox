import typer

from lake_sandbox.reorg_pattern.reorg_generator import delta_reorg

app = typer.Typer(help="Lake Sandbox CLI - Various utilities and tools")

# Register subcommands
app.command("delta-reorg")(delta_reorg)


def main() -> None:
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()